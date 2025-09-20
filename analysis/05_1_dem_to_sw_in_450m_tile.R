# This script is configured to run on UBELIX.
# To run it on workstation02, update the file Configuration section and numbers of available core
# because the input/output directories differ between servers.

# DEM → Slope/Aspect → Annual SW_in @ 450 m workflow with nested parallelism
# - External: furrr::future_map() (one worker per DEM tile)
# - Inner layer: foreach/doParallel (parallel radiation calculation within tiles)

library(terra)
library(dplyr)
library(tidyr)
library(purrr)
library(furrr)         # outer parallelism
library(fs)
library(stringr)
library(readr)
library(parallel)      # for core detection
library(doParallel)    # inner parallelism
library(foreach)

# --- User/project config ----
# source(here::here("config.R")) # no need for it, due to not execute in workstation02
source(here::here("R/raster_preprocess_save.R"))
source(here::here("R/aggregate_topography.R"))
source(here::here("R/helpers.R")) # SPLASH
source(here::here("R/calc_sw_in.R")) # SPLASH

# ----Data Source Configuration-----
# Input dir of DEM tiles
dem_30m_copernicus_dir <- file.path("/storage/scratch/giub_geco/tting/copernicus_dem_30m/copernicus_dem_30m")

# Output dir of SW_IN tiles
sw_in_450m_tile_dir <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/1_1_deg_tiles")

# Target grid for aggregation
twi_450m_mosaic_clean_path <- file.path("/storage/scratch/giub_geco/tting/global_twi_450m_clean/ga2_clean.nc")

# Check if input directory exists, stop if not
if (!dir.exists(dem_30m_copernicus_dir)) {
  stop(paste("Input directory does not exist:", dem_30m_copernicus_dir))
}

# Check if output directory exists, create if not
if (!dir.exists(sw_in_450m_tile_dir)) {
  dir.create(sw_in_450m_tile_dir, recursive = TRUE)
  message(paste("Created output directory:", sw_in_450m_tile_dir))
}

# Load target raster, and save the target resolution
twi <- rast(twi_450m_mosaic_clean_path)
res_tar <- res(twi)
rm(twi); gc()

# ---- Read input data ----
dem_files_all <- fs::dir_ls(
  path = dem_30m_copernicus_dir,
  glob = "*_DEM.tif",
  recurse = TRUE
)
message(sprintf("Found %d DEM tiles", length(dem_files_all)))

# ---- Processing info ----
start_idx <- 12001
end_idx   <- 26450
dem_files <- dem_files_all[start_idx:end_idx]
message(sprintf("Start processing: %d:%d (total %d DEMs)", start_idx, end_idx, length(dem_files)))

# Re-process missing tiles
# dem_files <- dem_files_all[c(1883, 9748, 9749, 9750, 9773, 9842, 9937)]
# message(sprintf("Start processing specific indices: 1883, 9748, 9749, 9750, 9773, 9842, 9937 (total %d DEMs)",
                # length(dem_files)))

# ---- Core configuration ----

# numbers of inner core
INNER_CORES <- 4

# Use the number of allocated CPU cores in SLURM, or detectCores() if there is no SLURM environment
available_cores <- as.integer(Sys.getenv("SLURM_CPUS_PER_TASK", unset = parallel::detectCores())) # on UBELIX
# available_cores <- 8 # test on workstation2
total_cores <- parallel::detectCores()  # Total number of cores on the node

# numbers of outer core
outer_cores <- max(1, floor(available_cores / INNER_CORES))

# Print the info of cores
message(sprintf("Node total cores: %d", total_cores))
message(sprintf("Available cores for this job: %d", available_cores))
message(sprintf("Using %d outer workers, each with %d inner cores", outer_cores, INNER_CORES))
message(sprintf("Total parallel threads: %d", outer_cores * INNER_CORES))

# ---- Outer parallel plan ----
plan(multisession, workers = outer_cores)
t0 <- Sys.time()
message(paste0("Calculation on DEM Start:", format(t0, "%Y-%m-%d %H:%M:%S")))

# ---- Inner cluster ----
cl <- makeCluster(INNER_CORES)
registerDoParallel(cl)

process_one_tile <- function(file) {

  # Output file path
  base_name <- fs::path_ext_remove(fs::path_file(file))
  output_path_sw_in_uneven <- file.path(sw_in_450m_tile_dir, paste0(base_name, "_to_sw_in_uneven_450m.nc"))
  output_path_sw_in_flat <- file.path(sw_in_450m_tile_dir, paste0(base_name, "_to_sw_in_flat_450m.nc"))

  # Check if it has been processed
  if (fs::file_exists(output_path_sw_in_uneven) && fs::file_exists(output_path_sw_in_flat)) {
    return(list(success = TRUE,
                file = file,
                # out_file_sw_in_uneven = output_path_sw_in_uneven,
                # out_file_sw_in_flat = output_path_sw_in_flat,
                skipped = TRUE,
                error = NULL))
  }

  tryCatch({

    # Read DEM
    dem <- terra::rast(file)

    # Get slope and aspect and Aggregate to ~450m
    aligned <- aggregate_topography(
      dem,
      res_tar = res_tar,
      if_resample = FALSE
    )

    # Extract + join
    df <- as.data.frame(aligned[["dem"]], xy = TRUE) |>
      left_join(as.data.frame(aligned[["slope"]], xy = TRUE), by = c("x", "y")) |>
      left_join(as.data.frame(aligned[["aspect"]], xy = TRUE), by = c("x", "y")) |>
      tibble::as_tibble() |>
      drop_na()
    names(df) <- c("lon", "lat", "dem", "slope", "aspect")

    if (nrow(df) == 0) {
      warning(sprintf("No valid cells after drop_na for %s", file))
      return(list(success = FALSE,
                  file = file,
                  # out_file_sw_in_uneven = NULL,
                  # out_file_sw_in_flat = NULL,
                  skipped = FALSE,
                  error = "no_valid_cells"))
    }

    # # Inner parallelism
    # cl <- makeCluster(INNER_CORES)
    # registerDoParallel(cl)

    # Chunk Processing
    chunk_size <- 5000  # rows per chunk, adjust based on memory
    chunks <- split(df, ceiling(seq_len(nrow(df)) / chunk_size))

    # Parallel Calculation for Each Chunk - Direct assignment
    df_calc <- foreach(
      chunk = chunks,
      .combine = bind_rows,
      .packages = c("dplyr"),
      .export   = c("calc_sw_in_daily", "calc_sw_in", "julian_day",
                    "berger_tls", "dcos", "dsin")
    ) %dopar% {

      # Calculate sw_in_uneven and sw_in_flat for entire chunk
      sw_in_uneven <- calc_sw_in(chunk$lat, chunk$slope, chunk$aspect, year = 2020)
      sw_in_flat <- calc_sw_in(chunk$lat, rep(0, nrow(chunk)), rep(0, nrow(chunk)), year = 2020)

      # Combine results back to dataframe
      chunk |>
        mutate(sw_in_uneven = sw_in_uneven,
               sw_in_flat = sw_in_flat)

    }

    # stopCluster(cl)
    # registerDoSEQ()

    # Build rasters
    crs_out <- terra::crs(aligned[["dem"]])
    sw_in_uneven <- terra::rast(df_calc[, c("lon", "lat", "sw_in_uneven")], type = "xyz", crs = crs_out)
    sw_in_flat <- terra::rast(df_calc[, c("lon", "lat", "sw_in_flat")], type = "xyz", crs = crs_out)

    # Write two separate NetCDFs
    terra::writeCDF(sw_in_uneven,output_path_sw_in_uneven,varname = "sw_in_uneven", overwrite = TRUE)
    terra::writeCDF(sw_in_flat, output_path_sw_in_flat, varname = "sw_in_flat", overwrite = TRUE)

    rm(chunks, dem, aligned, df, df_calc, sw_in_uneven, sw_in_flat)
    gc(full = TRUE)

    list(success = TRUE,
         file = file,
         # out_file_sw_in_uneven = output_path_sw_in_uneven,
         # out_file_sw_in_flat = output_path_sw_in_flat,
         skipped = FALSE,
         error = NULL)

  }, error = function(e) {
    list(success = FALSE,
         file = file,
         # out_file_sw_in_uneven = NULL,
         # out_file_sw_in_flat = NULL,
         skipped = FALSE,
         error = conditionMessage(e))
  }, finally = {
    rm(dem, aligned, df, df_calc, chunks)
    gc(full = TRUE)
  })
}

# ---- processing ----
all_results <- furrr::future_map(
  dem_files,
  process_one_tile,
  .progress = FALSE,
  .options = furrr::furrr_options(
    seed = TRUE,
    globals = c("raster_preprocess_save", "aggregate_topography",
                "calc_sw_in_daily", "calc_sw_in",
                "julian_day", "berger_tls", "dcos", "dsin",
                "res_tar", "sw_in_450m_tile_dir", "INNER_CORES"),
    packages = c("terra", "dplyr", "tidyr", "purrr",
                 "doParallel", "foreach", "parallel")
  )
)

# stop cluster
stopCluster(cl)
registerDoSEQ()

# Switch back to sequential execution
plan(sequential)

elapsed <- as.numeric(difftime(Sys.time(), t0, units = "mins"))
message(sprintf("Processing done [%.1f mins]", elapsed))

# Recursive file count
file_count <- length(list.files(path = sw_in_450m_tile_dir, recursive = TRUE, all.files = TRUE))
message(sprintf("Total number of files in %s: %d", sw_in_450m_tile_dir, file_count))

# ---- Summary ----
failed_results <- keep(all_results, ~ !.x$success)
success_count <- sum(map_lgl(all_results, ~ .x$success))
failed_count <- length(failed_results)

message("\n=== FINAL SUMMARY ===")
message(sprintf("Total processed: %d", length(all_results)))
message(sprintf("Success: %d", success_count))
message(sprintf("Failed: %d", failed_count))
message(sprintf("Success rate: %.1f%%", (success_count / length(all_results)) * 100))

if (length(failed_results) > 0) {
  message("\n=== FAILED FILES SUMMARY ===")
  message(sprintf("Total failed: %d", length(failed_results)))
  message("----------------------------------------")

  for (i in seq_along(failed_results)) {
    result <- failed_results[[i]]
    message(sprintf("%d. File: %s", i, basename(result$file)))
    message(sprintf("   Error: %s", result$error))
    message("----------------------------------------")
  }
} else {
  message("All files processed successfully! No failures.")
}


