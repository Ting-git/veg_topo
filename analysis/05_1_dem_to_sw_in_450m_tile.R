# This script is configured to run on UBELIX.
# To run it on workstation02, update the file paths in the Data Source Configuration section
# because the input/output directories differ between servers.

# DEM → Slope/Aspect → Annual SW_in @ 450 m workflow with nested parallelism
# - External: furrr::future_map() (one worker per DEM tile)
# - Inner layer: foreach/doParallel (parallel radiation calculation within tiles)

library(terra)
library(dplyr)
library(tidyr)
library(meteoland)
library(purrr)
library(furrr)         # outer parallelism
library(fs)
library(stringr)
library(here)
library(readr)
library(parallel)      # for core detection
library(doParallel)    # inner parallelism
library(foreach)

# --- User/project config ----
# source(here::here("config.R")) # no need for it, due to not execute in workstation02
source(here::here("R/process_raster.R"))
source(here::here("R/batch_process_rasters.R"))
source(here::here("R/compute_annual_radiation.R"))

# ----Data Source Configuration-----
# Input dir of DEM tiles
dem_30m_copernicus_dir <- file.path("/storage/scratch/giub_geco/tting/copernicus_dem_30m/copernicus_dem_30m")

# Check if input directory exists, stop if not
if (!dir.exists(dem_30m_copernicus_dir)) {
  stop(paste("Input directory does not exist:", dem_30m_copernicus_dir))
}

# Output dir of SW_IN tiles
sw_in_450m_tiles_dir <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/1_1_deg_tiles")

# Check if output directory exists, create if not
if (!dir.exists(sw_in_450m_tiles_dir)) {
  dir.create(sw_in_450m_tiles_dir, recursive = TRUE)
  message(paste("Created output directory:", sw_in_450m_tiles_dir))
}

# Target grid for aggregation
twi_450m_mosaic_clean_path <- file.path("/storage/scratch/giub_geco/tting/global_twi_450m_clean/ga2_clean.nc")
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
# start_idx <- 2000
# end_idx   <- 7500
# dem_files <- dem_files_all[start_idx:end_idx]
# message(sprintf("Start processing: %d:%d (total %d DEMs)", start_idx, end_idx, length(dem_files)))

dem_files <- dem_files_all[c(1883, 9748, 9749, 9750, 9773, 9842, 9937)]
message(sprintf("Start processing specific indices: 1883, 9748, 9749, 9750, 9773, 9842, 9937 (total %d DEMs)",
                length(dem_files)))

# ---- Core configuration ----
# 内层核数
INNER_CORES <- 4

# 在 SLURM 中使用分配的 CPU 核数，如果没有 SLURM 环境则使用 detectCores()
available_cores <- as.integer(Sys.getenv("SLURM_CPUS_PER_TASK", unset = parallel::detectCores()))
total_cores <- parallel::detectCores()  # 节点总核心数

# 外层 worker 数量
outer_cores <- max(1, floor(available_cores / INNER_CORES))

# 打印核心信息
message(sprintf("Node total cores: %d", total_cores))
message(sprintf("Available cores for this job: %d", available_cores))
message(sprintf("Using %d outer workers, each with %d inner cores", outer_cores, INNER_CORES))
message(sprintf("Total parallel threads: %d", outer_cores * INNER_CORES))

# ---- Outer parallel plan ----
plan(multisession, workers = outer_cores)
t0 <- Sys.time()

process_one_tile <- function(file) {
  # Output file path
  base_name <- fs::path_ext_remove(fs::path_file(file))
  output_path_sw_in <- file.path(sw_in_450m_tiles_dir, paste0(base_name, "_to_sw_in_450m.nc"))
  output_path_sw_in_flat <- file.path(sw_in_450m_tiles_dir, paste0(base_name, "_to_sw_in_flat_450m.nc"))

  # Check if it has been processed
  if (fs::file_exists(output_path_sw_in) && fs::file_exists(output_path_sw_in_flat)) {
    return(list(success = TRUE,
                file = file,
                out_file_sw_in = output_path_sw_in,
                out_file_sw_in_flat = output_path_sw_in_flat,
                skipped = TRUE,
                error = NULL))
  }

  tryCatch({

    # Read DEM
    dem <- terra::rast(file)

    # Compute slope/aspect
    slope  <- terra::terrain(dem, v = "slope", unit = "degrees")
    aspect <- terra::terrain(dem, v = "aspect", unit = "degrees")

    # Aggregate to ~450m
    aligned <- batch_process_rasters(
      list(dem = dem, slope = slope, aspect = aspect),
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
                  out_file_sw_in = NULL,
                  out_file_sw_in_flat = NULL,
                  skipped = FALSE,
                  error = "no_valid_cells"))
    }

    # Inner parallelism
    cl <- makeCluster(INNER_CORES)
    registerDoParallel(cl)

    df_calc <- foreach(i = 1:nrow(df), .combine = bind_rows,
                       .packages = c("dplyr", "purrr", "meteoland"),
                       .export = "compute_annual_radiation") %dopar% {
                         row <- df[i, ]

                         sw_in_value <- compute_annual_radiation(
                           lat_deg = row$lat,
                           slope_deg = row$slope,
                           aspect_deg = row$aspect,
                           year = 2020
                         )

                         sw_in_flat <- compute_annual_radiation(
                           lat_deg = row$lat,
                           slope_deg = 0,
                           aspect_deg = 0,
                           year = 2020
                         )

                         data.frame(
                           lon = row$lon,
                           lat = row$lat,
                           sw_in = sw_in_value,
                           sw_in_flat = sw_in_flat
                         )
                       }

    stopCluster(cl)
    registerDoSEQ()

    # Build rasters
    crs_out <- terra::crs(aligned[["dem"]])
    sw_in      <- terra::rast(df_calc[, c("lon", "lat", "sw_in")],      type = "xyz", crs = crs_out)
    sw_in_flat <- terra::rast(df_calc[, c("lon", "lat", "sw_in_flat")], type = "xyz", crs = crs_out)

    # Write two separate NetCDFs
    terra::writeCDF(sw_in,      output_path_sw_in,      varname = "sw_in",      overwrite = TRUE)
    terra::writeCDF(sw_in_flat, output_path_sw_in_flat, varname = "sw_in_flat", overwrite = TRUE)

    rm(dem, slope, aspect, aligned, df, df_calc, sw_in, sw_in_flat)
    gc(full = TRUE)

    list(success = TRUE,
         file = file,
         out_file_sw_in = output_path_sw_in,
         out_file_sw_in_flat = output_path_sw_in_flat,
         skipped = FALSE,
         error = NULL)

  }, error = function(e) {
    list(success = FALSE,
         file = file,
         out_file_sw_in = NULL,
         out_file_sw_in_flat = NULL,
         skipped = FALSE,
         error = conditionMessage(e))
  })
}

# Run in parallel over tiles
results <- furrr::future_map(
  dem_files,
  process_one_tile,
  .progress = FALSE,
  .options = furrr::furrr_options(
    seed = TRUE,
    globals = c("process_raster", "batch_process_rasters", "compute_annual_radiation",
                "res_tar", "sw_in_450m_tiles_dir", "INNER_CORES"),
    packages = c("terra", "dplyr", "tidyr", "purrr", "doParallel", "foreach", "parallel")
  )
)

plan(sequential)
elapsed <- as.numeric(difftime(Sys.time(), t0, units = "mins"))
message(sprintf("done [%.1f mins]", elapsed))

# 递归统计文件数量
file_count <- length(list.files(path = sw_in_450m_tiles_dir, recursive = TRUE, all.files = TRUE))
message(sprintf("Total number of files in %s: %d", sw_in_450m_tiles_dir, file_count))

# ---- Summary ----
failed_results <- keep(results, ~ !.x$success)
success_count <- sum(map_lgl(results, ~ .x$success))
failed_count <- length(failed_results)

message("\n=== FINAL SUMMARY ===")
message(sprintf("Total processed: %d", length(results)))
message(sprintf("Success: %d", success_count))
message(sprintf("Failed: %d", failed_count))
message(sprintf("Success rate: %.1f%%", (success_count / length(results)) * 100))

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


# ----- check the processed file -----

dem_base_names <- dem_files_all |>
  fs::path_file() |>
  fs::path_ext_remove()

sw_in_base_names <- fs::dir_ls(
  path = sw_in_450m_tiles_dir,
  glob = "*_to_sw_in_450m.nc",
  recurse = TRUE
) |>
  fs::path_file() |>
  fs::path_ext_remove() |>
  (\(x) gsub("_to_sw_in_450m$", "", x))()

sw_in_flat_base_names <- fs::dir_ls(
  path = sw_in_450m_tiles_dir,
  glob = "*_to_sw_in_flat_450m.nc",
  recurse = TRUE
)|>
  fs::path_file() |>
  fs::path_ext_remove() |>
  (\(x) gsub("_to_sw_in_flat_450m$", "", x))()

missing_sw_in <- setdiff(dem_base_names, sw_in_base_names)
missing_sw_in_flat <- setdiff(dem_base_names, sw_in_flat_base_names)

missing_sw_in_idx <- which(dem_base_names %in% missing_sw_in)
missing_sw_in_flat_idx <- which(dem_base_names %in% missing_sw_in_flat)

missing_sw_in_idx


# check
dem <- dem_files_all[1199]
r1 <- rast(dem)
plot(r1)

dem_base_name <- fs::path_ext_remove(fs::path_file(dem))
sw_in <- file.path(sw_in_450m_tiles_dir, paste0(dem_base_name, "_to_sw_in_450m.nc"))
sw_in_flat <- file.path(sw_in_450m_tiles_dir, paste0(dem_base_name, "_to_sw_in_flat_450m.nc"))

r2 <- rast(sw_in)
plot(r2)

r3 <- rast(sw_in_flat)
plot(r3)
