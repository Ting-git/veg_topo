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
library(here)
library(readr)
library(parallel)      # for core detection
library(doParallel)    # inner parallelism
library(foreach)

# --- User/project config ----
source(here::here("config.R"))
source(here::here("R/process_raster.R"))
source(here::here("R/batch_process_rasters.R"))
source(here::here("R/compute_annual_radiation.R"))

# ---- Core configuration ----
INNER_CORES <- 4
total_cores <- parallel::detectCores()
available_cores <- total_cores - 10  # 留一个给系统

# Outer workers = floor(available / inner)
outer_cores <- max(1, floor(available_cores / INNER_CORES))
if (outer_cores * INNER_CORES > available_cores) {
  outer_cores <- max(1, floor(available_cores / INNER_CORES))
}

message(sprintf("Core configuration: %d total cores, %d available", total_cores, available_cores))
message(sprintf("Using %d outer workers, each with %d inner cores", outer_cores, INNER_CORES))
message(sprintf("Total parallel threads: %d", outer_cores * INNER_CORES))

# ---- Input DEM tiles ----
dem_30m_copernicus_dir <- "/data_2/scratch/ting/veg_topo_data/data_raw/copernicus_dem_30m/copernicus_dem_30m"
dem_files_all <- fs::dir_ls(
  path = dem_30m_copernicus_dir,
  glob = "*_DEM.tif",
  recurse = TRUE
)
message(sprintf("Found %d DEM tiles", length(dem_files_all)))

# ---- Processing info ----
start_idx <- 201
end_idx   <- 1000
dem_files <- dem_files_all[start_idx:end_idx]
message(sprintf("Start processing: %d:%d (total %d DEMs)", start_idx, end_idx, length(dem_files)))

# ---- Target grid ----
twi <- rast(twi_450m_mosaic_clean_path)
res_tar <- res(twi)

# ---- Output dir ----
if (!dir_exists(sw_in_450m_dir)) dir_create(sw_in_450m_dir, recurse = TRUE)

# ---- Outer parallel plan ----
plan(multisession, workers = outer_cores)
t0 <- Sys.time()

process_one_tile <- function(file) {
  # 2 output
  output_path_sw_in      <- fs::path(sw_in_450m_dir, paste0(fs::path_ext_remove(fs::path_file(file)), "_to_sw_in_450m.nc"))
  output_path_sw_in_flat <- fs::path(sw_in_450m_dir, paste0(fs::path_ext_remove(fs::path_file(file)), "_to_sw_in_flat_450m.nc"))

  tryCatch({
    if (fs::file_exists(output_path_sw_in) && fs::file_exists(output_path_sw_in_flat)) {
      return(list(success = TRUE,
                  out_file_sw_in = output_path_sw_in,
                  out_file_sw_in_flat = output_path_sw_in_flat,
                  skipped = TRUE,
                  error = NULL))
    }

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
                  out_file_sw_in = NULL,
                  out_file_sw_in_flat = NULL,
                  skipped = FALSE,
                  error = "no_valid_cells"))
    }

    # Inner parallelism
    cl <- makeCluster(INNER_CORES)
    registerDoParallel(cl)

    df_calc <- foreach(i = 1:nrow(df), .combine = bind_rows,
                       .packages = c("dplyr", "purrr"),
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

    rm(dem, slope, aspect, aligned, df, df_calc, sw_in, sw_in_flat); gc()

    message(sprintf("Saved: %s", output_path_sw_in))
    message(sprintf("Saved: %s", output_path_sw_in_flat))

    list(success = TRUE,
         out_file_sw_in = output_path_sw_in,
         out_file_sw_in_flat = output_path_sw_in_flat,
         skipped = FALSE,
         error = NULL)

  }, error = function(e) {
    message(sprintf("❌ Error processing file: %s", file))
    message(sprintf("  → %s", e$message))
    list(success = FALSE,
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
  .progress = TRUE,
  .options = furrr::furrr_options(
    seed = TRUE,
    globals = c("process_raster", "batch_process_rasters", "compute_annual_radiation",
                "res_tar", "sw_in_450m_dir", "INNER_CORES"),
    packages = c("terra", "dplyr", "tidyr", "purrr", "doParallel", "foreach", "parallel")
  )
)

plan(sequential)
elapsed <- as.numeric(difftime(Sys.time(), t0, units = "mins"))
message(sprintf("done [%.1f mins]", elapsed))

# ---- Save results summary ----
results_tbl <- purrr::list_rbind(lapply(seq_along(results), function(i) {
  tibble(
    file = dem_files[[i]],
    success = results[[i]]$success,
    skipped = results[[i]]$skipped,
    out_file_sw_in = results[[i]]$out_file_sw_in %||% NA_character_,
    out_file_sw_in_flat = results[[i]]$out_file_sw_in_flat %||% NA_character_,
    error = results[[i]]$error %||% NA_character_
  )
}))

saveRDS(results, file = here::here("data/results_sw_in_aggregate_rawlist.rds"))
readr::write_csv(results_tbl, here::here("data/results_sw_in_aggregate_summary.csv"))

message(sprintf("Processing completed: %d success, %d failed",
                sum(results_tbl$success), sum(!results_tbl$success)))

invisible(results_tbl)



# check
# r1 <- rast("/data_2/scratch/ting/veg_topo_data/data/global_sw_in_450m/30_30_deg/Copernicus_DSM_COG_10_N00_00_E017_00_DEM_to_sw_in_flat_450m.nc")
# plot(r1)
#
# r2 <- rast("/data_2/scratch/ting/veg_topo_data/data/global_sw_in_450m/30_30_deg/Copernicus_DSM_COG_10_N00_00_E017_00_DEM_to_sw_in_450m.nc")
# plot(r2)
