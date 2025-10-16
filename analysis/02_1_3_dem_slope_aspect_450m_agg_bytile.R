
# ~ 16 min for parallel process: UBELIX, 110 cores
# ~ 120 min for mosaic, resample and save: UBELIX, 110 cores

# =============================================================================
# Script: Aggregate and Resample Global DEM Tiles
# Author : [Ting Tan]
# Date   : [2025-10-16]
#
# Description:
#   This script reads global 30m DEM tiles, computes slope and aspect,
#   aggregates them to ~450m resolution, mosaics the tiles, and resamples
#   to a target grid (TWI 450m). Parallel processing is used for speed.
# =============================================================================

# ---------------------------- Setup Environment ------------------------------

# Load required packages
library(terra)
library(furrr)
library(future)
library(fs)
library(here)

# Detect host and load configuration
hostname <- trimws(tolower(system("hostname", intern = TRUE)))

if (hostname == "dash") {
  message("💻 Workstation detected ('dash') → loading config.R")
  source(here::here("config.R"))
  workers <- 8
} else {
  message("🖥️ HPC environment detected (", hostname, ") → loading config_ubelix.R")
  source(here::here("config_ubelix.R"))
  workers <- 110
}

# Load project functions
source(here::here("R/raster_preprocess_save.R"))
source(here::here("R/aggregate_topography.R"))
source(here::here("R/mosaic_tiles.R"))
source(here::here("R/mosaic_tiles_gdal.R"))

# ---------------------------- Input / Output Paths ---------------------------

# Input: all 30m DEM tiles
dem_30m_tiles_path <- fs::dir_ls(path = dem_30m_copernicus_dir, glob = "*_DEM.tif", recurse = TRUE)
# dem_30m_tiles_path_sub <- dem_30m_tiles_path[1:2000]  # optional subset for testing

# Output directories for 450m tiles
dem_450m_tiles_dir <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/dem_1_1_deg")
slope_450m_tiles_dir <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/slope_1_1_deg")
aspect_450m_tiles_dir <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/aspect_1_1_deg")

dirs_to_create <- c(dem_450m_tiles_dir, slope_450m_tiles_dir, aspect_450m_tiles_dir)

# Create directories if they do not exist
for (d in dirs_to_create) {
  if (!dir.exists(d)) dir.create(d, recursive = TRUE)
}

# ---------------------------- Target Layer & Resolution ----------------------

# Load target raster for alignment and resampling
twi_450m_r <- terra::rast(twi_450m_mosaic_clean_path)
res_tar <- res(twi_450m_r)

# ---------------------------- Parallel Processing ----------------------------

plan(multisession, workers = workers)
t0 <- Sys.time()

# Aggregate DEM tiles to 450m and compute slope/aspect
results <- future_map(
  dem_30m_tiles_path,  # use dem_30m_tiles_path_sub for testing
  function(file) {
    tryCatch({
      # Define output file paths
      dem_output <- file.path(dem_450m_tiles_dir, paste0(sub("\\.tif$", "", basename(file)), "_to450m_dem.nc"))
      slope_output <- file.path(slope_450m_tiles_dir, paste0(sub("\\.tif$", "", basename(file)), "_to450m_slope.nc"))
      aspect_output <- file.path(aspect_450m_tiles_dir, paste0(sub("\\.tif$", "", basename(file)), "_to450m_aspect.nc"))
      output <- c(dem_output, slope_output, aspect_output)

      # Skip if already processed
      if (all(fs::file_exists(output))) return(list(success = TRUE, error = NULL))

      # Load DEM
      dem <- terra::rast(file)

      # Aggregate DEM, slope, and aspect to target resolution
      aligned <- aggregate_topography(dem, res_tar = res_tar, if_resample = FALSE)

      # Save outputs
      terra::writeCDF(aligned[["dem"]], dem_output, overwrite = TRUE, varname = "dem")
      terra::writeCDF(aligned[["slope"]], slope_output, overwrite = TRUE, varname = "slope")
      terra::writeCDF(aligned[["aspect"]], aspect_output, overwrite = TRUE, varname = "aspect")

      if (all(fs::file_exists(output))) message("✅ Saved: ", output)

      # Clean up
      rm(dem, aligned); gc()

      list(success = TRUE, error = NULL)

    }, error = function(e) {
      message(sprintf("❌ Error processing file: %s", file))
      message(sprintf("  → %s", e$message))
      list(success = FALSE, error = e$message)
    }, finally = { gc() })
  },
  .progress = FALSE,
  .options = furrr::furrr_options(
    seed = TRUE,
    globals = c("res_tar", "raster_preprocess_save",
                "aggregate_topography", "dem_450m_tiles_dir",
                "slope_450m_tiles_dir", "aspect_450m_tiles_dir"),
    packages = c("terra", "fs")
  )
)

plan(sequential)
gc()

message(sprintf("Processing completed [%.1fs]", difftime(Sys.time(), t0, units = "secs")))

# ---------------------------- Mosaic Tiles -----------------------------------
#
# message("🗺️ Mosaicing DEM...")
# dem_450m_mosaic <- mosaic_tiles(input_dir = dem_450m_tiles_dir,
#                                 output_file = NULL,
#                                 pattern = "*_DEM_to450m_dem.nc",
#                                 varname = "dem")
#
# message("🗺️ Mosaicing slope...")
# slope_450m_mosaic <- mosaic_tiles(input_dir = slope_450m_tiles_dir,
#                                   output_file = NULL,
#                                   pattern = "*_DEM_to450m_slope.nc",
#                                   varname = "slope")
#
# message("🗺️ Mosaicing aspect...")
# aspect_450m_mosaic <- mosaic_tiles(input_dir = aspect_450m_tiles_dir,
#                                    output_file = NULL,
#                                    pattern = "*_DEM_to450m_aspect.nc",
#                                    varname = "aspect")
#
# gc()

message("🗺️ Mosaicing DEM...")
dem_450m_mosaic <- mosaic_tiles_gdal(input_dir =  dem_450m_tiles_dir,
                                     output_file = NULL,
                                     pattern = "*_DEM_to450m_dem.nc",
                                     overwrite = TRUE,
                                     num_threads = max(1, floor(workers * 0.8)),
                                     max_files_per_vrt = 9000)

message("🗺️ Mosaicing slope...")
slope_450m_mosaic <- mosaic_tiles_gdal(input_dir =  slope_450m_tiles_dir,
                                       output_file = NULL,
                                       pattern = "*_DEM_to450m_slope.nc",
                                       overwrite = TRUE,
                                       num_threads = max(1, floor(workers * 0.8)),
                                       max_files_per_vrt = 9000)

message("🗺️ Mosaicing aspect...")
aspect_450m_mosaic <- mosaic_tiles_gdal(input_dir =  aspect_450m_tiles_dir,
                                        output_file = NULL,
                                        pattern = "*_DEM_to450m_aspect.nc",
                                        overwrite = TRUE,
                                        num_threads = max(1, floor(workers * 0.8)),
                                        max_files_per_vrt = 9000)

gc()
# ---------------------------- Resample to Target Grid ------------------------

message("🔄 Resampling DEM...")
dem_450m_resampled <- terra::resample(dem_450m_mosaic, twi_450m_r, method = "bilinear")

message("🔄 Resampling slope...")
slope_450m_resampled <- terra::resample(slope_450m_mosaic, twi_450m_r, method = "bilinear")

message("🔄 Resampling aspect...")
aspect_450m_resampled <- terra::resample(aspect_450m_mosaic, twi_450m_r, method = "bilinear")

# Cleanup intermediate mosaics
rm(dem_450m_mosaic, slope_450m_mosaic, aspect_450m_mosaic, twi_450m_r)
gc()

# ---------------------------- Save Final Results -----------------------------

terra::writeCDF(dem_450m_resampled, dem_450m_mosaic_path, overwrite = TRUE, varname = "dem")
terra::writeCDF(slope_450m_resampled, slope_450m_mosaic_path, overwrite = TRUE, varname = "slope")
terra::writeCDF(aspect_450m_resampled, aspect_450m_mosaic_path, overwrite = TRUE, varname = "aspect")

if (file.exists(dem_450m_mosaic_path)) message("✅ Saved: ", dem_450m_mosaic_path)
if (file.exists(slope_450m_mosaic_path)) message("✅ Saved: ", slope_450m_mosaic_path)
if (file.exists(aspect_450m_mosaic_path)) message("✅ Saved: ", aspect_450m_mosaic_path)

# Final cleanup
rm(list = ls())
gc()

# ----------------------------
# r1 <- terra::rast("/storage/scratch/giub_geco/tting/global_dem_slope_aspect_450m/dem_1_1_deg/Copernicus_DSM_COG_10_N00_00_E012_00_DEM_to450m_dem.nc")
# plot(r1)
#
# r2 <- terra::rast("/storage/scratch/giub_geco/tting/global_dem_450m/slope_1_1_deg/Copernicus_DSM_COG_10_N00_00_E012_00_DEM_to450m_slope.nc")
# plot(r2)
#
# r3 <- terra::rast("/storage/scratch/giub_geco/tting/global_dem_450m/aspect_1_1_deg/Copernicus_DSM_COG_10_N00_00_E012_00_DEM_to450m_aspect.nc")
# plot(r3)

