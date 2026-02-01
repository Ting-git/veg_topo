# ==============================================================================
# DEM Standard Deviation Aggregation (450m → 0.5°)
#
# Purpose:
#   Aggregate high-resolution DEM (450m) data to 0.5° (55km) resolution by
#   calculating standard deviation (sd) within each coarse grid cell.
#
# Input:
#   - dem_450m_mosaic_path : High-resolution DEM raster
#   - mi_55km_file         : Target 0.5° grid (for alignment/resampling)
#
# Output:
#   - dem_sd_55km_path     : Aggregated DEM standard deviation raster (0.5°)
#
# Steps:
#   1. Load configuration and dependencies
#   2. Aggregate 450m DEM to 0.5° using sd
#   3. Resample to target grid alignment
#   4. (Optional) Visualize and verify
#   5. Cleanup
# ==============================================================================

# -------------------- 1. Setup Environment ------------------------------------
library(terra)

# Automatically select configuration file
hostname <- trimws(tolower(system("hostname", intern = TRUE)))
if (hostname == "dash") {
  message("💻 Detected Worksation: dash → using config.R")
  source(here::here("config.R"))
} else {
  message("🖥️ Detected HPC environment (", hostname, ") → using config_ubelix.R")
  source(here::here("config_ubelix.R"))
}

# other custom functions
source(here::here("R/raster_preprocess_save.R"))

message("Starting DEM standard deviation aggregation (450m → 0.5°)...")

# -------------------- 2. Aggregation & Resampling -----------------------------
message("Aggregating DEM using standard deviation (sd)...")
r_out <- raster_preprocess_save(
  input        = dem_450m_mosaic_path,
  output       = dem_sd_55km_path,
  target       = mi_55km_file,
  varname      = "dem_sd",
  if_aggregate = TRUE,
  if_resample  = TRUE,
  fun          = sd,
  if_return_raster = TRUE
)

message("✅ DEM standard deviation aggregation completed.")
message("Output file: ", dem_sd_55km_path)

# -------------------- 3. Optional Check ---------------------------------------
# Uncomment this section to verify results visually
# message("Checking input and output rasters...")
# r_in  <- rast(dem_450m_mosaic_path)
# r_out <- rast(dem_sd_55km_path)
# r_tar <- rast(mi_55km_file)
#
# print(r_in)
# print(r_out)
#
# par(mfrow = c(1, 2))
# plot(r_in,  main = "DEM (450m)")
# plot(r_out, main = "DEM SD (0.5°)")
# par(mfrow = c(1, 1))
#
# summary(r_out)
# message("✅ Visualization complete.")

# -------------------- 4. Cleanup ----------------------------------------------
message("Cleaning up environment...")
rm(list = ls())
gc()
message("✅ Script finished successfully.")
