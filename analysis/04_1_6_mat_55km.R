# ==============================================================================
# Mean Annual Temperature (MAT) Aggregation: 1 km → 0.5°
#
# Purpose:
#   Compute the Mean Annual Temperature (MAT) from 12 monthly temperature rasters
#   and aggregate the result from high-resolution (30s ≈ 1 km) data
#   to 0.5° (≈55 km) for global analysis.
#
# Input:
#   - Monthly WorldClim temperature rasters (wc2.1_30s_tavg_01–12.tif)
#   - mi_55km_file : Target 0.5° grid for alignment/resampling
#
# Output:
#   - mat_55km_file: Aggregated 0.5° MAT raster
#
# Steps:
#   1. Setup environment
#   2. Load and process monthly rasters (compute MAT)
#   3. Aggregate to 0.5° using mean
#   4. (Optional) Visualize and check
#   5. Cleanup
# ==============================================================================

# -------------------- 1. Setup Environment ------------------------------------
library(terra)
source(here::here("config.R"))
source(here::here("R/raster_preprocess_save.R"))

message("🌍 Starting Mean Annual Temperature (MAT) aggregation...")

# -------------------- 2. Load & Compute MAT -----------------------------------
message("📦 Loading monthly WorldClim rasters and computing annual mean...")

# Path to monthly .tif files (WorldClim v2.1, 30s resolution)
folder_path <- "/data/archive/worldclim_fick_2017/data/"

# Generate filenames for 12 months (wc2.1_30s_tavg_01–12.tif)
months     <- sprintf("%02d", 1:12)
file_names <- paste0(folder_path, "wc2.1_30s_tavg_", months, ".tif")

# Load and compute annual mean temperature
monthly_rasters   <- rast(file_names)
annual_mean_temp  <- mean(monthly_rasters)
message("✅ Annual mean temperature calculated successfully.")

# Save temporary raster for aggregation
temp_path <- tempfile(fileext = ".tif")
writeRaster(annual_mean_temp, filename = temp_path, overwrite = TRUE)
message("📁 Temporary MAT raster written to: ", temp_path)

# -------------------- 3. Aggregation ------------------------------------------
message("🔧 Aggregating MAT raster to 0.5° resolution...")

r_out <- raster_preprocess_save(
  input            = temp_path,
  output           = mat_55km_file,
  target           = mi_55km_file,
  varname          = "mat",
  if_aggregate     = TRUE,
  if_resample      = TRUE,
  fun              = mean,
  if_return_raster = TRUE
)

message("✅ MAT aggregation completed successfully!")
message("📁 Output saved at: ", mat_55km_file)

# -------------------- 4. Optional Check ---------------------------------------
# Uncomment below to visually inspect results
# message("🔍 Checking MAT raster output...")
# r_out <- rast(mat_55km_file)
# print(r_out)
# plot(r_out, main = "Mean Annual Temperature (0.5°)")
# summary(r_out)
# message("✅ Visualization complete.")

# -------------------- 5. Cleanup ----------------------------------------------
message("🧹 Cleaning up temporary files and environment...")
unlink(temp_path)
rm(list = ls())
gc()
message("✅ Script finished successfully.")
