# ==============================================================================
# annual_mean_temperature_pipeline.R
#
# Computes annual mean temperature (MAT) from monthly WorldClim .tif files
# and preprocesses/resamples to target raster resolution.
# Steps:
#   1. Setup environment
#   2. Load monthly data and calculate annual mean
#   3. Save temporary raster
#   4. Resample and save final output
# ==============================================================================

# ------------ Set Up ----------------------------------------------------------
library(terra)
library(here)

# Load configuration and helper functions
source(here::here("config.R"))
source(here::here("R/raster_preprocess_save.R"))

# ------ Load Data and Compute Annual Mean Temperature -------------------------

# Folder containing the monthly .tif files
folder_path <- "/data/archive/worldclim_fick_2017/data/"

# Generate file names: wc2.1_30s_tavg_01.tif to wc2.1_30s_tavg_12.tif
months <- sprintf("%02d", 1:12)
file_names <- paste0(folder_path, "wc2.1_30s_tavg_", months, ".tif")

# Load all 12 monthly rasters into a single SpatRaster object
monthly_rasters <- rast(file_names)

# Compute the annual mean temperature (per-pixel mean)
annual_mean_temp <- mean(monthly_rasters)

# Save to a temporary file
temp_path <- tempfile(fileext = ".tif")
writeRaster(annual_mean_temp, filename = temp_path, overwrite = TRUE)
message("Temporary annual mean raster saved: ", temp_path)

# ------ Aggregation / Preprocess to Target -----------------------------------

raster_preprocess_save(
  input       = temp_path,
  output      = mat_5km_file,
  target      = cor_twi_vegh_mosaic_file,
  varname     = "mat",
  if_aggregate = TRUE,
  if_round_fact = TRUE,
  if_resample = TRUE
)

# Clean up temporary file
unlink(temp_path)
