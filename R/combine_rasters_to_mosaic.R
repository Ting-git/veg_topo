library(terra)
library(stringr)
library(here)
library(fs)
library(purrr)

source(here("R/aggregate_byfile.R"))
source(here("R/get_tif_files.R"))

# -------------------
# Configuration
# -------------------

# File paths
dir_vegh_450m <- here::here("data/vegheight_450m_products/")  # Path for saving modified data
dir_vegh_10m <- file.path("/data_2/vegheight_lang_2023/")  # Path for higher resolution data
file_ga2 <- "/data/archive/gti_marthews_2015/data/ga2.nc"  # File path for target raster

# Get .tif files from source directory
files_vegh_10m_all <- get_all_files(dir_vegh_10m)  # Assuming this function returns full file paths
files_vegh_10m <- files_vegh_10m_all[26:26]  # Select the 26th file for testing

# Apply function to each .tif file
purrr::walk(files_vegh_10m, ~aggregate_byfile(.x, terra::rast(file_ga2), dir_vegh_450m))

# Load processed rasters

files_vegh_450m <- get_all_files(dir_vegh_450m)  # Get processed .tif files
rasters_vegh_450m <- lapply(files_vegh_450m, terra::rast)  # Convert to SpatRaster objects

# Merge rasters into a single mosaic
raster_mosaic <- do.call(terra::merge, rasters_vegh_450m)

# Plot the mosaic raster
terra::plot(raster_mosaic)

file_mosaic <- file.path(here::here("data/"), "mosaic_vegh_450m.nc")

# Save the merged raster as a NetCDF file
terra::writeCDF(raster_mosaic, file_mosaic, overwrite = TRUE)


