# ---------------------------------------
# Set up
# ---------------------------------------
library(terra)
library(stringr)
library(here)
library(fs)
library(purrr)
library(furrr)
library(progress)
library(progressr)
library(cli)

source(here::here("R/aggregate_byfile.R"))

# ---------------------------------------
# Configuration
# ---------------------------------------
# Define file paths
dir_vegh_450m <- "/data_2/scratch/ting/vegh_450m/3_3_deg/"  # Path for saving modified data
dir_vegh_10m <- "/data_2/archive/vegheight_lang_2023/data/3deg_cogs/"  # Path for higher resolution data
file_ga2 <- "/data/archive/gti_marthews_2015/data/ga2.nc"  # Target raster file path
file_vegh_mosaic <- file.path(dirname(dir_vegh_450m), "vegh_450m_2020_mosaic2.nc")

# ---------------------------------------
# Aggregation (vegh 10m --> vegh 450m)
# ---------------------------------------

# Get .tif files from source directory
files_vegh_10m_all <- fs::dir_ls(path = dir_vegh_10m, glob = "*_Map.tif")
files_vegh_10m <- files_vegh_10m_all[2300:2651]  # Process a subset of files (for manual work division)


gc()
# Set up parallel processing (adjust based on available CPU cores)
plan(multisession, workers = 4)

# Enable progress bar
handlers("cli")

# Progress bar tracking
with_progress({
  pb <- progressor(along = files_vegh_10m)  # Create progress bar

  results <- future_map(files_vegh_10m, safely(~{

    # future_map() 使用 furrr 进行 并行计算，默认情况下，
    # 每个 worker 进程都是独立的 R 进程，不会自动继承主进程的环境变量
    rast_tar <- terra::rast(file_ga2)
    # Aggregate raster files
    result <- aggregate_byfile(.x, rast_tar, dir_vegh_450m)
    # Update progress bar at each step
    pb()
    return(result)  # Return result for each processed file
  }))
})

# Switch back to single-threaded execution after completion
plan(sequential)

rm(rast_tar)  # Remove the list of rasters to free memory
gc()  # Trigger garbage collection to release memory

# ---------------------------------------------------
# Combination (Merge netCDF files into a mosaic)
# ---------------------------------------------------

# Load processed rasters
files_vegh_450m <- fs::dir_ls(path = dir_vegh_450m, glob = "*_to450m.nc")
rasters_vegh_450m <- lapply(files_vegh_450m, terra::rast)  # Convert to SpatRaster objects

# lapply(rasters_vegh_450m[2639:2640], function(r) list(dim(r), crs(r), ext(r)))

# Merge rasters into a single mosaic
raster_mosaic <- do.call(terra::merge, list(rasters_vegh_450m))

# Clear the loaded rasters from memory after merging
rm(rasters_vegh_450m)  # Remove the list of rasters to free memory
gc()  # Trigger garbage collection to release memory

# Save the merged raster as a NetCDF file
terra::writeCDF(raster_mosaic, file_vegh_mosaic, overwrite = TRUE)

# Clear the merged raster from memory after saving it
rm(raster_mosaic)  # Remove the merged raster
gc()  # Trigger garbage collection again to release memory

