# ==============================================================================
# Protected Areas (PA) Rasterization and Aggregation (0.02° → 0.5°)
#
# Purpose:
#   1. Convert multiple PA shapefiles into 0.02° rasters
#   2. Merge them using GDAL (max overlap)
#   3. Aggregate to 0.5° resolution to represent PA coverage fraction
#
# Input:
#   - pa_shp0, pa_shp1, pa_shp2 : Shapefiles of protected areas (from config.R)
#   - mi_55km_file              : Target 0.5° grid for alignment
#
# Output:
#   - fpa_55km_path             : Fraction of protected area (0.5° resolution)
#
# Steps:
#   1. Setup environment and paths
#   2. Rasterize shapefiles with gdal_rasterize
#   3. Merge rasters using gdal_calc.py (maximum overlap)
#   4. Aggregate to 0.5° grid (fraction of coverage)
#   5. (Optional) Check and visualize
#   6. Cleanup
# ==============================================================================

# -------------------- 1. Setup Environment ------------------------------------
library(terra)
# library(rnaturalearth) # optional if using shapefile data

source(here::here("config.R"))
source(here::here("R/raster_preprocess_save.R"))

message("Starting PA rasterization and aggregation process...")

# -------------------- 2. Define Input & Temp Paths ----------------------------
message("Configuring file paths...")

# Shapefiles of protected areas
shp_files <- c(pa_shp0, pa_shp1, pa_shp2)

# Temporary directory for intermediate rasters
temp_dir <- tempdir()
out_files <- file.path(temp_dir, paste0("pa_", 0:2, ".tif"))
pa_merged_22km_path <- file.path(temp_dir, "pa_merged_22km.tif")

# -------------------- 3. Rasterize Shapefiles ---------------------------------
message("Rasterizing shapefiles to 0.02° grid using GDAL...")

rasterize_shp <- function(shp_path, out_tif) {
  cmd <- sprintf(
    "gdal_rasterize -burn 1 -tr 0.02 0.02 -te -180 -60 180 90 -ot Byte -of GTiff '%s' '%s'",
    shp_path, out_tif
  )
  cat("Running:", cmd, "\n")
  system(cmd)
}

# Apply rasterization to all shapefiles
mapply(rasterize_shp, shp_files, out_files)

# -------------------- 4. Merge Rasters ----------------------------------------
message("Merging rasterized shapefiles using gdal_calc.py...")

merge_cmd <- sprintf(
  paste(
    "gdal_calc.py",
    "-A '%s'",
    "-B '%s'",
    "-C '%s'",
    "--outfile='%s'",
    "--calc=\"maximum(A,B,C)\"",
    "--NoDataValue=0",
    "--overwrite"
  ),
  out_files[1], out_files[2], out_files[3], pa_merged_22km_path
)

cat("Running:", merge_cmd, "\n")
system(merge_cmd)

message("✅ Raster merging completed.")
message("Intermediate merged raster saved at: ", pa_merged_22km_path)

# -------------------- 5. Aggregate to 0.5° Resolution -------------------------
message("Aggregating merged PA raster to 0.5° grid (fraction coverage)...")

r_out <- raster_preprocess_save(
  input        = pa_merged_22km_path,
  output       = fpa_55km_path,
  target       = mi_55km_file,
  varname      = "fpa",
  if_aggregate = TRUE,
  if_resample  = TRUE,
  fun = function(x, na.rm) {
    if (all(is.na(x))) {
      return(0)
    } else {
      return(sum(x == 1, na.rm = na.rm) / length(x))
    }
  },
  if_return_raster = TRUE
)

message("✅ Aggregation completed successfully.")
message("Output file: ", fpa_55km_path)

# -------------------- 6. Optional Check ---------------------------------------
# Uncomment to inspect results
# message("Checking output raster...")
# r_out <- rast(fpa_55km_path)
# print(r_out)
# plot(r_out, main = "Fraction of Protected Area (0.5°)")
# message("✅ Visualization complete.")

# -------------------- 7. Cleanup ----------------------------------------------
message("Cleaning up environment...")
rm(list = ls())
gc()
message("✅ Script finished successfully.")
