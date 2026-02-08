# ==============================================================================
# Ecoregion Vector to Raster Pipeline (5km)
#
# Purpose:
#   Convert ecoregion vector polygons to a raster at 5km resolution
#   aligned with a reference raster.
#
# Input:
#   - ecoregion_path           : Vector shapefile of ecoregions
#   - cor_twi_vegh_mosaic_file : Reference raster for alignment/resampling
#
# Output:
#   - ecoregion_5km_path       : Rasterized ecoregion NetCDF file
# ==============================================================================

# -------------------- 1. Setup Environment ------------------------------------
library(terra)
library(sf)

# Load configuration and helper functions
source(here::here("config.R"))

# Create output directory
if (!dir.exists(dirname(ecoregion_5km_path))) dir.create(dirname(ecoregion_5km_path), recursive = TRUE)
message("✅  Output directory:", dirname(ecoregion_5km_path))

message("🌍 Starting ecoregion rasterization...")
# -------------------- 2. Rasterization ----------------------------------------
# Read target raster (for matching resolution/extent)
r_tar <- rast(cor_twi_vegh_mosaic_file)[[1]]

# Read ecoregion vector file
v <- vect(ecoregion_path)

# Create empty raster template with target extent, resolution, and CRS
r_template <- rast(ext(r_tar), resolution = 0.05, crs = crs(r_tar))

# Rasterize vector polygons using 'BIOME_NUM' field
r_rasterized <- rasterize(v, r_template, field = "BIOME_NUM")

# -------------------- 3. Resample to Target Raster ----------------------------
r_resampled <- terra::resample(r_rasterized, r_tar, method = "bilinear")

# -------------------- 4. Save Raster ------------------------------------------
terra::writeCDF(r_resampled, ecoregion_5km_path, overwrite = TRUE, varname = "BIOME_NUM")

if (file.exists(ecoregion_5km_path)) {
  message("📁 Saved: ", ecoregion_5km_path)
} else {
  message("❌ Failed to save ecoregion raster.")
}

# -------------------- 5. Optional Check ---------------------------------------
# Uncomment below to inspect raster
# plot(r_tar, main = "Reference Raster")
# plot(r_rasterized, main = "Rasterized Ecoregions")
# freq_table <- freq(r_rasterized)
# print(freq_table)
# message("✅ Visualization and frequency table complete.")
