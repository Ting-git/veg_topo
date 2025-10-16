# ---- Setup -------------------------------------------------------------------

# Load required libraries
library(terra)    # for modern spatial raster/vector operations
library(sf)       # for reading/writing vector spatial data

# Load configuration and helper functions
source(here::here("config.R"))

# ---- Vector to Raster -------------------------------------------------------------------
# Read target raster (for matching resolution/extent)
r_tar <- rast(cor_twi_vegh_mosaic_file)[[1]]

# Read ecoregion vector file
v <- vect(ecoregion_path)

# Create empty raster template with given extent, resolution, and CRS
r <- rast(ext(r_tar), resolution = 0.05, crs = crs(r_tar))

# Rasterize vector polygons using BIOME_NUM field
r_rasterized <- rasterize(v, r, field = "BIOME_NUM")

# Resample rasterized data to match target raster using bilinear interpolation
r_resampled <- terra::resample(r_rasterized, r_tar, method = "bilinear")

# Write output raster to NetCDF format
terra::writeCDF(r_resampled, ecoregion_5km_path, overwrite = TRUE, varname = "BIOME_NUM")

if(file.exists(ecoregion_5km_path)) message("Saved: ", ecoregion_5km_path)
# ---- Check Raster -------------------------------------------------------------------

plot(r_tar)
plot(r_rasterized)

# freq_table <- freq(r_rasterized)
# print(freq_table)
