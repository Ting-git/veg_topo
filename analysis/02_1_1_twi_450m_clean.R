# ------Set up------------------------------------------------------------------

library(terra)

source(here::here("config.R"))

# ------Data Clean: 10 min--------------------------------------------------------------

# Load the raster
twi_r <- terra::rast(twi_450m_path)

# Replace -1 values with NA
twi_r[twi_r == -1] <- NA

# Expand the extent
# target_ext <- ext(-180, 180, -60, 90)
# twi_r <- terra::extend(twi_r, target_ext)

# Save to NetCDF
writeCDF(twi_r, file = twi_450m_mosaic_clean_path, overwrite = TRUE)
message("✅ Saved successfully to: ", twi_450m_mosaic_clean_path)

# Clean up memory
rm(twi_r)
gc()
