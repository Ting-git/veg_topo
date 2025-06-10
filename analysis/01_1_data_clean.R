# ------Set up------------------------------------------------------------------

library(terra)

# ------Configuration-----------------------------------------------------------

twi_450m_path <- file.path("/data/archive/gti_marthews_2015/data/ga2.nc")  # Target raster file path
twi_450m_clean_path <- file.path("/data_2/scratch/ting/data/twi_marthew_450m/ga2_clean.nc")

# ------Data Clean--------------------------------------------------------------

# Load the raster
twi_r <- terra::rast(twi_450m_path)

# Replace -1 values with NA
twi_r[twi_r == -1] <- NA

# Save to NetCDF
writeCDF(twi_r, file = twi_450m_clean_path, overwrite = TRUE)
message("✅ Saved successfully to: ", twi_450m_clean_path)

# Clean up memory
rm(twi_r)
gc()
