# ==============================================================================
# Purpose:
# Compute mean annual climatic variables from monthly WorldClim .tif files (1km)
# and zonal aggregate to 450m (15 arcsec):
#   - Mean Annual Temperature (MAT)  - monthly mean
#   - Annual Total Precipitation (MAP) - monthly sum
#   - Annual Total Solar Radiation (SRAD) - monthly sum
# ==============================================================================

# ------------- Setup ----------------------------------------------------------
library(terra)
source(here::here("R/config.R"))
source(here::here("R/raster_preprocess_save.R"))
source(here::here("R/create_aligned_template.R"))

# Create output directory
if (!dir.exists(dirname(mat_450m_file))) dir.create(dirname(mat_450m_file), recursive = TRUE)

# ------------- Process: MAT (mean), MAP & SRAD (sum) --------------------------
vars    <- c("tavg", "prec", "srad")
outputs <- c(mat_450m_file, map_450m_file, srad_450km_file)

# Create 5km template once (reused for all variables)
align_template_450m <- rast(twi_450m_mosaic_clean_path)

for (i in seq_along(vars)) {
  var    <- vars[i]
  output <- outputs[i]

  # Monthly file paths: wc2.1_30s_{var}_01.tif to ..._12.tif
  months <- sprintf("%02d", 1:12)
  monthly_files <- paste0(worldclim_1km_dir, "wc2.1_30s_", var, "_", months, ".tif")

  # Load 12 months and compute annual statistic
  monthly_rasters <- rast(monthly_files)
  annual_result <- if (var == "tavg") mean(monthly_rasters) else sum(monthly_rasters)

  message("Resampling...")
  r_out <- terra::resample(monthly_rasters, align_template_450m, method = "bilinear")

  message("Saving...")
  terra::writeRaster(
    r_out,
    output,
    filetype  = "GTiff",
    gdal      = c("COMPRESS=LZW", "BIGTIFF=YES", "TILED=YES", "BLOCKXSIZE=256", "BLOCKYSIZE=256"),
    overwrite = overwrite,
    datatype  = "FLT4S",
    NAflag    = -9999
  )
  if (file.exists(output_file)) message("✅ Saved: ", output_file)
}
