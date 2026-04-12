# ==============================================================================
# Purpose:
# Compute mean annual climatic variables from monthly WorldClim .tif files (1km)
# and zonal aggregate to 5km (0.05°):
#   - Mean Annual Temperature (MAT)  - monthly mean
#   - Annual Total Precipitation (MAP) - monthly sum
#   - Annual Total Solar Radiation (SRAD) - monthly sum

# Run time:
# ~132 mins on UBELIX
# ==============================================================================

# ------------- Setup ----------------------------------------------------------
library(terra)
source(here::here("R/config.R"))
source(here::here("R/raster_preprocess_save.R"))
source(here::here("R/create_aligned_template.R"))

# Create output directory
if (!dir.exists(dirname(mat_5km_file))) dir.create(dirname(mat_5km_file), recursive = TRUE)

# ------------- Process: MAT (mean), MAP & SRAD (sum) --------------------------
vars    <- c("tavg", "prec", "srad")
outputs <- c(mat_5km_file, map_5km_file, srad_5km_file)

# Create 5km template once (reused for all variables)
align_template_5km <- create_aligned_template(twi_450m_mosaic_clean_path)

for (i in seq_along(vars)) {
  var    <- vars[i]
  output <- outputs[i]

  # Monthly file paths: wc2.1_30s_{var}_01.tif to ..._12.tif
  months <- sprintf("%02d", 1:12)
  monthly_files <- paste0(worldclim_1km_dir, "wc2.1_30s_", var, "_", months, ".tif")

  # more info here:
  # https://api.rdocumentation.org/packages/geodata/versions/0.5-8/topics/worldclim

  # Load 12 months and compute annual statistic
  monthly_rasters <- rast(monthly_files)
  annual_result <- switch(var,
                          tavg = mean(monthly_rasters),  # °C, annual mean temperature
                          prec = sum(monthly_rasters),   # mm, annual total precipitation
                          srad = mean(monthly_rasters)   # kJ m⁻² day⁻¹, mean monthly daily radiation or annual mean daily solar radiation
  )

  # Save to temporary file
  temp_path <- tempfile(fileext = ".tif")
  writeRaster(annual_result, filename = temp_path, overwrite = TRUE)
  message(var, " -> ", basename(temp_path))

  # Align to 5km grid and aggregate zones
  raster_preprocess_save(
    input            = temp_path,
    output           = output,
    target           = align_template_5km,
    varname          = var,
    if_zonal         = TRUE,
    fun              = "mean",
    if_aggregate     = FALSE,
    if_resample      = FALSE,
    if_return_raster = FALSE
  )
}

# -------------------- Optional Check ---------------------------
# r <- rast(mat_5km_file)
# print(r)
# summary(r)
# plot(r, main = "Mean annual temperature (5km)")
#
# r <- rast(map_5km_file)
# print(r)
# summary(r)
# plot(r, main = "Mean annual precipitation (5km)")
#
# r <- rast(srad_5km_file)
# print(r)
# summary(r)
# plot(r, main = "Mean annual solar radiation (5km)")
