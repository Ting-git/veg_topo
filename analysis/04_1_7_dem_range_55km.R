library(terra)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/aggregate_byfile.R"))

# ------ Load and calculation --------------------------------------------------
max_r <- rast(dem_max_10km_path)
min_r <- rast(dem_min_10km_path)

range_r <- max_r - min_r

# Write the raster to the temporary file
temp_path <- tempfile(fileext = ".tif")
writeRaster(range_r, filename = temp_path, overwrite = TRUE)

# ------ Aggregation -----------------------------------------------------------

# Aggregation
aggregate_byfile(
  input_path = temp_path,
  output_path = dem_rg_55km_path,
  target_path = ai_55km_file,
  varname = "dem_range",
  if_resample = TRUE
)


# check the output
# r3 <- rast(dem_rg_55km_path)
# r3
# plot(r3)

unlink(temp_path)
