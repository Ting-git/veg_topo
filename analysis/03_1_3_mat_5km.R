# ~ 6 min
# ------------ Set Up ----------------------------------------------------------
library(terra)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/raster_preprocess_save.R"))

# ------ Load Data and MAT calculateion ---------------------------------------------

# Set the path to the folder containing the monthly .tif files
folder_path <- "/data/archive/worldclim_fick_2017/data/"

# Generate the list of file names: wc2.1_30s_tavg_01.tif to wc2.1_30s_tavg_12.tif
months <- sprintf("%02d", 1:12)
file_names <- paste0(folder_path, "wc2.1_30s_tavg_", months, ".tif")

# Load all 12 monthly raster files into a single SpatRaster object
monthly_rasters <- rast(file_names)

# Compute the annual mean temperature (per pixel average)
annual_mean_temp <- mean(monthly_rasters)

# plot(annual_mean_temp, main = "Annual Mean Temperature (°C)")
# plot(annual_mean_temp)

# Write the raster to the temporary file
temp_path <- tempfile(fileext = ".tif")
writeRaster(annual_mean_temp, filename = temp_path, overwrite = TRUE)

# ------ Aggregation -----------------------------------------------------------

# Aggregation
raster_preprocess_save(
  input = temp_path,
  output = mat_5km_file,
  target = cor_twi_vegh_mosaic_file,
  varname = "mat",
  if_resample = TRUE
)

if(file.exists(mat_5km_file)) message("Saved: ", mat_5km_file)

# # check the output
# r2 <- rast(mat_5km_file)
# r2
# plot(r2)

unlink(temp_path)
