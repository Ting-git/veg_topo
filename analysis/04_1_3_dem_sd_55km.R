library(terra)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/aggregate_byfile.R"))

# ------ Aggregation ---------------------------------------------

# Aggregation
aggregate_byfile(
  input_path = dem_sd_10km_path,
  output_path = dem_sd_55km_path,
  target_path = ai_55km_file,
  varname = "dem_sd",
  if_resample = TRUE
)

# # check the input
# r1 <- rast(dem_sd_10km_path)
# r1
#
# r2 <- rast(ai_55km_file)
# r2
#
# # check the output
# r3 <- rast(dem_sd_55km_path)
# r3
#
# plot(r3)
# ------ Cleanup ---------------------------------------------------------------
rm(list = ls())
gc()
