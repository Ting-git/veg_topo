# ------------ Set Up ----------------------------------------------------------

library(terra)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/aggregate_byfile.R"))

# ------ Aggregation -----------------------------------------------------------

# aggregation
aggregate_byfile(
  input_path = mi_5km_file,
  output_path = mi_55km_file,
  xres_tar = 0.5,
  yres_tar = 0.5,
  varname = "moisture_index",
  if_resample = FALSE,
  fun = mean
)

# # check the input
# ai_5km_r <- rast(mi_5km_file)
# ai_5km_r
#
# # check the output
# r2 <- rast(mi_55km_file)
# r2
# plot(r2)

# ------ Cleanup ---------------------------------------------------------------
rm(list = ls())
gc()
