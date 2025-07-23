# ------------ Set Up ----------------------------------------------------------

library(terra)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/aggregate_byfile.R"))

# ------ Aggregation -----------------------------------------------------------

# load input data
ai_5km_r <- rast(ai_5km_file)
ai_5km_r

# Expand the extent
# target_ext <- ext(-180, 180, -60, 90)
# ai_5km_r <- terra::extend(ai_5km_r, target_ext)
# ai_5km_r

aggregate_byfile(
  input_path = ai_5km_file,
  output_path = ai_11km_file,
  xres_tar = 0.1,
  yres_tar = 0.1,
  varname = "aridity_index",
  if_resample = FALSE
)

# check the output
r2 <- rast(ai_11km_file)
r2

# ------ Cleanup ---------------------------------------------------------------
rm(list = ls())
gc()
