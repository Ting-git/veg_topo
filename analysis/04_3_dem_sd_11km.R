library(terra)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/aggregate_byfile.R"))

# ------ Aggregation ---------------------------------------------
# check the input
r1 <- rast(dtm_sd_11km_path)
r1

r2 <- rast(ai_11km_file)
r2

aggregate_byfile(
  input_path = dtm_sd_11km_path,
  output_path = dtm_sd_11km_re_path,
  target_path = ai_11km_file,
  varname = "dem_sd",
  if_resample = TRUE
)

# check the output
r3 <- rast(dtm_sd_11km_re_path)
r3
# ------ Cleanup ---------------------------------------------------------------
rm(list = ls())
gc()
