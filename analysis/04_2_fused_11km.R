# ------------ Set Up ----------------------------------------------------------
library(terra)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/aggregate_byfile.R"))

# ------ Aggregation ---------------------------------------------
# check the output
r1 <- rast(fused_5km_file)
r1

aggregate_byfile(
  input_path = fused_5km_file,
  output_path = fused_11km_file,
  target_path = ai_11km_file,
  varname = "fused",
  if_resample = TRUE
)

# check the output
r2 <- rast(fused_11km_file)
r2

# ------ Cleanup ---------------------------------------------------------------
rm(list = ls())
gc()
