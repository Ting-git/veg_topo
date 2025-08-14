# ------------ Set Up ----------------------------------------------------------
library(terra)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/aggregate_byfile.R"))

# ------ Aggregation ---------------------------------------------

# Aggregation
aggregate_byfile(
  input_path = fwi_5km_file,
  output_path = fwi_55km_file,
  target_path = ai_55km_file,
  varname = "fused",
  if_resample = TRUE
)

# # check the input
# r1 <- rast(fused_5km_file)
# r1
#
# # check the output
# r2 <- rast(fused_55km_file)
# r2
#
# plot(r2)

