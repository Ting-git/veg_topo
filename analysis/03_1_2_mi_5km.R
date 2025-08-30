# ------Set up------------------------------------------------------------------
library(terra)

source(here::here("config.R"))
source(here::here("R/aggregate_byfile.R"))

# ------ Aggregation -----------------------------------------------------------

aggregate_byfile(
  input_path = mi_950m_file,
  output_path = mi_5km_file,
  target_path = cor_twi_vegh_mosaic_file,
  varname = "moisture_index",
  if_resample = TRUE,
  na_value = 0
)

# check the output
# r <- rast(mi_5km_file)
# plot(r)
# summary(r)
