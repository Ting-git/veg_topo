# ------Set up------------------------------------------------------------------------
library(terra)

source(here::here("config.R"))
source(here::here("R/aggregate_byfile.R"))

# ------ Aggregation ---------------------------------------------
aggregate_byfile(
  input_path = ai_950m_file,
  output_path = ai_5km_file,
  target_path = cor_twi_vegh_mosaic_file,
  if_resample = TRUE
)


