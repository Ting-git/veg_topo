# ------Set up------------------------------------------------------------------
library(terra)

source(here::here("config.R"))
source(here::here("R/raster_preprocess_save.R"))

# ------ Aggregation -----------------------------------------------------------

raster_preprocess_save(
  input = mi_950m_file,
  output = mi_5km_file,
  target = cor_twi_vegh_mosaic_file,
  varname = "moisture_index",
  if_resample = TRUE,
  na_value = 0,
  if_return_raster = FALSE
)

# check the output
# r <- rast(mi_5km_file)
# plot(r)
# summary(r)
