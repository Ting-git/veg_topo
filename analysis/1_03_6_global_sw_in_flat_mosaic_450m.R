# ~ 41 min on UBELIX
# ------------- Setup-----------------------------------------------------------

library(terra)
library(fs)

source(here::here("R/config.R"))
source(here::here("R/mosaic_tiles.R"))

# ------------- Mosaicing ------------------------------------------------------
tictoc::tic()

message("Mosaicing flat earth incoming radiation...")
sw_in_mosaic <- mosaic_tiles(
  input_dir   = sw_in_flat_450m_tile_dir,
  output_file = sw_in_flat_450m_path,
  pattern = "*_to_sw_in_flat_450m.nc",
  target_grid = twi_450m_mosaic_clean_path,
  if_resample = TRUE)

tictoc::toc()
