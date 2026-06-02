
# ~ 43 min on UBELIX
# ------------- Setup-----------------------------------------------------------

library(terra)
library(fs)

source(here::here("R/config.R"))
source(here::here("R/mosaic_tiles_gdal.R"))

ext <- ext(rast(twi_450m_mosaic_clean_path))
rast(dem_450m_mosaic_path)
# ------------- Mosaicing DEM------------------------------------------------------
tictoc::tic()

message("Mosaicing DEM...")
sw_in_mosaic <- mosaic_tiles_gdal(
  input_dir   = dem_450m_tiles_dir,
  output_file = dem_450m_mosaic_path,
  pattern = "*_15_arcscd.tif",
  extent = ext)

tictoc::toc()

# ------------- Mosaicing Rin------------------------------------------------------

tictoc::tic()
message("Mosaicing Rin...")
sw_in_mosaic <- mosaic_tiles_gdal(
  input_dir   = rin_450m_tiles_dir,
  output_file = rin_450m_mosaic_path,
  pattern = "*_15_arcscd.tif",
  extent = ext)
tictoc::toc()
