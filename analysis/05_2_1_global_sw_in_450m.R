# ------ Setup-------------------------------------------------

library(terra)
library(fs)
# library(here)
hostname <- trimws(tolower(system("hostname", intern = TRUE)))
if (hostname == "dash") {
  message("💻 Detected Workstation: dash → using config.R")
  source(here::here("config.R"))
} else {
  message("🖥️ Detected HPC environment (", hostname, ") → using config_ubelix.R")
  source(here::here("config_ubelix.R"))
}
source(here::here("R/mosaic_tiles.R"))

# File Configuration ------------------------------------------------------
# Output file paths
output_dir <- fs::path_dir(sw_in_450m_tile_dir)

# Load data ------------------------------------------------------

# Load target grid and create mask
twi_450m_r <- rast(twi_450m_mosaic_clean_path)

# Mosaicing ------------------------------------------------------

message("Starting mosaicing...")
sw_in_mosaic <- mosaic_tiles(
  input_dir   = sw_in_450m_tile_dir,
  output_file = NULL,
  pattern = "*_to_sw_in_uneven_450m.nc",
  varname = "sw_in")

# Check mosaicing process
if (is.null(sw_in_mosaic) || terra::ncell(sw_in_mosaic) == 0) {
  stop("Mosaicing failed - no data produced")
}
message("Mosaicing completed. Starting resample...")

# Resample, Calculation and Mask ------------------------------------------------------

# Resample to target grid
sw_in_resampled <- terra::resample(sw_in_mosaic, twi_450m_r, method = "bilinear")

rm(sw_in_mosaic)
gc()

# Apply land mask to remove ocean areas
sw_in_resampled <- mask(sw_in_resampled, twi_450m_r)

# Save Results ------------------------------------------------------------

# Save output files
message("Saving as GeoTIFF...")
terra::writeRaster(
  sw_in_resampled,
  sw_in_uneven_450m_path,
  filetype = "GTiff",
  gdal = c("COMPRESS=LZW", "BIGTIFF=YES", "TILED=YES", "BLOCKXSIZE=256", "BLOCKYSIZE=256"),
  overwrite = TRUE,
  datatype = "FLT4S",
  NAflag = -9999
)
if(file.exists(sw_in_uneven_450m_path)) message("✅ Saved: ", sw_in_uneven_450m_path)

rm(list = ls())
gc()

# check result
# sw_in <- rast(sw_in_uneven_450m_path)
# plot(sw_in)
