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
message("Starting mosaicing for flat earth radiation...")
sw_in_flat_mosaic <- mosaic_tiles(
  input_dir   = sw_in_450m_tile_dir,
  output_file = NULL,
  pattern = "*_to_sw_in_flat_450m.nc",
  varname = "sw_in_flat")
message("Mosaicing completed.")

# Resample, Calculation and Mask ------------------------------------------------------
message("Starting resampling...")
sw_in_flat_resampled <- terra::resample(sw_in_flat_mosaic, twi_450m_r, method = "bilinear")
message("Resampling completed.")

rm(sw_in_flat_mosaic)
gc()

message("Applying land mask...")
sw_in_flat_resampled <- mask(sw_in_flat_resampled, twi_450m_r)
message("Masking completed.")

# Save Results ------------------------------------------------------------

# Save output files
message("Saving as GeoTIFF...")
terra::writeRaster(
  sw_in_flat_resampled,
  sw_in_flat_450m_path,
  filetype = "GTiff",
  gdal = c("COMPRESS=LZW", "BIGTIFF=YES", "TILED=YES", "BLOCKXSIZE=256", "BLOCKYSIZE=256"),
  overwrite = TRUE,
  datatype = "FLT4S",
  NAflag = -9999
)
if(file.exists(sw_in_flat_450m_path)) message("✅ Saved: ", sw_in_flat_450m_path)

rm(list = ls())
gc()

# check result
# sw_in <- rast(sw_in_450m_path)
# plot(sw_in)
