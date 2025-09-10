# ------ Setup-------------------------------------------------

library(terra)
library(fs)
# library(here)

source(here::here("R/mosaic_tiles.R"))

# File Configuration ------------------------------------------------------

sw_in_450m_tile_dir <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/1_1_deg_tiles")
twi_450m_mosaic_clean_path <- file.path("/storage/scratch/giub_geco/tting/global_twi_450m_clean/ga2_clean.nc")

# Output file paths
output_dir <- fs::path_dir(sw_in_450m_tile_dir)
sw_in_450m_path <- file.path(output_dir, "sw_in_450m.nc")
sw_in_flat_450m_path <- file.path(output_dir, "sw_in_flat_450m.nc")
sw_in_terrain_effect_path <- file.path(output_dir, "sw_in_terrain_effect_450m.nc")

# Load data ------------------------------------------------------

# Load target grid and create mask
twi_450m_r <- rast(twi_450m_mosaic_clean_path)
land_mask <- !is.na(twi_450m_r)  # Create mask from non-NA values

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

rm(sw_in_flat_mosaic)  # 立即清理镶嵌数据
gc()

message("Applying land mask...")
sw_in_flat_resampled <- mask(sw_in_flat_resampled, land_mask)
message("Masking completed.")

# Save Results ------------------------------------------------------------

# Save output files
terra::writeCDF(sw_in_flat_resampled, sw_in_flat_450m_path, overwrite = TRUE, varname = "sw_in_flat")
if(file.exists(sw_in_flat_450m_path)) message("✅ Saved: ", sw_in_flat_450m_path)

rm(list = ls())
gc()

# check result
# sw_in <- rast(sw_in_450m_path)
# plot(sw_in)
