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

# Calculation and Mask ------------------------------------------------------
message("Loading preprocessed data...")
sw_in_450m <- terra::rast(sw_in_450m_path)
sw_in_flat_450m <- terra::rast(sw_in_flat_450m_path)

# 检查数据是否成功加载
if (!all(c(terra::nrow(sw_in_450m), terra::nrow(sw_in_flat_450m)) > 0)) {
  stop("Failed to load input files")
}

message("Calculating terrain effect (sw_in / sw_in_flat)...")
sw_in_terrain_effect <- sw_in_450m / sw_in_flat_450m

message("Applying land mask...")
sw_in_terrain_effect <- mask(sw_in_terrain_effect, twi_450m_r)
message("Calculation completed.")

# Save Results ------------------------------------------------------------

# Save output files
terra::writeCDF(sw_in_terrain_effect, sw_in_terrain_effect_path, overwrite = TRUE, varname = "sw_in_terrain_effect")
if(file.exists(sw_in_terrain_effect_path)) message("✅ Saved: ", sw_in_terrain_effect_path)

rm(list = ls())
gc()

# check result
# sw_in <- rast(sw_in_450m_path)
# plot(sw_in)
