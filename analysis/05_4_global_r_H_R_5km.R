# done [25.1 min] with 8 cores on workstation2
# done [6.5 min] with 49 cores on UBELIX

# ------Load required libraries-------------------------------------------------

library(terra)     # For handling raster data
library(furrr)
library(dplyr)

# ------Load configuration and helper functions---------------------------------

# source(here::here("config.R"))
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/calculate_correlation_bywin.R"))
source(here::here("R/mosaic_tiles.R"))

# ------File Configuration------------------------------------------------------

valid_tiles_info_path <- here::here("data/valid_tiles_info.rds")
vegh_450m_mosaic_path <- file.path("/storage/scratch/giub_geco/tting/global_vegh_450m/vegh_450m_2020_mosaic.nc")
sw_in_terrain_effect_path <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_terrain_effect_450m.nc")

# output files
r_H_R_tiles_dir <- file.path("/storage/scratch/giub_geco/tting/global_r_H_R_5km/30_30_deg_tiles")
if (!dir.exists(r_H_R_tiles_dir)) {
  dir.create(r_H_R_tiles_dir, recursive = TRUE)
}

r_H_R_5km_path <- file.path("/storage/scratch/giub_geco/tting/global_r_H_R_5km/r_H_R_5km.nc")
pval_r_H_R_5km_path <- file.path("/storage/scratch/giub_geco/tting/global_r_H_R_5km/pval_r_H_R_5km.nc")

# ------Analysis---------------------------------------------
# read the information of valid tiles
tiles_info <- readRDS(valid_tiles_info_path)

# ------------parallel process for each tiles-----------------------------------

# Clear memory and set up parallel processing
gc()

available_cores <- parallelly::availableCores()
plan(multisession, workers = available_cores)
message("Using all ", available_cores, " available cores")

t0 <- Sys.time()

# Safest approach - use list indexing
results <- future_pmap(
  tiles_info,
  function(...) {
    args <- list(...)
    tryCatch({
      tile_id <- args$tile_id
      tile_extent <- terra::ext(args$xmin, args$xmax, args$ymin, args$ymax)

      # -------Data Preprocessing------------
      # Load rasters
      terrain_effect_r <- rast(sw_in_terrain_effect_path)  # Replaced TWI with terrain effect
      vegh_r <- rast(vegh_450m_mosaic_path)

      # Crop to tile
      terrain_effect_rc <- crop(terrain_effect_r, tile_extent)  # Replaced TWI with terrain effect
      vegh_rc <- crop(vegh_r, tile_extent)

      # Stack and name layers
      stacked <- c(terrain_effect_rc, vegh_rc)  # Changed order: terrain effect first
      names(stacked) <- c("terrain_effect", "vegh")  # Updated layer names

      # Clean up temporary rasters
      rm(terrain_effect_r, vegh_r, terrain_effect_rc, vegh_rc)
      gc()

      # Create windows and calculate correlation
      df_cor  <- create_spatial_windows(stacked) |>
        calculate_correlation_bywin()

      colnames(df_cor)
      # ------5km output-------
      cor_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "correlation")], type = "xyz", crs = "EPSG:4326")
      cor_nc_path <- file.path(r_H_R_tiles_dir,
                               paste0("r_H_R_5km_", tile_id, "_map.nc"))  # Updated filename
      terra::writeCDF(cor_r, cor_nc_path, varnames = "r_H_R", overwrite = TRUE)  # Updated varname
      if(file.exists(cor_nc_path)) message(paste0("Saved:", cor_nc_path))

      pval_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "cor_pval")], type = "xyz", crs = "EPSG:4326")
      pval_nc_path <- file.path(r_H_R_tiles_dir,
                                paste0("r_H_R_5km_", tile_id, "_pval.nc"))  # Updated filename
      terra::writeCDF(pval_r, pval_nc_path, varnames = "r_H_R_pval", overwrite = TRUE)  # Updated varname
      if(file.exists(pval_nc_path)) message(paste0("Saved:", pval_nc_path))

      rm(df_cor, cor_r, pval_r)
      gc()

    }, error = function(e) {
      msg <- sprintf("Tile %s failed at %s: %s",
                     args$tile_id %||% "unknown",
                     Sys.time(),
                     conditionMessage(e))
      message("❌ ", msg)
      return(list(success = FALSE, error = msg, tile_id = args$tile_id))
    })
  },
  .options = furrr_options(seed = TRUE)
)

plan(sequential)
gc()

# Print processing time
message(sprintf("done [%.1f min]", difftime(Sys.time(), t0, units = "mins")))

# -------- Combination ---------------------------------------------------------

# mosaicing the r(H~R) map (slope aspect Terrain Effect vs Vegetation Height)
mosaic_tiles(
  input_dir   = r_H_R_tiles_dir,
  output_file = r_H_R_5km_path,
  pattern = "*_map.nc",
  varname = "r_H_R")

# mosaicing the pval of r(H~R) map
mosaic_tiles(
  input_dir   = r_H_R_tiles_dir,
  output_file = pval_r_H_R_5km_path,
  pattern = "*_pval.nc",
  varname = "r_H_R_pval")

# ---------- Delete intermediate data ------------------------------------------
# List all files in the directory r_H_R_tiles_dir that match "*.nc"
# If there are any files found, delete them

# cor_5km_tiles_path <- fs::dir_ls(path = r_H_R_tiles_dir, glob = "*.nc")
# if (length(cor_5km_tiles_path) > 0) file.remove(cor_5km_tiles_path)

# check the output
# r_H_R_5km_path <- file.path("/storage/scratch/giub_geco/tting/global_r_H_R_5km/r_H_R_5km.nc")
# pval_r_H_R_5km_path <- file.path("/storage/scratch/giub_geco/tting/global_r_H_R_5km/pval_r_H_R_5km.nc")
# plot(terra::rast(r_H_R_5km_path))
# plot(terra::rast(pval_r_H_R_5km_path))
