# done [25.1 min]

# ------Load required libraries-------------------------------------------------

library(terra)     # For handling raster data
library(furrr)
library(dplyr)

# ------Load configuration and helper functions---------------------------------

source(here::here("config.R"))
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/calculate_correlation_bywin.R"))
source(here::here("R/mosaic_tiles.R"))

# ------Analysis---------------------------------------------

# read the information of valid tiles
tiles_info <- readRDS(valid_tiles_info_path)

# ------------parallel process for each tiles-----------------------------------

# Clear memory and set up parallel processing
gc()
plan(multisession, workers = 8)
t0 <- Sys.time()

# Safest approach - use list indexing
results <- future_pmap(
  tiles_info,
  function(...) {
    args <- list(...)
    tryCatch({
      tile_id <- args$tile_id
      tile_extent <- terra::ext(args$xmin, args$xmax, args$ymin, args$ymax)

      # -------Data Pre------------
      # Load rasters
      twi_r <- rast(twi_450m_mosaic_clean_path)
      vegh_r <- rast(vegh_450m_mosaic_path)

      # Crop to tile
      twi_rc <- crop(twi_r, tile_extent)
      vegh_rc <- crop(vegh_r, tile_extent)

      # Stack and name layers
      stacked <- c(twi_rc, vegh_rc)
      names(stacked) <- c("twi", "vegh")

      # Clean up temporary rasters
      rm(twi_r, vegh_r, twi_rc, vegh_rc)
      gc()

      # Create windows and calculate correlation
      df_cor  <- create_spatial_windows(stacked) |>
        calculate_correlation_bywin()

      colnames(df_cor)
      # ------5km output-------
      cor_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "correlation")], type = "xyz", crs = "EPSG:4326")
      cor_nc_path <- file.path(cor_twi_vegh_tiles_dir,
                               paste0("r_H_TWI_5km_", tile_id, "_map.nc"))
      terra::writeCDF(cor_r, cor_nc_path, varnames = "r_H_TWI", overwrite = TRUE)
      if(file.exists(cor_nc_path)) message(paste0("Saved:", cor_nc_path))

      pval_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "cor_pval")], type = "xyz", crs = "EPSG:4326")
      pval_nc_path <- file.path(cor_twi_vegh_tiles_dir,
                                paste0("r_H_TWI_5km_", tile_id, "_pval.nc"))
      terra::writeCDF(pval_r, pval_nc_path, varnames = "r_H_TWI_pval", overwrite = TRUE)
      if(file.exists(pval_nc_path)) message(paste0("Saved:", pval_nc_path))

      # peak_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "peak")], type = "xyz", crs = "EPSG:4326")
      # nc_path <- file.path(cor_twi_vegh_tiles_dir,
      #                      paste0("r_H_TWI_5km_", tile_id, "_peak.nc"))
      # terra::writeCDF(pval_r, nc_path, varnames = "r_H_TWI_peak", overwrite = TRUE)

      rm(df_cor, cor_r, pval_r)
      gc()

    }, error = function(e) {
      msg <- sprintf("Tile %s failed: %s", args$tile_id %||% "unknown", conditionMessage(e))
      message("❌ ", msg)
      return(list(success = FALSE, error = msg))
    })
  },
  .options = furrr_options(seed = TRUE)
)

plan(sequential)
gc()

# Print processing time
message(sprintf("done [%.1f min]", difftime(Sys.time(), t0, units = "mins")))

# -------- Combination ---------------------------------------------------------


# mosacing the r(H~TWI) map
mosaic_tiles(
  input_dir   = cor_twi_vegh_tiles_dir,
  output_file = cor_twi_vegh_mosaic_file,
  pattern = "*_map.nc",
  varname = "correlation")

# mosacing the pval ofr(H~TWI) map
mosaic_tiles(
  input_dir   = cor_twi_vegh_tiles_dir,
  output_file = pval_cor_twi_vegh_mosaic_file,
  pattern = "*_pval.nc",
  varname = "cor_pval")


# ---------- Delete intermediate data ------------------------------------------
# List all files in the directory cor_twi_vegh_tiles_dir that match "*_to450m.nc"
# If there are any files found, delete them

# cor_5km_tiles_path <- fs::dir_ls(path = cor_twi_vegh_tiles_dir, glob = "*_to450m.nc")
# if (length(cor_5km_tiles_path) > 0) file.remove(cor_5km_tiles_path)


