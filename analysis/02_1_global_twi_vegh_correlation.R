# done [1569.1s] there are 7 tiles failed due to NULL twi or vegh layer!!!
# these need to modify the preprocess function to exclude these 7 tiles as well

# ------Load required libraries-------------------------------------------------------------

library(terra)     # For handling raster data
library(furrr)
library(dplyr)
# ------Load configuration and helper functions---------------------------------------------

source(here::here("config.R"))
source(here::here("R/split_window_analysis.R"))

# ------Analysis---------------------------------------------

# read the information of valid tiles
tiles_info <- readRDS(tiles_info_path)

# # -----------single test--------------------------
# tile_id <- as.character(tiles_info[1, 1])
# xmin <- as.numeric(tiles_info[1, 2])
# xmax <- as.numeric(tiles_info[1, 3])
# ymin <- as.numeric(tiles_info[1, 4])
# ymax <- as.numeric(tiles_info[1, 5])
# premerg_file <- as.character(tiles_info[1, 6])
#
#
# # -------Load pre-processed raster------------
# premerg_r <- terra::rast(premerg_file)
#
# df_cor <- windows_cor_analysis(premerg_r) # grouped and nested df!!!

# df_data <- df_cor |>
#   tidyr::unnest(cols = c(data)) |>  # Explicit cols parameter
#   ungroup()
#
# # ------Convert to coarser raster (5km) and save output-------
# cor_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "correlation", "cor_pval")], type = "xyz", crs = "EPSG:4326")
# names(cor_r) <- c("correlation", "cor_pval")
# nc_path <- file.path(cor_twi_vegh_tiles_dir, paste0("cor_twi_vegh_5km_", tile_id, ".nc"))
# terra::writeCDF(cor_r, nc_path, overwrite = TRUE)
#
# # ------Convert to finer raster (450m) and save output-------
# # Create raster from dataframe
# cor_r_2 <- terra::rast(
#   df_data[, c("lon", "lat", "lon_mid", "lat_mid", "twi", "vegh", "n_obs", "correlation", "cor_pval")],
#   type = "xyz",
#   crs = "EPSG:4326"
# )
#
# # Assign proper names matching the column order in the raster
# names(cor_r_2) <- c("lon", "lat", "lon_mid", "lat_mid", "twi", "vegh", "n_obs", "correlation", "cor_pval")
#
# # Write to NetCDF file
# nc_path2 <- file.path(cor_twi_vegh_tiles_dir, paste0("cor_twi_vegh_450m_", tile_id, ".nc"))
# terra::writeCDF(cor_r_2, nc_path2, overwrite = TRUE)
#

# ------------parallel process--------------------------------------------------

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
      premerg_file <- args$premerg_file

      message("Processing tile: ", tile_id)

      # -------Load pre-processed raster------------
      premerg_r <- terra::rast(premerg_file)

      df_cor <- windows_cor_analysis(premerg_r)

      df_data <- df_cor |>
        tidyr::unnest(cols = c(data)) |>  # Explicit cols parameter
        ungroup()

      # ------5km output-------
      cor_r <- terra::rast(
        df_cor[, c("lon_mid", "lat_mid", "correlation", "cor_pval")],
        type = "xyz",
        crs = "EPSG:4326"
      )
      names(cor_r) <- c("correlation", "cor_pval")
      nc_path <- file.path(cor_twi_vegh_tiles_dir, paste0("cor_twi_vegh_5km_", tile_id, ".nc"))
      terra::writeCDF(cor_r, nc_path, overwrite = TRUE)

      # ------450m output-------
      cor_r_2 <- terra::rast(
        df_data[, c("lon", "lat", "lon_mid", "lat_mid", "twi", "vegh", "n_obs", "correlation", "cor_pval")],
        type = "xyz",
        crs = "EPSG:4326"
      )
      names(cor_r_2) <- c("lon_mid", "lat_mid", "twi", "vegh", "n_obs", "correlation", "cor_pval")

      nc_path2 <- file.path(cor_twi_vegh_tiles_dir, paste0("cor_twi_vegh_450m_", tile_id, ".nc"))
      terra::writeCDF(cor_r_2, nc_path2, overwrite = TRUE)

      return(list(success = TRUE, tile = tile_id))

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
message(sprintf("done [%.1fs]", difftime(Sys.time(), t0, units = "secs")))
