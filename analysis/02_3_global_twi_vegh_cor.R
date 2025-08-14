# done [25.1 min]

# ------Load required libraries-------------------------------------------------------------

library(terra)     # For handling raster data
library(furrr)
library(dplyr)

# ------Load configuration and helper functions---------------------------------------------

source(here::here("config.R"))
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/calculate_window_correlations.R"))
source(here::here("R/mosaicing.R"))

# ------Analysis---------------------------------------------

# read the information of valid tiles
tiles_info <- readRDS(valid_tiles_info_path)

# ------------parallel process for each tiles--------------------------------------------------

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

      # df_cor <- windows_cor_analysis(premerg_r)
      df_cor  <- create_spatial_windows(premerg_r) |>
        calculate_window_correlations()

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

      nc_path2 <- file.path(cor_twi_vegh_tiles_dir, "cor_450m", paste0("cor_twi_vegh_450m_", tile_id, ".nc"))
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
message(sprintf("done [%.1f min]", difftime(Sys.time(), t0, units = "mins")))

# -------- Combination ----------------------------------------------------------

mosaic_tiles(
  input_dir   = cor_twi_vegh_tiles_dir,
  output_file = cor_twi_vegh_mosaic_file,
  layer_names = c("correlation", "cor_pval")
)

