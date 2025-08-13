# ~1.4 min

# ------Load required libraries-------------------------------------------------------------
library(terra)     # For handling raster data
library(dplyr)
library(furrr)     # For functional programming tools like pmap_dfr
library(future)

source(here::here("config.R"))
source(here::here("R/generate_tile_grid.R"))
source(here::here("R/extent_to_tile_ids.R"))
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/calculate_correlation_bywin.R"))
# source(here::here("R/filter_land_tiles.R"))


# --- Load Region Info ---
regA_info <- readRDS(here::here("data/df_samples_A.rds")) |>
  select(ends_with("label"), ends_with("min"), ends_with("max"))

# --------------- none paralell testing ----------------------------------------

# --- Set Region Extent ---

# reg_id <- regA_info$strata_A_label[1]
#
# xmin <- regA_info$xmin[1]
# xmax <- regA_info$xmax[1]
# ymin <- regA_info$ymin[1]
# ymax <- regA_info$ymax[1]
#
# ext <- terra::ext(xmin, xmax, ymin, ymax)

# ---main processing ---
# copy to here
# ----------------------

# --------- Parallel Processing for Each Regions -------------------------------

gc()
plan(multisession, workers = 8)

t00 <- Sys.time()
message(paste0("Regional Correlation Analysis Start:", format(t00, "%Y-%m-%d %H:%M:%S")))

results <- future_pmap(
  regA_info,
  function(...) {
    args <- list(...)
    tryCatch({

      t0 <- Sys.time()

      # set region info
      reg_id <- args$strata_A_label
      ext <- terra::ext(args$xmin, args$xmax, args$ymin, args$ymax)

      # ---- main processing ---------------------------------------------------

      # --- Load TWI Raster ---
      twi_r <- terra::rast(twi_30m_path)
      names(twi_r) = "twi"
      twi_rc <- terra::crop(twi_r, ext)

      # --- Load and Prepare Vegetation Height Raster ---
      tile_ids <- extent_to_tile_ids(ext)  # Assume this function is defined elsewhere
      vegh_filepaths <- file.path(vegh_10m_tiles_dir, paste0("ETH_GlobalCanopyHeight_10m_2020_", tile_ids, "_Map.tif"))

      # Check for missing tiles
      missing_tiles <- vegh_filepaths[!file.exists(vegh_filepaths)]
      if (length(missing_tiles) > 0) {
        stop("Missing vegH tiles: ", paste(missing_tiles, collapse = ", "))
      }

      # Load and mosaic vegH tiles if needed
      vegh_r <- if (length(vegh_filepaths) > 1) {
        vegh_rs <- lapply(vegh_filepaths, terra::rast)
        do.call(terra::mosaic, c(vegh_rs, fun = mean))
      } else {
        terra::rast(vegh_filepaths)
      }
      names(vegh_r) = "vegh"

      # Crop and resample vegH to TWI
      vegh_rc <- terra::crop(vegh_r, ext)
      vegh_rr <- terra::resample(vegh_rc, twi_rc, method = "bilinear")

      # --- Stack Rasters ---

      stacked_r <- c(twi_rc, vegh_rr)
      names(stacked_r) <- c("twi", "vegh")

      # Optional: Plot for visual check
      # plot(stacked_r, axes = TRUE, asp = 1)

      # --- Create Spatial Windows and Compute Correlation ---
      dwin <- 0.005  # ~500m window at equator
      df_win <- create_spatial_windows(stacked_r, dwin = dwin)  # Custom function
      df_cor <- calculate_correlation_bywin(df_win)             # Custom function

      # --- cor - Convert to Raster and Save as NetCDF ---
      cor_r <- terra::rast(
        df_cor[, c("lon_mid", "lat_mid", "correlation")],
        type = "xyz",
        crs = "EPSG:4326"
      )
      names(cor_r) <- "correlation"

      nc_path <- file.path(regA_cor_twi_vegh_dir, paste0("regA_", reg_id, "_cor_twi_vegh_500m.nc"))
      terra::writeCDF(cor_r, nc_path, overwrite = TRUE)

      message("Saved: ", nc_path)

      # --- vegh - Save as NetCDF ---

      vegh_nc_path <- file.path(regA_cor_twi_vegh_dir, paste0("regA_", reg_id, "_vegh_30m.nc"))
      terra::writeCDF(vegh_rr, vegh_nc_path, overwrite = TRUE)

      message("Saved: ", vegh_nc_path)

      # --- twi - Save as NetCDF ---

      twi_nc_path <- file.path(regA_cor_twi_vegh_dir, paste0("regA_", reg_id, "_twi_30m.nc"))
      terra::writeCDF(twi_rc, twi_nc_path, overwrite = TRUE)

      message("Saved: ", twi_nc_path)

      # ------------------------------------------------------------------------

      message(sprintf("region %s done [%.1f mins]", reg_id, difftime(Sys.time(), t0, units = "mins")))

    }, error = function(e) {
      msg <- sprintf("Region %s failed: %s", args$strata_A_label %||% "unknown", conditionMessage(e))
      message("❌ ", msg)
      return(list(success = FALSE, error = msg))
    })
  },
  .options = furrr_options(seed = TRUE)
)

plan(sequential)
gc()

elapsed <- as.numeric(difftime(Sys.time(), t00, units = "mins"))
message(sprintf("All regions done [%.1f mins]", elapsed))



