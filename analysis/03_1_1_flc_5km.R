# ==============================================================================
# land_use_fraction_pipeline.R
#
# Processes global land cover tiles to calculate land use fractions (used, bare, water)
# at 5km resolution. Steps:
#   1. Setup environment and directories
#   2. Load tile info and helper functions
#   3. Parallel process tiles to compute fractions
#   4. Save tile-level results
#   5. Mosaic all tiles to global map
#   6. Resample to target raster
#   7. Clean intermediate files
#   8. Save final global outputs as NetCDF
#
# Dependencies: terra, tidyr, dplyr, purrr, furrr, fs
# ==============================================================================

# Load packages
library(terra)
library(tidyr)
library(dplyr)
library(purrr)
library(furrr)

# Load config and helpers
source(here::here("config.R"))
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/calculate_fraction_land_use.R"))
source(here::here("R/mosaic_tiles.R"))

# Read tile information
tiles_info <- readRDS(valid_tiles_info_path)

# Set output directory
tile_output_dir <- file.path(veg_topo_extr_dir, "data/global_flc_5km/30_30_deg")
if (!dir.exists(tile_output_dir)) dir.create(tile_output_dir, recursive = TRUE)

# Calculate fraction of land use - parallel processing
gc()
plan(multisession, workers = 8)

t00 <- Sys.time()
message(paste0("Start processing: ", format(t00, "%Y-%m-%d %H:%M:%S")))

results <- future_pmap(
  tiles_info,
  function(...) {
    args <- list(...)
    tryCatch({
      tile_id <- args$tile_id

      # Define tile extent and crop landcover data
      ext <- terra::ext(args$xmin, args$xmax, args$ymin, args$ymax)
      lc_r <- terra::rast(cci_landcover_path, lyrs = "lccs_class")
      rc <- terra::crop(lc_r, ext)

      # Track processing time
      t0 <- Sys.time()
      print(t0)

      # Create spatial windows and calculate fractions
      df_win <- create_spatial_windows(rc, value_vars = "lccs_class", dwin = 0.05)
      output_file <- file.path(tile_output_dir, paste0("flc_5km_", tile_id, ".nc"))
      df_flc <- calculate_fraction_land_use(df_win, output_file = output_file)

      if (file.exists(output_file)) {
        message(sprintf("Tile %s done [%.1f mins]", tile_id,
                        difftime(Sys.time(), t0, units = "mins")))
      }

      rm(lc_r, rc, df_win, df_flc); gc()
      return(list(success = TRUE, error = NULL))

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

elapsed <- as.numeric(difftime(Sys.time(), t00, units = "mins"))
message(sprintf("All tiles done [%.1f mins]", elapsed))

# Mosaic all tiles
mosaic_r <- mosaic_tiles(input_dir = tile_output_dir)

# Resample mosaic to match reference raster
cor_r <- terra::rast(cor_twi_vegh_mosaic_file)
mosaic_rr <- terra::resample(mosaic_r, cor_r, method = "bilinear")

# Clean up intermediate files
tiles_path <- fs::dir_ls(path = tile_output_dir, glob = "*.nc")
if(length(tiles_path) > 0) file.remove(tiles_path)

# Save final outputs
output_files <- c(fused_5km_file, fbare_5km_file, fwi_5km_file)
varnames <- c("fused", "fbare", "fwi")

for (i in 1:3) {
  terra::writeCDF(mosaic_rr[[i]], output_files[i], varnames = varnames[i], overwrite = TRUE)
  message("✅ saved: ", output_files[i])
}

elapsed <- as.numeric(difftime(Sys.time(), t00, units = "mins"))
message(sprintf("Mosaicing done [%.1f mins]", elapsed))
