# ==============================================================================
# Purpose:
#   Process global land cover tiles to calculate fractions of:
#     - Vegetation cover (fused)
#     - Bare ground (fbare)
#     - Water (fwater)
#     - Snow/ice (fsnow)
#   at 5km resolution.
#
# Run Time: ~7 mins on UBELIX with 16 core
# ==============================================================================

# -------------------- 1. Setup Environment ------------------------------------
library(terra)
library(dplyr)
library(tidyr)
library(purrr)
library(furrr)
library(fs)

# Load custom functions
source(here::here("R/config.R"))
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/create_aligned_template.R"))
source(here::here("R/calculate_fraction_land_cover.R"))
source(here::here("R/mosaic_tiles.R"))
source(here::here("R/raster_preprocess_save.R"))

# Set worker numbers for different system
hostname <- trimws(tolower(system("hostname", intern = TRUE)))
if (hostname == "dash") {
  workers = 4
  message("→ using ", workers, " workers")

} else {
  workers = 16
  message("→ using ", workers, " workers")
}
# Create output directory
if (!dir.exists(flc_tile_dir)) dir.create(flc_tile_dir, recursive = TRUE)

# -------------------- 3. Parallel Tile Processing -----------------------------
gc()
plan(multisession, workers = workers)

t_start <- Sys.time()
message("⏱ Land-cover fraction pipeline started at: ", format(t_start, "%Y-%m-%d %H:%M:%S"))

# Load Tile Information
tiles_info <- readRDS(valid_tiles_info_path)

# Parallel process
results <- future_pmap(
  tiles_info,
  function(...) {
    args <- list(...)
    tryCatch({

      # set output per tile
      tile_id <- args$tile_id
      output_file <- file.path(flc_tile_dir, paste0("flc_5km_", tile_id, ".nc"))

      # Check if files have been processed
      if (fs::file_exists(output_file)) {
        message("Existed: ", tile_id)
        return(list(success = TRUE, error = NULL))
      }

      # ---- Start process the tile -----
      message("🔹 Processing tile: ", tile_id)

      # Define tile extent and crop land cover raster
      ext <- terra::ext(args$xmin, args$xmax, args$ymin, args$ymax)
      lc_r <- terra::rast(cci_landcover_path, lyrs = "lccs_class")
      rc   <- terra::crop(lc_r, ext)

      # Record tile processing start
      t_tile_start <- Sys.time()

      # Create spatial windows and calculate fractions
      df_win <- create_spatial_windows(rc, value_vars = "lccs_class", dwin = 0.05)
      df_flc <- calculate_fraction_land_cover(df_win, output_file = output_file)

      # Log tile completion
      if (file.exists(output_file)) {
        elapsed_tile <- difftime(Sys.time(), t_tile_start, units = "mins")
        message(sprintf("✅ Tile %s completed [%.1f mins]", tile_id, elapsed_tile))
      }

      # Clean tile variables
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

elapsed_total <- difftime(Sys.time(), t_start, units = "mins")
message(sprintf("📦 All tiles processed [%.1f mins]", elapsed_total))

# -------------------- 4. Mosaic All Tiles -------------------------------------
message("🗺 Mosaicing all tiles into global raster...")

# Create template raster aligned to 5km grid and mosaic
align_template_5km <- create_aligned_template(twi_450m_mosaic_clean_path)
mosaic_r <- mosaic_tiles(
  input_dir   = flc_tile_dir,
  output_file = NULL,
  pattern = "*.nc",
  target_grid = align_template_5km,
  if_resample = TRUE
)

# -------------------- 5. Save to Single Layer Raster -------------------------
message("🔧 Saving single-layer rasters...")

# Set output files and variable names
output_files <- c(fused_5km_file, fbare_5km_file, fwater_5km_file, fsnow_5km_file)
varnames     <- c("fused", "fbare", "fwater", "fsnow")

# save rasters to single-layer rasters
raster_preprocess_save(
  input            = mosaic_r,
  output           = output_files,
  varname          = varnames,
  if_aggregate     = FALSE,
  if_resample      = FALSE,
  if_return_raster = FALSE
)

elapsed_total <- difftime(Sys.time(), t_start, units = "mins")
message(sprintf("🎉 Pipeline completed successfully [%.1f mins]", elapsed_total))

# # -------------------- 6. Cleanup Intermediate Tiles ---------------------------
# tiles_path <- fs::dir_ls(path = flc_tile_dir, glob = "*.nc")
# if (length(tiles_path) > 0) {
#   message("🧹 Cleaning intermediate tile files...")
#   file.remove(tiles_path)
#   message("✅ Intermediate files removed.")
# }

# -------------------- 7. Optional Check ---------------------------
# r <- rast(fused_5km_file)
# print(r)
# summary(r)
# plot(r, main = "Used land fraction (5km)")
#
# r <- rast(fbare_5km_file)
# print(r)
# summary(r)
# plot(r, main = "Bare land fraction (5km)")
#
# r <- rast(fwater_5km_file)
# print(r)
# summary(r)
# plot(r, main = "Water body fraction (5km)")
#
# r <- rast(fsnow_5km_file)
# print(r)
# summary(r)
# plot(r, main = "Permanent snow and ice fraction (5km)")
