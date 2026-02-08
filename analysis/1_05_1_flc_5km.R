# ==============================================================================
#
# Purpose:
#   Process global land cover tiles to calculate fractions of:
#     - Vegetation cover (fused)
#     - Bare ground (fbare)
#     - Water (fwater)
#     - Snow/ice (fsnow)
#   at 5km resolution. Steps:
#     1. Setup environment and directories
#     2. Load tile information and helper functions
#     3. Parallel process each tile to compute fractions
#     4. Save tile-level results
#     5. Mosaic all tiles into a global map
#     6. Resample to reference raster and Save final global outputs as NetCDF
#     7. Clean intermediate files
#
# Dependencies: terra, dplyr, tidyr, purrr, furrr, fs
# ==============================================================================

# -------------------- 1. Setup Environment ------------------------------------
library(terra)
library(dplyr)
library(tidyr)
library(purrr)
library(furrr)
library(fs)

# Automatically select configuration file
hostname <- trimws(tolower(system("hostname", intern = TRUE)))
if (hostname == "dash") {
  message("💻 Detected Worksation: dash → using config.R")
  source(here::here("config.R"))
  workers = 8
} else {
  message("🖥️ Detected HPC environment (", hostname, ") → using config_ubelix.R")
  source(here::here("config_ubelix.R"))
  workers = 49
}

# Load custom functions
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/calculate_fraction_land_cover.R"))
source(here::here("R/mosaic_tiles.R"))
source(here::here("R/raster_preprocess_save.R"))

# Create output directory
if (!dir.exists(flc_tile_dir)) dir.create(flc_tile_dir, recursive = TRUE)
message("✅ Tile output directory: ", flc_tile_dir)

message("🌍 Starting land-cover fraction pipeline...")

# -------------------- 2. Load Tile Information --------------------------------
tiles_info <- readRDS(valid_tiles_info_path)
tile_output_dir <- flc_tile_dir

# -------------------- 3. Parallel Tile Processing -----------------------------
gc()
plan(multisession, workers = workers)
t_start <- Sys.time()
message("⏱ Pipeline started at: ", format(t_start, "%Y-%m-%d %H:%M:%S"))

results <- future_pmap(
  tiles_info,
  function(...) {
    args <- list(...)
    tryCatch({
      tile_id <- args$tile_id
      message("🔹 Processing tile: ", tile_id)

      # Define tile extent and crop land cover raster
      ext <- terra::ext(args$xmin, args$xmax, args$ymin, args$ymax)
      lc_r <- terra::rast(cci_landcover_path, lyrs = "lccs_class")
      rc   <- terra::crop(lc_r, ext)

      # Record tile processing start
      t_tile_start <- Sys.time()

      # Create spatial windows and calculate fractions
      df_win <- create_spatial_windows(rc, value_vars = "lccs_class", dwin = 0.05)
      output_file <- file.path(tile_output_dir, paste0("flc_5km_", tile_id, ".nc"))
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

mosaic_r <- mosaic_tiles(input_dir = tile_output_dir)
message("✅ Mosaic created successfully.")

# -------------------- 5. Resample to Reference Raster -------------------------
# Set output files and varnames for seperately saving
output_files <- c(fused_5km_file, fbare_5km_file, fwater_5km_file, fsnow_5km_file)
varnames     <- c("fused", "fbare", "fwater", "fsnow")

message("🔧 Resampling mosaic to match reference raster...")
raster_preprocess_save(
  input            = mosaic_r,
  output           = output_files,
  target           = cor_twi_vegh_mosaic_file,
  varname          = varnames,
  if_aggregate     = FALSE,
  if_resample      = TRUE,
  if_return_raster = FALSE
)
message("✅ Resampling completed.")

# -------------------- 6. Cleanup Intermediate Tiles ---------------------------
tiles_path <- fs::dir_ls(path = tile_output_dir, glob = "*.nc")
if (length(tiles_path) > 0) {
  message("🧹 Cleaning intermediate tile files...")
  file.remove(tiles_path)
  message("✅ Intermediate files removed.")
}

elapsed_total <- difftime(Sys.time(), t_start, units = "mins")
message(sprintf("🎉 Pipeline completed successfully [%.1f mins]", elapsed_total))
