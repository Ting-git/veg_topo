# ==============================================================================
#
# Purpose:
#   Process global land cover tiles to calculate fractions of:
#     - Vegetation cover (fused)
#     - Bare ground (fbare)
#     - Water (fwater)
#     - Snow/ice (fsnow)
#   at 5km resolution.
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
  workers = 8 # ~20 min
} else {
  message("🖥️ Detected HPC environment (", hostname, ") → using config_ubelix.R")
  source(here::here("config_ubelix.R"))
  workers = 49 # ~8 min
}

# Load custom functions
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/create_aligned_template.R"))
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

# -------------------- 5. Save to Single Layer Raster -------------------------
# Set output files and variable names
output_files <- c(fused_5km_file, fbare_5km_file, fwater_5km_file, fsnow_5km_file)
varnames     <- c("fused", "fbare", "fwater", "fsnow")

message("🔧 Saving single-layer rasters...")

# Create template raster aligned to 5km grid
align_template_5km <- create_aligned_template(twi_450m_mosaic_clean_path)

# Resample and save rasters
raster_preprocess_save(
  input            = mosaic_r,
  output           = output_files,
  target           = align_template_5km,
  varname          = varnames,
  if_aggregate     = FALSE,
  if_resample      = TRUE,
  if_return_raster = FALSE
)

message("✅ Saved single-layer rasters.")

# -------------------- 6. Cleanup Intermediate Tiles ---------------------------
tiles_path <- fs::dir_ls(path = tile_output_dir, glob = "*.nc")
if (length(tiles_path) > 0) {
  message("🧹 Cleaning intermediate tile files...")
  file.remove(tiles_path)
  message("✅ Intermediate files removed.")
}

elapsed_total <- difftime(Sys.time(), t_start, units = "mins")
message(sprintf("🎉 Pipeline completed successfully [%.1f mins]", elapsed_total))
