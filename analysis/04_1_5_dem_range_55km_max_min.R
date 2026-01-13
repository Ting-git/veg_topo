# ============================================================
# Elevation Range Calculation (0.5° grid from 450m DEM)
#
# Steps:
#   1. Load DEM and target grid
#   2. Compute max and min elevation per grid cell
#   3. Compute elevation range (max - min)
#   4. Optionally test on a smaller region
# ============================================================

library(terra)
source(here::here("config.R"))
source(here::here("R/raster_preprocess_save.R"))

# ---------------- 1. Load raster data ------------------------
message("Loading input rasters...")
mi  <- terra::rast(mi_55km_file)             # Target 0.5° grid
dem <- terra::rast(dem_450m_mosaic_path)     # High-resolution DEM

plot(dem, main = "Original DEM (450m)")

# ---------------- 2. Compute max  -----------------
message("Calculating max  elevation...")
dem_max <- raster_preprocess_save(
  input   = dem,
  output  = NULL,
  target  = mi,
  varname = "dem_max",
  if_aggregate = TRUE,
  fun = max,
  if_round_fact  = TRUE,
  if_resample    = FALSE,
  if_return_raster = TRUE
)

# ---------------- 3. Compute min ------------------
message("Calculating min elevation...")
dem_min <- raster_preprocess_save(
  input   = dem,
  output  = NULL,
  target  = mi,
  varname = "dem_min",
  if_aggregate = TRUE,
  fun = min,
  if_round_fact  = TRUE,
  if_resample    = FALSE,
  if_return_raster = TRUE
)

# ---------------- 4. Compute elevation range -----------------
message("Computing elevation range (max - min)...")
dem_rg <- dem_max - dem_min
plot(dem_rg, main = "Elevation Range (max - min)")

# ---------------- 5. Resample to target grid & save ----------
message("Resampling elevation range to 0.5° grid...")
dem_rg <- raster_preprocess_save(
  input   = dem_rg,
  output  = dem_rg_max_min_55km_path,
  target  = mi,
  varname = "dem_rg",
  if_aggregate = FALSE,
  if_resample  = TRUE,
  if_return_raster = TRUE
)

message("✅ Elevation range computation completed.")
message("Saved : ", dem_rg_max_min_55km_path)

# ---------------- 6 (optional ). Check the output ----------
# r <- terra::rast(dem_rg_55km_path)
# r
# summary(r)
# plot(r)
