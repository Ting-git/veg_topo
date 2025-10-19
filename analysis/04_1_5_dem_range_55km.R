# ============================================================
# Elevation Range Calculation (0.5° grid from 450m DEM)
#
# Steps:
#   1. Load DEM and target grid
#   2. Compute 95th and 5th percentile elevation per grid cell
#   3. Compute elevation range (95p - 05p)
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

# ---------------- 2. Compute 95th percentile -----------------
message("Calculating 95th percentile elevation...")
dem_95p <- raster_preprocess_save(
  input   = dem,
  output  = NULL,
  target  = mi,
  varname = "dem_95p",
  if_aggregate = TRUE,
  fun = function(x, ...) {
    x <- x[!is.na(x)]
    if (length(x) == 0) return(NA_real_)
    as.numeric(quantile(x, 0.95))
  },
  if_round_fact  = TRUE,
  if_resample    = FALSE,
  if_return_raster = TRUE
)

# ---------------- 3. Compute 5th percentile ------------------
message("Calculating 5th percentile elevation...")
dem_05p <- raster_preprocess_save(
  input   = dem,
  output  = NULL,
  target  = mi,
  varname = "dem_05p",
  if_aggregate = TRUE,
  fun = function(x, ...) {
    x <- x[!is.na(x)]
    if (length(x) == 0) return(NA_real_)
    as.numeric(quantile(x, 0.05))
  },
  if_round_fact  = TRUE,
  if_resample    = FALSE,
  if_return_raster = TRUE
)

# ---------------- 4. Compute elevation range -----------------
message("Computing elevation range (95p - 05p)...")
dem_rg <- dem_95p - dem_05p
plot(dem_rg, main = "Elevation Range (95p - 05p)")

# ---------------- 5. Resample to target grid & save ----------
message("Resampling elevation range to 0.5° grid...")
dem_rg <- raster_preprocess_save(
  input   = dem_rg,
  output  = dem_rg_55km_path,
  target  = mi,
  varname = "dem_rg",
  if_aggregate = FALSE,
  if_resample  = TRUE,
  if_return_raster = TRUE
)

message("✅ Elevation range computation completed.")
message("Saved : ", dem_rg_55km_path)

# ---------------- 6 (optional ). Check the output ----------
# r <- terra::rast(dem_rg_55km_path)
# r
# summary(r)
# plot(r)

# ============================================================
# Optional: test in a smaller region (for quick verification)
# ============================================================

# message("Running small-region test (optional)...")
#
# ext <- terra::ext(6, 10, 44, 48)
# mi_test  <- terra::crop(mi,  ext)
# dem_test <- terra::crop(dem, ext)
#
# plot(dem_test, main = "DEM (test region)")
#
# # Compute 95p and 05p for test region
# dem_95p_test <- raster_preprocess_save(
#   input   = dem_test,
#   output  = NULL,
#   target  = mi_test,
#   varname = "dem_95p",
#   if_aggregate = TRUE,
#   fun = function(x, ...) {
#     x <- x[!is.na(x)]
#     if (length(x) == 0) return(NA_real_)
#     as.numeric(quantile(x, 0.95))
#   },
#   if_round_fact = TRUE
# )
#
# dem_05p_test <- raster_preprocess_save(
#   input   = dem_test,
#   output  = NULL,
#   target  = mi_test,
#   varname = "dem_05p",
#   if_aggregate = TRUE,
#   fun = function(x, ...) {
#     x <- x[!is.na(x)]
#     if (length(x) == 0) return(NA_real_)
#     as.numeric(quantile(x, 0.05))
#   },
#   if_round_fact = TRUE
# )
#
# dem_rg_test <- dem_95p_test - dem_05p_test
# plot(dem_rg_test, main = "Test Region: Elevation Range (95p - 05p)")
#
# dem_rg_test <- raster_preprocess_save(
#   input   = dem_rg_test,
#   output  = NULL,
#   target  = mi_test,
#   varname = "dem_rg",
#   if_resample = TRUE
# )
#
# message("✅ Small-region test completed.")
