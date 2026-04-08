# ============================================================
# Elevation Range Calculation (0.5° grid from 450m DEM)
#
# Steps:
#   1. Load DEM and target grid
#   2. Compute 98th and 2th percentile elevation per grid cell
#   3. Compute elevation range (98p - 02p)
#   4. Optionally test on a smaller region
# ============================================================

library(terra)

source(here::here("R/config.R"))
source(here::here("R/create_aligned_template.R"))
source(here::here("R/raster_preprocess_save.R"))

# ---------------- 1. Create template grid ------------------------
# Create template raster aligned to 55km grid
align_template_55km <- create_aligned_template(twi_450m_mosaic_clean_path, dwin = 0.5)

# ---------------- 2. Compute 98th percentile -----------------
message("Calculating 98th percentile elevation...")
dem_98p <- raster_preprocess_save(
  input   = dem_450m_mosaic_path,
  output  = NULL,
  target  = align_template_55km,
  varname = "dem_98p",
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

# ---------------- 3. Compute 2th percentile ------------------
message("Calculating 2th percentile elevation...")
dem_02p <- raster_preprocess_save(
  input   = dem_450m_mosaic_path,
  output  = NULL,
  target  = align_template_55km,
  varname = "dem_02p",
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
message("Computing elevation range (98p - 02p)...")
dem_rg <- dem_98p - dem_02p

# ---------------- 5. Resample to target grid & save ----------
message("Resampling elevation range to 0.5° grid...")
dem_rg <- raster_preprocess_save(
  input   = dem_rg,
  output  = dem_rg_98p_02p_55km_path,
  target  = mi,
  varname = "dem_rg",
  if_aggregate = FALSE,
  if_resample  = TRUE,
  if_return_raster = TRUE
)

message("✅ Elevation range computation completed.")
message("Saved : ", dem_rg_98p_02p_55km_path)

# ---------------- 6 (optional ). Check the output ----------
# r <- terra::rast(dem_rg_55km_path)
# r
# summary(r)
# plot(r)

