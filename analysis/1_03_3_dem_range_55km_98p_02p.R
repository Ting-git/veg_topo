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

# Create output directory
if (!dir.exists(dirname(dem_rg_98p_02p_55km_path))) dir.create(dirname(dem_rg_98p_02p_55km_path), recursive = TRUE)

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
  if_zonal = TRUE,
  if_aggregate = FALSE,
  fun = function(values, coverage_fractions) {
    as.numeric(quantile(values, 0.98, na.rm = TRUE))
  },
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
  if_zonal = TRUE,
  fun = function(values, coverage_fractions) {
    as.numeric(quantile(values, 0.02, na.rm = TRUE))
  },
  if_aggregate = FALSE,
  if_round_fact  = TRUE,
  if_resample    = FALSE,
  if_return_raster = TRUE
)

# ---------------- 4. Compute elevation range -----------------

message("Computing elevation range (98p - 02p)...")
dem_rg <- dem_98p - dem_02p

terra::writeCDF(dem_rg, dem_rg_98p_02p_55km_path, overwrite = TRUE, varname = "dem_rg_98p_02p")
if (file.exists(dem_rg_98p_02p_55km_path)) message("✅ Saved : ", dem_rg_98p_02p_55km_path)

# # ---------------- 5 (optional ). Check the output ----------
# r <- terra::rast(dem_rg_98p_02p_55km_path)
# r
# summary(r)
# plot(r,  main = "Elevation range (0.5°)")

