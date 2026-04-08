
# ==============================================================================
# Moisture Index Aggregation
# Purpose:
#   Aggregate global 0.05° (5km) moisture index data to 0.5° (55km) resolution.
# Steps:
#   1. Load input raster and configuration
#   2. Align extent to 0.5° global grid
#   3. Aggregate using mean
#   4. Save to file and visualize results
# ==============================================================================

# -------------------- 1. Setup Environment ------------------------------------
library(terra)

source(here::here("config.R"))
source(here::here("R/raster_preprocess_save.R"))

# Create output directory
if (!dir.exists(dirname(mi_55km_file))) dir.create(dirname(mi_55km_file), recursive = TRUE)

# ---------------- 2. Create template grid ------------------------
# Create template raster aligned to 55km grid
align_template_55km <- create_aligned_template(twi_450m_mosaic_clean_path, dwin = 0.5)

# ---------------- 3. aggregation (0.05° → 0.5°) -----------------
message("Aggregation MI (0.05° → 0.5°)")
raster_preprocess_save(
  input   = mi_5km_file,
  output  = mi_55km_file,
  target  = align_template_55km,
  varname = "mi",
  if_zonal = TRUE,
  if_aggregate = FALSE,
  fun = mean,
  if_resample    = FALSE,
  if_return_raster = FALSE
)

# # -------------------- 4. Quick Check & Visualization --------------------------
# message("Checking input and output rasters...")
#
# mi_5km_r  <- terra::rast(mi_5km_file)
# mi_55km_r <- terra::rast(mi_55km_file)
#
# print(mi_5km_r)
# print(mi_55km_r)
#
# par(mfrow = c(1, 2))
# plot(mi_5km_r, main = "Moisture Index (0.05°)")
# plot(mi_55km_r, main = "Moisture Index (0.5°)")
# par(mfrow = c(1, 1))
#
# message("✅ Visualization complete.")

