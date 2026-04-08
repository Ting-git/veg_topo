# ==============================================================================
# Vegetated Area Fraction Aggregation (450m → 0.5°)
#
# Purpose:
#   Aggregate high-resolution fraction of vegetated area (450m) data to 0.5° (~55km)
#   resolution by calculating the fraction of vegetated area within each
#   coarse grid cell (non-zero vegetation height / total pixels).
# ==============================================================================

# -------------------- 1. Setup Environment ------------------------------------
library(terra)

# other custom functions
source(here::here("R/config.R"))
source(here::here("R/raster_preprocess_save.R"))
source(here::here("R/create_aligned_template.R"))

# Create output directory
if (!dir.exists(dirname(fveg_55km_path))) dir.create(dirname(fveg_55km_path), recursive = TRUE)

# -------------------- 2. Aggregation & Resampling -----------------------------
message("Aggregating vegetated area fraction...")

# Create template raster aligned to 5km grid
align_template_55km <- create_aligned_template(twi_450m_mosaic_clean_path, dwin=0.5)

r_out <- raster_preprocess_save(
  input        = fveg_450m_mosaic_path,
  output       = fveg_55km_path,
  target       = align_template_55km,
  varname      = "fveg",
  if_aggregate = TRUE,
  if_resample  = TRUE,
  if_return_raster = FALSE
)

# # -------------------- 3. Optional Check ---------------------------------------
# Uncomment this section to verify results visually
# message("Checking input and output rasters...")
r_in  <- rast(fveg_450m_mosaic_path)
# r_out <- rast(fveg_55km_path)
#
print(r_in)
# print(r_out)
#
# par(mfrow = c(1, 2))
# plot(r_in,  main = "Vegetation Height (450m)")
# plot(r_out, main = "Vegetated Area Fraction (0.5°)")
# par(mfrow = c(1, 1))
#
# summary(r_out)


# -------------------- 4. Cleanup ----------------------------------------------
message("Cleaning up environment...")
rm(list = ls())
gc()
message("✅ Script finished successfully.")
