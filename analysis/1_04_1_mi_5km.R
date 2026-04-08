# ==============================================================================
# Moisture Index (MI) Aggregation Pipeline: 950m → 5km
#
# Purpose:
#   Aggregate fine-resolution (950m) global Moisture Index (MI) data
#   to 5km resolution, aligned with reference vegetation raster.
#
# Input:
#   - mi_950m_file             : Original 950m resolution MI raster
#   - cor_twi_vegh_mosaic_file : Reference raster for alignment/resampling
#
# Output:
#   - mi_5km_file              : Aggregated 5km resolution MI raster
# ==============================================================================

# -------------------- 1. Setup Environment ------------------------------------
library(terra)
library(here)

# Load custom functions
source(here::here("R/config.R"))
source(here::here("R/raster_preprocess_save.R"))
source(here::here("R/create_aligned_template.R"))

# Create output directory
if (!dir.exists(dirname(mi_5km_file))) dir.create(dirname(mi_5km_file), recursive = TRUE)

# -------------------- 2. Aggregation & Resampling -----------------------------
message("🔧 Aggregating MI raster and resampling to reference grid...")

# Create template raster aligned to 5km grid
align_template_5km <- create_aligned_template(twi_450m_mosaic_clean_path)

raster_preprocess_save(
  input            = mi_950m_file,
  output           = mi_5km_file,
  target           = align_template_5km,
  varname          = "moisture_index",
  na_value         = 0,
  if_aggregate     = TRUE,
  if_resample      = TRUE,
  if_return_raster = FALSE
)

message("✅ Completed.")

# -------------------- 3. Optional Visualization & Summary --------------------
# Uncomment below for quick visual inspection
# r <- rast(mi_5km_file)
# print(r)
# plot(r, main = "Moisture Index (5km)")
# summary(r)
# message("✅ Visualization and summary complete.")
