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

# Load configuration and helper functions
source(here::here("config.R"))
source(here::here("R/raster_preprocess_save.R"))

message("🌍 Starting Moisture Index aggregation: 950m → 5km...")

# -------------------- 2. Aggregation & Resampling -----------------------------
message("🔧 Aggregating MI raster and resampling to reference grid...")

raster_preprocess_save(
  input            = mi_950m_file,
  output           = mi_5km_file,
  target           = cor_twi_vegh_mosaic_file,
  varname          = "moisture_index",
  if_aggregate     = TRUE,
  if_resample      = TRUE,
  na_value         = 0,
  if_return_raster = FALSE
)

# Check if output exists and print message
if (file.exists(mi_5km_file)) {
  message("✅ Moisture Index aggregation completed successfully!")
  message("📁 Output saved at: ", mi_5km_file)
} else {
  message("❌ Aggregation failed: output file not found.")
}

# -------------------- 3. Optional Visualization & Summary --------------------
# Uncomment below for quick visual inspection
# r <- rast(mi_5km_file)
# print(r)
# plot(r, main = "Moisture Index (5km)")
# summary(r)
# message("✅ Visualization and summary complete.")
