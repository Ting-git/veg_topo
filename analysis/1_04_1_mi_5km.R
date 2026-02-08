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

# Automatically select configuration file
hostname <- trimws(tolower(system("hostname", intern = TRUE)))
if (hostname == "dash") {
  message("💻 Detected Worksation: dash → using config.R")
  source(here::here("config.R"))
} else {
  message("🖥️ Detected HPC environment (", hostname, ") → using config_ubelix.R")
  source(here::here("config_ubelix.R"))
}

# Load custom functions
source(here::here("R/raster_preprocess_save.R"))

# Create output directory
if (!dir.exists(dirname(mi_5km_file))) dir.create(dirname(mi_5km_file), recursive = TRUE)
message("✅  Output directory:", dirname(mi_5km_file))

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
