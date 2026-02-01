# ==============================================================================
# Vegetated Area Fraction Aggregation (450m → 0.5°)
#
# Purpose:
#   Aggregate high-resolution vegetation height (450m) data to 0.5° (~55km)
#   resolution by calculating the fraction of vegetated area within each
#   coarse grid cell (non-zero vegetation height / total pixels).
#
# Input:
#   - vegh_450m_mosaic_path : High-resolution vegetation height raster
#   - mi_55km_file          : Target 0.5° grid (for alignment/resampling)
#
# Output:
#   - fveg_55km_path        : Aggregated vegetated area fraction raster (0.5°)
#
# Steps:
#   1. Load configuration and dependencies
#   2. Aggregate 450m vegetation data to 0.5° using custom fraction function
#   3. Resample to target grid alignment
#   4. (Optional) Visualize and verify
#   5. Cleanup
# ==============================================================================

# -------------------- 1. Setup Environment ------------------------------------
library(terra)

# Automatically select configuration file
hostname <- trimws(tolower(system("hostname", intern = TRUE)))
if (hostname == "dash") {
  message("💻 Detected Worksation: dash → using config.R")
  source(here::here("config.R"))
} else {
  message("🖥️ Detected HPC environment (", hostname, ") → using config_ubelix.R")
  source(here::here("config_ubelix.R"))
}

# other custom functions
source(here::here("R/raster_preprocess_save.R"))

message("Starting vegetated area fraction aggregation (450m → 0.5°)...")

# -------------------- 2. Aggregation & Resampling -----------------------------
message("Aggregating vegetation data using vegetated area fraction...")

r_out <- raster_preprocess_save(
  input        = vegh_450m_mosaic_path,
  output       = fveg_55km_path,
  target       = mi_55km_file,
  varname      = "fveg",
  if_aggregate = TRUE,
  fun = function(x, na.rm) {
    total <- length(x)                  # total number of pixels INCLUDING NA
    if (all(is.na(x))) return(NA)       # if all NA return NA
    veg_count <- sum(!is.na(x) & x > 0)  # count of vegetated pixels
    return(veg_count / total)            # fraction over TOTAL pixels, including NA
  },
  if_resample  = TRUE,
  if_return_raster = TRUE
)

message("✅ Vegetated area fraction aggregation completed successfully.")
message("Output file: ", fveg_55km_path)

# -------------------- 3. Optional Check ---------------------------------------
# Uncomment this section to verify results visually
# message("Checking input and output rasters...")
# r_in  <- rast(vegh_450m_mosaic_path)
# r_out <- rast(fveg_55km_path)
# r_tar <- rast(mi_55km_file)
#
# print(r_in)
# print(r_out)
#
# par(mfrow = c(1, 2))
# plot(r_in,  main = "Vegetation Height (450m)")
# plot(r_out, main = "Vegetated Area Fraction (0.5°)")
# par(mfrow = c(1, 1))
#
# summary(r_out)
# message("✅ Visualization complete.")

# -------------------- 4. Cleanup ----------------------------------------------
message("Cleaning up environment...")
rm(list = ls())
gc()
message("✅ Script finished successfully.")
