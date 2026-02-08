# ==============================================================================
# Fused Data Aggregation and Resampling (0.05° → 0.5°)
#
# Purpose:
#   Aggregate and resample fused 5km (0.05°) raster data to 0.5° (55km) resolution.
#
# Input:
#   - fused_5km_file : 5km fused raster file (from config.R)
#   - mi_55km_file   : target 0.5° grid (for alignment/resampling)
#
# Output:
#   - fused_55km_file: 0.5° aggregated and resampled fused raster
#
# Steps:
#   1. Load configuration and dependencies
#   2. Aggregate 0.05° fused data to 0.5° using mean
#   3. Resample to align with moisture index grid
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

source(here::here("R/raster_preprocess_save.R"))

# Create output directory
if (!dir.exists(dirname(fused_55km_file))) dir.create(dirname(fused_55km_file), recursive = TRUE)
message("✅ Output directory:", dirname(fused_55km_file))

message("Starting fused raster aggregation (0.05° → 0.5°)...")
# -------------------- 2. Aggregation & Resampling -----------------------------

input_files <- c(fused_5km_file, fbare_5km_file, fwater_5km_file, fsnow_5km_file)
output_files <- c(fused_55km_file, fbare_55km_file, fwater_55km_file, fsnow_55km_file)

varnames     <- c("fused", "fbare", "fwater", "fsnow")
rasters <- lapply(input_files, rast)
stacked <- rast(rasters)
names(stacked) <- varnames

message("Aggregating and resampling fused raster...")
r_out <- raster_preprocess_save(
  input        = stacked,
  output       = output_files,
  target       = mi_55km_file,
  varname      = varnames,
  if_aggregate = TRUE,
  if_resample  = TRUE,
  fun          = mean,
  if_return_raster = TRUE
)

message("✅ Aggregation and resampling completed.")

# -------------------- 3. Optional Check ---------------------------------------
# Uncomment to verify results
# message("Checking input and output rasters...")
# r_in  <- rast(fused_5km_file)
# r_out <- rast(fused_55km_file)
#
# print(r_in)
# print(r_out)
#
# par(mfrow = c(1, 2))
# plot(r_in,  main = "Fused Raster (0.05°)")
# plot(r_out, main = "Fused Raster (0.5°)")
# par(mfrow = c(1, 1))
#
# message("✅ Visualization complete.")

# -------------------- 4. Cleanup ----------------------------------------------
message("Cleaning up environment...")
rm(list = ls())
gc()
message("✅ Script finished successfully.")
