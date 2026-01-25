
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

message("Starting moisture index aggregation (0.05° → 0.5°)...")

# -------------------- 2. Load Input Raster ------------------------------------
message("Loading 5km moisture index raster...")
r_in <- terra::rast(mi_5km_file)
plot(r_in, main = "Original Moisture Index (0.05°)")

# -------------------- 3. Define Target Resolution & Extent --------------------
message("Preparing target resolution and extent...")
res_tar <- c(0.5, 0.5)  # 0.5° target resolution

# Align to 0.5° grid boundaries
aligned_extent <- terra::ext(-180, 180, -56.5, 86.5)
r_in_ext <- terra::extend(r_in, aligned_extent)

# -------------------- 4. Aggregate Raster -------------------------------------
message("Aggregating raster to 0.5° resolution...")
r_out <- raster_preprocess_save(
  input        = r_in_ext,
  output       = mi_55km_file,
  res_tar      = res_tar,
  varname      = "moisture_index",
  if_aggregate = TRUE,
  if_round_fact = TRUE,
  if_resample  = FALSE,
  fun          = mean,
  if_return_raster = TRUE
)

message("✅ Aggregation completed successfully.")
message("Output file: ", mi_55km_file)

# -------------------- 5. Quick Check & Visualization --------------------------
message("Checking input and output rasters...")

mi_5km_r  <- terra::rast(mi_5km_file)
mi_55km_r <- terra::rast(mi_55km_file)

print(mi_5km_r)
print(mi_55km_r)

par(mfrow = c(1, 2))
plot(mi_5km_r, main = "Moisture Index (0.05°)")
plot(mi_55km_r, main = "Moisture Index (0.5°)")
par(mfrow = c(1, 1))

message("✅ Visualization complete.")

# -------------------- 6. Cleanup ----------------------------------------------
rm(list = ls())
gc()
message("✅ Script finished successfully.")

