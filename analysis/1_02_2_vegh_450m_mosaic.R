# ～ 15 min： on UBELIX
# --------------- Setup --------------------------------------------------------

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
source(here::here("R/mosaic_tiles.R"))
# --------------- Load Data ----------------------------------------------------

# Load target grid and create mask
twi_450m_r <- rast(twi_450m_mosaic_clean_path)

# --------------- Mosaic and Resample ------------------------------------------
# Mosaicing
message("Starting mosaicing...")
vegh_450m_mosaic <- mosaic_tiles(
  input_dir   = vegh_450m_tiles_dir,
  output_file = NULL,
  pattern = "*_Map_to450m.nc",
  varname = "vegh")

# Check mosaicing result
if (is.null(vegh_450m_mosaic) || terra::ncell(vegh_450m_mosaic) == 0) {
  stop("Mosaicing failed - no data produced")
}
message("Mosaicing completed. Starting resample...")

# Resample to target grid
vegh_450m_resampled <- terra::resample(vegh_450m_mosaic, twi_450m_r, method = "bilinear")

rm(vegh_450m_mosaic, twi_450m_r); gc()

# ---------- Save Results ------------------------------------------------------

# Save output files
terra::writeCDF(vegh_450m_resampled, vegh_450m_mosaic_path, overwrite = TRUE, varname = "vegh")
if(file.exists(vegh_450m_mosaic_path)) message("✅ Saved: ", vegh_450m_mosaic_path)

rm(list = ls())
gc()

# check result
# r <- rast(vegh_450m_mosaic_path)
# plot(r)
