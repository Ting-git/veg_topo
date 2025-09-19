library(terra)
library(fs)

# source(here::here("config.R")) # no need for it, due to not execute in workstation02

# ---- Data Source Configuration ----
dem_30m_copernicus_dir <- "/storage/scratch/giub_geco/tting/copernicus_dem_30m/copernicus_dem_30m"
sw_in_450m_tile_dir <- "/storage/scratch/giub_geco/tting/global_sw_in_450m/1_1_deg_tiles"

# Read input DEM files
dem_files_all <- list.files(
  path = dem_30m_copernicus_dir,
  pattern = "*_DEM\\.tif$",
  recursive = TRUE,
  full.names = TRUE
)

message(sprintf("Found %d DEM tiles", length(dem_files_all)))

# Get processed SW_IN files
sw_in_uneven_files <- list.files(
  path = sw_in_450m_tile_dir,
  pattern = "*_to_sw_in_uneven_450m\\.nc$",
  recursive = TRUE,
  full.names = TRUE
)

sw_in_flat_files <- list.files(
  path = sw_in_450m_tile_dir,
  pattern = "*_to_sw_in_flat_450m\\.nc$",
  recursive = TRUE,
  full.names = TRUE
)

# Extract base names for comparison
dem_base_names <- gsub("\\.tif$", "", basename(dem_files_all))
sw_in_uneven_base_names <- gsub("_to_sw_in_uneven_450m\\.nc$", "", basename(sw_in_uneven_files))
sw_in_flat_base_names <- gsub("_to_sw_in_flat_450m\\.nc$", "", basename(sw_in_flat_files))

# Find missing files
missing_sw_in_uneven <- setdiff(dem_base_names, sw_in_uneven_base_names)
missing_sw_in_flat <- setdiff(dem_base_names, sw_in_flat_base_names)

message(sprintf("Missing uneven SW_IN files: %d", length(missing_sw_in_uneven)))
message(sprintf("Missing flat SW_IN files: %d", length(missing_sw_in_flat)))

# Check a single tile (simplified)
check_tile <- function(tile_index) {
  if (tile_index > length(dem_files_all)) {
    message("Tile index out of range")
    return(NULL)
  }

  dem_file <- dem_files_all[tile_index]
  dem_base_name <- gsub("\\.tif$", "", basename(dem_file))

  sw_in_uneven <- file.path(sw_in_450m_tile_dir, paste0(dem_base_name, "_to_sw_in_uneven_450m.nc"))
  sw_in_flat <- file.path(sw_in_450m_tile_dir, paste0(dem_base_name, "_to_sw_in_flat_450m.nc"))

  # Check if output files exist
  if (!file.exists(sw_in_uneven) || !file.exists(sw_in_flat)) {
    message(sprintf("Output files missing for: %s", dem_base_name))
    return(NULL)
  }

  # Read and return rasters
  dem_raster <- rast(dem_file)
  uneven_raster <- rast(sw_in_uneven)
  flat_raster <- rast(sw_in_flat)

  return(list(dem = dem_raster, uneven = uneven_raster, flat = flat_raster))
}

# Example: Check tile 1199
tile_data <- check_tile(2)
if (!is.null(tile_data)) {
  plot(tile_data$dem, main = "DEM")
  plot(tile_data$uneven, main = "Uneven SW_IN")
  plot(tile_data$flat, main = "Flat SW_IN")
}
