# ~30 min

# ------Load required libraries-------------------------------------------------------------
library(terra)     # For handling raster data
library(purrr)     # For functional programming tools like pmap_dfr
library(sf)

# ------Load configuration and helper functions---------------------------------------------
source(here::here("config.R"))
source(here::here("R/build_global_tiles.R")) # Load tile grid generation function

# ------Generate global tile grid-----------------------------------------------------------
tile_grid <- generate_tile_grid(lon_step = 30, lat_step = 30) # Create 30x30 degree tiles
# tile_grid <- tile_grid[32:35,]  # Select a subset of tiles (example range)

# ------Load raster layers to be preprocessed-----------------------------------------------
twi_r <- rast(twi_450m_mosaic_clean_path)
vegh_r <- rast(vegh_450m_mosaic_path)
raster_list <- list("twi" = twi_r , "vegh" = vegh_r) # Combine into a named list

# ------ Crop and merge for each tile, and exclude invalid tiles ---------------
# Record start time
start_time <- Sys.time()

# Define processing function
process_tile <- function(tile_id, xmin, xmax, ymin, ymax) {
  tryCatch({
    preprocess_single_tile(
      tile_id = tile_id,
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax,
      raster_list = list("twi" = twi_r, "vegh" = vegh_r),
      output_dir = twi_vegh_merg_450m_tiles_dir
    )
    return(TRUE)  # Return TRUE on success
  }, error = function(e) {
    message(sprintf("Failed to process tile %s: %s", tile_id, e$message))
    return(FALSE)  # Return FALSE on failure
  })
}

# Process all tiles
processing_results <- mapply(
  process_tile,
  tile_id = tile_grid$tile_id,
  xmin = tile_grid$xmin,
  xmax = tile_grid$xmax,
  ymin = tile_grid$ymin,
  ymax = tile_grid$ymax
)

# Generate summary report
successful_tiles <- sum(processing_results)
failed_tiles <- length(processing_results) - successful_tiles

message(sprintf("Processing complete! Success: %d, Failed: %d",
                successful_tiles, failed_tiles))

# Clean up memory
rm(twi_r, vegh_r)
gc()

# Display total processing time
message(sprintf("Total processing time: %.1f minutes",
                difftime(Sys.time(), start_time, units = "mins")))

# ------Report processing time and save results---------------------------------------------
message(sprintf("done [%.1f min]", difftime(Sys.time(), t0, units = "mins")))
saveRDS(successful_results, file = here::here("data/valid_tiles_info.rds"))

# --------Save as GeoPackage -----------------------------------------------------

# Load tile boundary info (xmin, xmax, ymin, ymax, tile_id, etc.)
valid_tiles_info <- readRDS(valid_tiles_info_path)

# Create list of rectangle polygons with tile_id as attribute
rects_list <- lapply(1:nrow(valid_tiles_info), function(i) {
  # Define polygon geometry using corner coordinates
  geom <- st_sfc(st_polygon(list(rbind(
    c(valid_tiles_info$xmin[i], valid_tiles_info$ymin[i]),
    c(valid_tiles_info$xmin[i], valid_tiles_info$ymax[i]),
    c(valid_tiles_info$xmax[i], valid_tiles_info$ymax[i]),
    c(valid_tiles_info$xmax[i], valid_tiles_info$ymin[i]),
    c(valid_tiles_info$xmin[i], valid_tiles_info$ymin[i])
  ))), crs = 4326)  # Set CRS to WGS84 (EPSG:4326)

  # Create a single-feature sf object with tile_id as a field
  st_sf(name = valid_tiles_info$tile_id[i], geometry = geom)
})

# Combine all individual polygons into a single sf object
rects_sf <- do.call(rbind, rects_list)

# Save the resulting sf object to disk (GeoPackage, Shapefile, etc.)
st_write(rects_sf, valid_geotiles_path, layer = "valid_tile", delete_layer = TRUE)
