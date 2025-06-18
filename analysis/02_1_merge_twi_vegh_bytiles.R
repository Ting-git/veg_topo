# ~30 min

# ------Load required libraries-------------------------------------------------------------
library(terra)     # For handling raster data
library(purrr)     # For functional programming tools like pmap_dfr

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

# ------Sequentially preprocess each tile---------------------------------------------------
t0 <- Sys.time()  # Start timing

# Wrap the function with safely() to capture results and errors
safe_preprocess <- purrr::safely(
  preprocess_single_tile,
  otherwise = NULL,  # Return NULL if an error occurs
  quiet = FALSE      # Set to TRUE to suppress error messages
)

# Process all tiles, storing both results and errors
all_results <- purrr::pmap(
  list(
    tile_grid$tile_id,
    tile_grid$xmin, tile_grid$xmax,
    tile_grid$ymin, tile_grid$ymax
  ),
  .f = safe_preprocess,
  raster_list = list("twi" = twi_r, "vegh" = vegh_r),
  output_dir = twi_vegh_merg_450m_tiles_dir
)

# Extract and print results
successful_results <- purrr::map_dfr(all_results, "result") # discarding errors
errors <- purrr::map(all_results, "error")
failed_tiles <- which(!purrr::map_lgl(errors, is.null))

if (length(failed_tiles) > 0) {
  message("Failed to process tiles: ", paste(failed_tiles, collapse = ", "))
} else {
  message("All tiles processed successfully!")
}

# free memory
rm(twi_r, vegh_r, raster_list)
gc()

# ------Report processing time and save results---------------------------------------------
message(sprintf("done [%.1fs]", difftime(Sys.time(), t0, units = "secs")))  #
saveRDS(successful_results, file = here::here("data/predata_info.rds"))

