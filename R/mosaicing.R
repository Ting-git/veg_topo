mosaic_tiles <- function(input_dir,
                         output_file,
                         pattern = "*.nc",
                         overwrite = TRUE,
                         crs = NULL,
                         layer_names = NULL) {
  # List matching files
  tile_paths <- fs::dir_ls(path = input_dir, glob = pattern)

  if (length(tile_paths) == 0) {
    stop("No matching files found in: ", input_dir)
  }

  # Read and merge rasters
  tile_rasters <- lapply(tile_paths, terra::rast)
  mosaic_raster <- do.call(terra::merge, unname(tile_rasters))

  # Set CRS if provided
  if (!is.null(crs)) {
    terra::crs(mosaic_raster) <- crs
  }

  # Set layer names
  if (is.null(layer_names)) {
    names(mosaic_raster) <- paste0("band", seq_len(terra::nlyr(mosaic_raster)))
  } else {
    if (length(layer_names) != terra::nlyr(mosaic_raster)) {
      stop("Length of 'layer_names' does not match number of raster layers")
    }
    names(mosaic_raster) <- layer_names
  }

  # Write output
  terra::writeCDF(mosaic_raster, output_file, overwrite = overwrite)

  # Success message
  message("✅ Mosaic saved successfully to: ", output_file)

  return(invisible(mosaic_raster))
}
