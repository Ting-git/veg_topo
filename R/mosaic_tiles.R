
mosaic_tiles <- function(input_dir,
                         output_file = NULL,
                         pattern = "*.nc",
                         overwrite = TRUE,
                         crs = NULL,
                         varname = "band") {
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

  # Write output if path provided
  if (!is.null(output_file)) {
    terra::writeCDF(mosaic_raster, output_file, varname = varname, overwrite = overwrite)
    if(file.exists(output_file)) message("✅ Mosaic saved successfully to: ", output_file)
  }

  return(invisible(mosaic_raster))
}
