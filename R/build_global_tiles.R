# -----------------------------------------------

#' Generate Global Tiles Grid
#'
#' Creates a grid of global tiles with specified longitude and latitude steps.
#'
#' @param lon_step Numeric. Longitudinal step size in degrees (default: 30).
#' @param lat_step Numeric. Latitudinal step size in degrees (default: 30).
#' @return A data.frame containing tile names and their geographic extents.
generate_tile_grid <- function(lon_step = 30, lat_step = 30) {
  # Generate global tile boundaries
  lon_edges <- seq(-180, 180, by = lon_step)
  lat_edges <- seq(-60, 90, by = lat_step)

  # Helper functions for naming
  make_lon_label <- function(lon) {
    ifelse(lon < 0, paste0(abs(lon), "W"), paste0(lon, "E"))
  }

  make_lat_label <- function(lat) {
    ifelse(lat < 0, paste0(abs(lat), "S"), paste0(lat, "N"))
  }

  # Initialize output data frame
  tile_grid <- data.frame(
    tile_id = character(),
    xmin = numeric(),
    xmax = numeric(),
    ymin = numeric(),
    ymax = numeric(),
    stringsAsFactors = FALSE
  )

  # Generate all possible tiles
  for (lat_i in 1:(length(lat_edges) - 1)) {
    for (lon_j in 1:(length(lon_edges) - 1)) {
      # Create extent object first
      tile_ext <- terra::ext(
        lon_edges[lon_j],
        lon_edges[lon_j + 1],
        lat_edges[lat_i],
        lat_edges[lat_i + 1]
      )

      tile_id <- paste0(
        make_lat_label(lat_edges[lat_i]), "_",
        make_lon_label(lon_edges[lon_j])
      )

      # Use terra's extent accessors
      tile_grid <- rbind(tile_grid, data.frame(
        tile_id = tile_id,
        xmin = terra::xmin(tile_ext),
        xmax = terra::xmax(tile_ext),
        ymin = terra::ymin(tile_ext),
        ymax = terra::ymax(tile_ext),
        stringsAsFactors = FALSE
      ))
    }
  }

  return(tile_grid)
}

# -----------------------------------------------

#' Preprocess Multipal Raster Data for a Single Tile
#'
#' Processes a list of rasters by cropping them to a specified extent, resampling to a common grid (using the first raster as reference),
#' and saving the results as a NetCDF file. Returns metadata about the processed tile.
#'
#' @param tile_id Character or numeric identifier for the tile (used in output filename)
#' @param xmin,xmax,ymin,ymax Numeric coordinates defining the tile extent
#' @param raster_list List of SpatRaster objects to process
#' @param output_dir Character path to directory where output files should be saved
#' @param prefix Character prefix for output filenames (default: "tile_")
#' @param resample_method Character resampling method (default: "bilinear")
#'        Options: "near", "bilinear", "cubic", "cubicspline", "lanczos"
#'
#' @return A data frame containing:
#' \itemize{
#'   \item{tile_id - The input tile identifier}
#'   \item{xmin, xmax, ymin, ymax - The tile extent coordinates}
#'   \item{premerg_file - Path to the output NetCDF file}
#' }
#' Returns NULL if processing failed for all rasters.
#'
#' @details The function:
#' \itemize{
#'   \item{Crops all rasters to the specified extent}
#'   \item{Uses the first valid raster as the spatial reference}
#'   \item{Resamples subsequent rasters to match the reference}
#'   \item{Merges layers and saves as NetCDF}
#'   \item{Preserves original layer names when available}
#' }
#'
#' @examples
#' \dontrun{
#'   result <- preprocess_single_tile(
#'     tile_id = "0N_90E",
#'     xmin = 0, xmax = 30,
#'     ymin = 90, ymax = 120,
#'     raster_list = list("twi" = twi_r, "vegh" = vegh_r),
#'     output_dir = temp_dir
#'   )
#' }
#'
preprocess_single_tile <- function(
    tile_id,
    xmin,
    xmax,
    ymin,
    ymax,
    raster_list,
    output_dir,
    prefix = "tile_",
    resample_method = "bilinear"
) {

  if (is.null(names(raster_list))) {
    names(raster_list) <- paste0("layer_", seq_along(raster_list))
  }

  # ------main process

  # Define tile extent (xmin, xmax, ymin, ymax)
  tile_extent <- terra::ext(xmin, xmax, ymin, ymax)

  # Process each raster: crop and resample to reference (first raster)
  cropped_rasters <- lapply(seq_along(raster_list), function(j) {
    r <- raster_list[[j]]  # Get current raster

    # Try cropping - skip on failure
    cropped <- tryCatch(terra::crop(r, tile_extent),
                        error = function(e) {
                          warning("Tile ", tile_id, " (layer ", j, ") crop failed: ", e$message)
                          return(NULL)
                        })

    # Skip if crop failed or has no data
    if (is.null(cropped) || all(is.na(terra::values(cropped)))) return(NULL)

    # First raster becomes reference
    if (j == 1) {
      ref_raster <<- cropped[[1]]  # Set global reference
      names(cropped) <- names(raster_list)[j]
      return(cropped)
    }

    # Safety check
    if (!exists("ref_raster")) stop("Missing reference raster - check first layer")

    # Resample to match reference
    resampled <- terra::resample(cropped, ref_raster, method = resample_method)
    names(resampled) <- names(raster_list)[j]
    return(resampled)
  })

  # Remove failed crops and validate output
  cropped_rasters <- Filter(Negate(is.null), cropped_rasters)
  if (length(cropped_rasters) == 0) {
    warning("No valid rasters for tile: ", tile_id)
    return(NULL)
  }

  # Merge and save results
  merged_tile <- terra::rast(cropped_rasters)  # Combine all layers
  output_file <- file.path(output_dir, paste0(prefix, tile_id, ".nc"))

  terra::writeCDF(
    merged_tile,
    filename = output_file,
    overwrite = TRUE,
    varname = names(merged_tile)  # Preserve layer names
  )

  message("✅ Successfully saved tile ", tile_id, " to:\n", normalizePath(output_file))
  # ------

  return(data.frame(
    tile_id = tile_id,
    xmin = xmin,
    xmax = xmax,
    ymin = ymin,
    ymax = ymax,
    premerg_file = output_file,
    stringsAsFactors = FALSE
  ))
}
