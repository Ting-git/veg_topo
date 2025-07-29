#' Convert a terra extent to global tile IDs
#'
#' This function calculates the names of all tiles (e.g., "S03W012")
#' that overlap with a given geographic extent. The world is divided into
#' equally sized rectangular tiles, each identified by its lower-left
#' (southwest) corner using a naming convention (e.g., "N00E003").
#'
#' @param ext A `terra::ext` object representing the spatial extent (SpatExtent).
#' @param tile_size Size of the tile in degrees (both latitude and longitude). Default is 3.
#'
#' @return A sorted character vector of unique tile IDs overlapping the extent.
#'
#' @examples
#' library(terra)
#' e <- ext(-12.5, 0.2, -4.2, 2.8)
#' extent_to_tile_ids(e, tile_size = 3)
#'
#' @export
extent_to_tile_ids <- function(ext, tile_size = 3) {
  # Ensure input is a SpatExtent object
  if (!inherits(ext, "SpatExtent")) {
    stop("Input 'ext' must be a terra::ext object (SpatExtent).")
  }

  if (tile_size <= 0 || tile_size > 180) {
    stop("tile_size must be in the range (0, 180]")
  }

  # Extract coordinates from terra::ext
  xmin <- terra::xmin(ext)
  xmax <- terra::xmax(ext)
  ymin <- terra::ymin(ext)
  ymax <- terra::ymax(ext)

  # Align to tile boundaries (include max edges)
  lon_seq <- seq(
    floor(xmin / tile_size) * tile_size,
    floor((xmax - 1e-9) / tile_size) * tile_size,
    by = tile_size
  )

  lat_seq <- seq(
    floor(ymin / tile_size) * tile_size,
    floor((ymax - 1e-9) / tile_size) * tile_size,
    by = tile_size
  )

  # Generate tile names
  tile_ids <- c()
  for (lat in lat_seq) {
    for (lon in lon_seq) {
      lat_prefix <- ifelse(lat >= 0, "N", "S")
      lon_prefix <- ifelse(lon >= 0, "E", "W")
      tile_id <- sprintf("%s%02d%s%03d",
                         lat_prefix, abs(lat),
                         lon_prefix, abs(lon))
      tile_ids <- c(tile_ids, tile_id)
    }
  }

  return(sort(unique(tile_ids)))
}
