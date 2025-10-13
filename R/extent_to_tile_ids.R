#' Convert a terra extent to global tile IDs or raster
#'
#' This function calculates the names of all tiles (e.g., "S03W012")
#' that overlap with a given geographic extent. Optionally, it can load
#' and mosaic the tiles into a single raster.
#'
#' @param ext A terra::ext object representing the spatial extent (SpatExtent).
#' @param tile_size Size of the tile in degrees (both latitude and longitude). Default is 3.
#' @param return_raster Logical. If TRUE, returns a mosaicked raster. If FALSE, returns tile IDs. Default is TRUE.
#' @param source Tile naming source, either "lang_vegh_10m" or "copernicus_dem_30m".
#' @param tiles_dir Directory containing the tiles.
#'
#' @return Depending on return_raster, either a sorted character vector of tile IDs or a SpatRaster object.
#'
#' @examples
#' library(terra)
#' e <- ext(-12.5, 0.2, -4.2, 2.8)
#' extent_to_tile_ids(e, tile_size = 3, return_raster = TRUE, source = "lang_vegh_10m", tiles_dir = vegh_10m_tiles_dir)
#' extent_to_tile_ids(e, tile_size = 1, return_raster = TRUE, source = "copernicus_dem_30m", tiles_dir = dem_30m_copernicus_dir)
#'
#' @export
extent_to_tile_ids <- function(ext,
                               tile_size = 3,
                               return_raster = TRUE,
                               source = "lang_vegh_10m",
                               tiles_dir = vegh_10m_tiles_dir) {

  # Validate inputs
  if (!inherits(ext, "SpatExtent")) stop("Input 'ext' must be a terra::ext object (SpatExtent).")
  if (tile_size <= 0 || tile_size > 180) stop("tile_size must be in the range (0, 180].")
  source <- match.arg(source, choices = c("lang_vegh_10m", "copernicus_dem_30m"))

  # Extract extent coordinates
  xmin <- terra::xmin(ext)
  xmax <- terra::xmax(ext)
  ymin <- terra::ymin(ext)
  ymax <- terra::ymax(ext)

  # Align coordinates to tile boundaries
  lon_seq <- seq(floor(xmin / tile_size) * tile_size,
                 floor((xmax - 1e-9) / tile_size) * tile_size,
                 by = tile_size)
  lat_seq <- seq(floor(ymin / tile_size) * tile_size,
                 floor((ymax - 1e-9) / tile_size) * tile_size,
                 by = tile_size)

  # Generate tile IDs
  tile_ids <- c()
  for (lat in lat_seq) {
    for (lon in lon_seq) {
      lat_prefix <- ifelse(lat >= 0, "N", "S")
      lon_prefix <- ifelse(lon >= 0, "E", "W")

      tile_id <- switch(
        source,
        lang_vegh_10m = sprintf("ETH_GlobalCanopyHeight_10m_2020_%s%02d%s%03d_Map.tif",
                                lat_prefix, abs(lat), lon_prefix, abs(lon)),
        copernicus_dem_30m = sprintf(
          "Copernicus_DSM_COG_10_%s%02d_00_%s%03d_00_DEM/Copernicus_DSM_COG_10_%s%02d_00_%s%03d_00_DEM.tif",
          lat_prefix, abs(lat), lon_prefix, abs(lon),
          lat_prefix, abs(lat), lon_prefix, abs(lon)
        )
      )

      tile_ids <- c(tile_ids, tile_id)
    }
  }

  # Return only tile IDs if requested
  if (!return_raster) return(sort(unique(tile_ids)))

  # Keep only existing tiles
  existing_files <- file.path(tiles_dir, tile_ids)
  existing_files <- existing_files[file.exists(existing_files)]

  if (length(existing_files) == 0) {
    warning("No tiles exist for the given extent: ", paste(tile_ids, collapse = ", "))
    return(NULL)
  }

  # Load rasters safely
  rs <- lapply(existing_files, function(f) {
    r <- try(terra::rast(f), silent = TRUE)
    if (inherits(r, "try-error")) {
      warning("Failed to load raster: ", f)
      return(NULL)
    }
    r
  })
  rs <- Filter(Negate(is.null), rs)

  if (length(rs) == 0) {
    warning("No valid raster files loaded for extent.")
    return(NULL)
  }

  # Project and merge
  ref <- rs[[1]]
  rs <- lapply(rs, function(r) terra::project(r, terra::crs(ref)))

  r <- if (length(rs) == 1) rs[[1]] else Reduce(terra::merge, rs)

  # ⚠️ 防御性检查
  if (is.null(r)) {
    warning("Failed to merge rasters — returning NULL.")
    return(NULL)
  }

  # Crop to input extent
  r <- terra::crop(r, ext)

  # ⚠️ 只有非空时才设置名字
  if (!is.null(r)) {
    names(r) <- source
  }

  return(r)
}
