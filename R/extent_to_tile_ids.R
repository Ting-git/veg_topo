#' Convert a terra extent to global tile IDs or raster (fully memory-safe)
#'
#' This function determines which tiles overlap a given geographic extent
#' and, if requested, loads and mosaics them into a single raster object.
#' Memory usage is optimized by processing tiles incrementally.
#'
#' @param ext A terra::ext object (SpatExtent)
#' @param tile_size Size of each tile in degrees (default = 3)
#' @param return_raster Logical. If TRUE, returns a mosaicked SpatRaster;
#'        if FALSE, returns a character vector of intersecting tile IDs
#' @param source Tile naming source, either "lang_vegh_10m" or "copernicus_dem_30m"
#' @param tiles_dir Directory containing the tile files
#'
#' @return Either a vector of tile IDs (if return_raster = FALSE) or a SpatRaster object
#' @export
extent_to_tile_ids <- function(ext,
                               tile_size = 3,
                               return_raster = TRUE,
                               source = "lang_vegh_10m",
                               tiles_dir = vegh_10m_tiles_dir) {

  # ---- Input validation ----
  if (!inherits(ext, "SpatExtent"))
    stop("Input 'ext' must be a terra::ext object (SpatExtent).")
  if (!dir.exists(tiles_dir))
    stop("tiles_dir does not exist: ", tiles_dir)
  if (tile_size <= 0 || tile_size > 180)
    stop("tile_size must be within (0, 180].")
  source <- match.arg(source, choices = c("lang_vegh_10m", "copernicus_dem_30m"))

  # ---- Extract extent coordinates ----
  xmin <- terra::xmin(ext)
  xmax <- terra::xmax(ext)
  ymin <- terra::ymin(ext)
  ymax <- terra::ymax(ext)

  # ---- Align to tile grid ----
  lon_seq <- seq(floor(xmin / tile_size) * tile_size,
                 floor((xmax - 1e-9) / tile_size) * tile_size,
                 by = tile_size)
  lat_seq <- seq(floor(ymin / tile_size) * tile_size,
                 floor((ymax - 1e-9) / tile_size) * tile_size,
                 by = tile_size)

  # ---- Generate tile IDs ----
  tile_ids <- c()
  for (lat in lat_seq) {
    for (lon in lon_seq) {
      lat_prefix <- ifelse(lat >= 0, "N", "S")
      lon_prefix <- ifelse(lon >= 0, "E", "W")
      tile_id <- switch(
        source,
        lang_vegh_10m = sprintf(
          "ETH_GlobalCanopyHeight_10m_2020_%s%02d%s%03d_Map.tif",
          lat_prefix, abs(lat), lon_prefix, abs(lon)
        ),
        copernicus_dem_30m = sprintf(
          "Copernicus_DSM_COG_10_%s%02d_00_%s%03d_00_DEM/Copernicus_DSM_COG_10_%s%02d_00_%s%03d_00_DEM.tif",
          lat_prefix, abs(lat), lon_prefix, abs(lon),
          lat_prefix, abs(lat), lon_prefix, abs(lon)
        )
      )
      tile_ids <- c(tile_ids, tile_id)
    }
  }
  tile_ids <- sort(unique(tile_ids))

  # ---- Filter existing files ----
  existing_files <- file.path(tiles_dir, tile_ids)
  existing_files <- existing_files[file.exists(existing_files)]
  if (length(existing_files) == 0) {
    warning("No matching tiles exist for the given extent.")
    return(NULL)
  }

  # ---- Return only files if requested ----
  if (!return_raster) return(existing_files)

  # ---- Memory-optimized raster loading ----
  if (length(existing_files) == 1) {
    # Single tile case
    r <- terra::rast(existing_files[1])
    r <- terra::crop(r, ext)
  } else {
    # Multiple tiles: process incrementally
    r <- NULL
    for (file in existing_files) {
      tile <- terra::crop(terra::rast(file), ext)
      if (is.null(r)) {
        r <- tile
      } else {
        r <- terra::mosaic(r, tile, fun = "mean")
        rm(tile)
        # Force garbage collection every 5 tiles
        if (which(existing_files == file) %% 5 == 0) gc()
      }
    }
  }

  names(r) <- source
  return(r)
}
