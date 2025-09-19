#' Aggregate Topography Rasters
#'
#' This function takes a digital elevation model (DEM) raster and computes the slope and aspect.
#' It then aggregates the DEM, slope, and aspect to a specified target resolution.
#' Aspect is handled correctly by converting it to vector components before aggregation.
#'
#' @param dem A single-layer SpatRaster representing the digital elevation model.
#' @param res_tar Numeric vector of length 2, specifying target resolution c(xres, yres).
#'
#' @return A list of SpatRaster layers:
#'   - dem: aggregated DEM
#'   - slope: aggregated slope (degrees)
#'   - aspect: aggregated aspect (degrees, 0–360)
#'
#' @examples
#' # Aggregate DEM to 100x100 m resolution
#' result <- aggregate_topography(dem_raster, res_tar = c(100, 100))
#' plot(result$dem)
#' plot(result$slope)
#' plot(result$aspect)
aggregate_topography <- function(dem, res_tar = NULL, target = NULL, if_resample = FALSE) {
  # Calculate slope (degrees) and aspect (radians)
  slope  <- terrain(dem, v = "slope", unit = "degrees")
  aspect <- terrain(dem, v = "aspect", unit = "radians")

  # Convert aspect to vector components for proper averaging
  x_comp <- cos(aspect)
  y_comp <- sin(aspect)

  # Combine all rasters for aggregation
  rasters <- c(dem, slope, x_comp, y_comp)
  aggregated <- raster_preprocess_save(input = rasters, res_tar = res_tar, target = target, if_resample = if_resample)

  # Extract results
  dem_agg   <- aggregated[[1]]
  slope_agg <- aggregated[[2]]
  x_agg     <- aggregated[[3]]
  y_agg     <- aggregated[[4]]

  # Reconstruct aspect in degrees
  aspect_agg <- atan2(y_agg, x_agg) * 180 / pi
  aspect_agg[aspect_agg < 0] <- aspect_agg[aspect_agg < 0] + 360

  list(dem = dem_agg, slope = slope_agg, aspect = aspect_agg)
}
