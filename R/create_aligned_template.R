#' Create a raster template aligned to an input raster
#'
#' Generates a template raster covering the input raster's extent,
#' aligned to a regular grid of size `dwin`. Uses the same alignment
#' logic as `create_spatial_windows()` for consistent spatial windows.
#'
#' @param input Input raster (SpatRaster, Raster, or its file path)
#' @param dwin Grid/window size
#' @param crs_out Optional CRS for output raster (default: same as input)
#' @return Aligned SpatRaster template
#'
#' @examples
#' r <- rast(twi_450m_mosaic_clean_path)
#' template <- create_aligned_template(r, dwin = 0.05)
#' template
create_aligned_template <- function(input, dwin = 0.05, crs_out = NULL) {

  # ---- Load raster ----
  if (is.character(input)) input <- terra::rast(input)
  if (!inherits(input, "SpatRaster")) stop("Input must be a SpatRaster or valid file path.")

  # Get input raster extent
  e <- ext(input)

  # Align the extent to the grid defined by dwin (floor/ceiling)
  xmin_aligned <- floor(e$xmin / dwin) * dwin
  xmax_aligned <- ceiling(e$xmax / dwin) * dwin
  ymin_aligned <- floor(e$ymin / dwin) * dwin
  ymax_aligned <- ceiling(e$ymax / dwin) * dwin

  # Compute number of rows and columns to cover the aligned extent
  ncols <- round((xmax_aligned - xmin_aligned) / dwin)
  nrows <- round((ymax_aligned - ymin_aligned) / dwin)

  # Use input CRS if crs_out not provided
  if (is.null(crs_out)) crs_out <- crs(input)

  # Create the template raster
  template <- rast(
    nrows = nrows,
    ncols = ncols,
    xmin = xmin_aligned,
    xmax = xmax_aligned,
    ymin = ymin_aligned,
    ymax = ymax_aligned,
    crs = crs_out
  )

  return(template)
}
