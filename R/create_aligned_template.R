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

  # ---- Helper function to determine decimal places ----
  get_decimal_places <- function(x) {
    if (x == 0) return(0)
    x_str <- format(x, scientific = FALSE)
    if (!grepl("\\.", x_str)) return(0)
    nchar(strsplit(x_str, "\\.")[[1]][2])
  }

  # Automatically determine rounding precision from dwin
  dwin_decimals <- get_decimal_places(dwin)
  round <- dwin_decimals + 1

  # ---- Load raster ----
  if (is.character(input)) input <- terra::rast(input)
  if (!inherits(input, "SpatRaster")) stop("Input must be a SpatRaster or valid file path.")

  # Get input raster extent
  e <- ext(input)
  xmin <- round(e$xmin, round)
  xmax <- round(e$xmax, round)
  ymin <- round(e$ymin, round)
  ymax <- round(e$ymax, round)

  # Align the extent to the grid defined by dwin (floor/ceiling)
  xmin_aligned <- floor(xmin / dwin) * dwin
  xmax_aligned <- ceiling(xmax / dwin) * dwin
  ymin_aligned <- floor(ymin / dwin) * dwin
  ymax_aligned <- ceiling(ymax / dwin) * dwin

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
