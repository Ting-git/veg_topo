#' Create a raster template aligned to an input raster
#'
#' Generates a template raster covering the input raster's extent,
#' aligned to a regular grid of size `res_out`. Uses the same alignment
#' logic as `create_spatial_windows()` for consistent spatial windows.
#'
#' @param input Input raster (SpatRaster, Raster, or its file path)
#' @param res_out Grid/window size
#' @param crs_out Optional CRS for output raster (default: same as input)
#' @return Aligned SpatRaster template
#'
#' @examples
#' r <- rast(twi_450m_mosaic_clean_path)
#' template <- create_aligned_template(r, res_out = 0.05)
#' template
create_aligned_template <- function(input, res_out = 0.05, crs_out = "EPSG:4326") {

  # ---- Helper function to determine decimal places ----
  get_decimal_places <- function(x) {
    if (x == 0) return(0)
    x_str <- format(x, scientific = FALSE)
    if (!grepl("\\.", x_str)) return(0)
    nchar(strsplit(x_str, "\\.")[[1]][2])
  }

  # Automatically determine rounding precision from res_out
  res_out_decimals <- get_decimal_places(res_out)
  round <- res_out_decimals + 1

  # ---- Load raster ----
  if (is.character(input)) input <- terra::rast(input)
  if (!inherits(input, "SpatRaster")) stop("Input must be a SpatRaster or valid file path.")

  # Get input raster extent
  e <- if(crs(input) != "EPSG:4326") get_lonlat_extent(input) else ext(input)

  xmin <- round(e$xmin, round)
  xmax <- round(e$xmax, round)
  ymin <- round(e$ymin, round)
  ymax <- round(e$ymax, round)

  # Align the extent to the grid defined by res_out (floor/ceiling)
  xmin_aligned <- floor(xmin / res_out) * res_out
  xmax_aligned <- ceiling(xmax / res_out) * res_out
  ymin_aligned <- floor(ymin / res_out) * res_out
  ymax_aligned <- ceiling(ymax / res_out) * res_out

  # Compute number of rows and columns to cover the aligned extent
  ncols <- round((xmax_aligned - xmin_aligned) / res_out)
  nrows <- round((ymax_aligned - ymin_aligned) / res_out)

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
