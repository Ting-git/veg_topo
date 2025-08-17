#' Minimal Raster Processing: Aggregate, Resample, Mask
#'
#' Aggregate raster to target resolution, optionally resample and mask.
#' Input, output, and target can be file paths or SpatRaster objects.
#'
#' @param input Raster file path or SpatRaster object.
#' @param output Output file path (optional). If NULL, returns a SpatRaster.
#' @param res_tar Target resolution c(xres, yres) (optional if target is provided).
#' @param target Target raster file path or SpatRaster (optional).
#' @param varname Variable name when writing NetCDF (default "band").
#' @param if_resample Logical, resample to target grid.
#' @param if_mask Logical, mask raster to target extent.
#' @param na_value Numeric value to treat as NA.
#' @param fun Aggregation function (default: mean).
#'
#' @return SpatRaster object.
process_raster <- function(input, output = NULL, res_tar = NULL, target = NULL,
                               varname = "band", if_resample = FALSE, if_mask = FALSE,
                               na_value = NULL, fun = mean) {

  r_in <- if (is.character(input)) terra::rast(input) else input
  if (!is.null(na_value)) r_in[r_in == na_value] <- NA

  if (!is.null(target)) {
    r_tar <- if (is.character(target)) terra::rast(target)[[1]] else target[[1]]
    res_tar <- c(terra::xres(r_tar), terra::yres(r_tar))
  }

  fact_x <- res_tar[1] / terra::xres(r_in)
  fact_y <- res_tar[2] / terra::yres(r_in)
  r_out <- if (fact_x >= 1 && fact_y >= 1) terra::aggregate(r_in, fact = c(fact_x, fact_y), fun = fun, na.rm = TRUE) else r_in

  if (if_resample && !is.null(target)) r_out <- terra::resample(r_out, r_tar, method = "bilinear")
  if (if_mask && !is.null(target)) r_out <- terra::mask(r_out, r_tar)

  if (!is.null(output)) {
    ext <- tools::file_ext(output)
    if (tolower(ext) %in% c("nc", "cdf")) terra::writeCDF(r_out, output, overwrite = TRUE, varname = varname)
    else terra::writeRaster(r_out, output, overwrite = TRUE)
  }

  return(r_out)
}

