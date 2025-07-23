#' Aggregate raster to target resolution with optional resampling and masking
#'
#' Aggregate an input raster to a specified resolution or match a target raster's resolution.
#' You must provide either xres_tar and yres_tar **or** a target_path raster (not both).
#' If you want to resample or mask the raster, you **must** provide target_path.
#'
#' @param input_path  Path to the input raster file.
#' @param output_path Path where the output raster will be saved.
#' @param xres_tar    Target resolution in x-direction (numeric), required if target_path is NULL.
#' @param yres_tar    Target resolution in y-direction (numeric), required if target_path is NULL.
#' @param target_path Path to target raster file, required if resampling or masking.
#' @param if_resample Logical, whether to resample input raster to target grid (requires target_path).
#' @param if_mask     Logical, whether to mask output raster by target extent (requires target_path).
#'
#' @return The output file path (character).
aggregate_byfile <- function(input_path, output_path,
                             xres_tar = NULL,
                             yres_tar = NULL,
                             target_path = NULL,
                             varname = "band",
                             if_resample = FALSE,
                             if_mask = FALSE) {

  # Validate input: resampling or masking requires target_path
  if (if_resample && is.null(target_path)) {
    stop("Error: if_resample = TRUE requires a non-NULL target_path.")
  }
  if (if_mask && is.null(target_path)) {
    stop("Error: if_mask = TRUE requires a non-NULL target_path.")
  }

  # Load input raster
  r_in <- terra::rast(input_path)

  # If target raster is provided, load it and extract resolution
  if (!is.null(target_path)) {
    r_tar <- terra::rast(target_path)[[1]]
    xres_tar <- terra::xres(r_tar)
    yres_tar <- terra::yres(r_tar)
  }

  # Compute aggregation factors
  fact_x <- as.integer(round(xres_tar / terra::xres(r_in)))
  fact_y <- as.integer(round(yres_tar / terra::yres(r_in)))

  # Aggregate raster if factors are >= 1
  if (fact_x >= 1 && fact_y >= 1) {
    r_out <- terra::aggregate(r_in, fact = c(fact_x, fact_y), fun = mean, na.rm = TRUE)
  } else {
    r_out <- r_in
  }

  # Resample to target raster grid if requested
  if (if_resample) {
    r_out <- terra::resample(r_out, r_tar, method = "bilinear")
  }

  # Mask raster using target extent if requested
  if (if_mask) {
    r_out <- terra::mask(r_out, r_tar)
  }

  # Write the output raster to file (NetCDF format), preserving original layer name
  terra::writeCDF(r_out, output_path, overwrite = TRUE, varname = varname)

  message(paste("Saved:", output_path))

  # Clean up
  rm(r_in, r_out)
  if (exists("r_tar")) rm(r_tar)
  gc()

  return(output_path)
}
