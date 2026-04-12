#' Preprocess Raster: Aggregate, Resample, Mask, and Save Multi-layer Rasters
#'
#' This function preprocesses raster data (single or multi-layer) by optionally:
#' - Aggregating to a target resolution (traditional method)
#' - Zonal aggregation to target grid (precise, no interpolation)
#' - Masking using a target raster
#' - Replacing a specified NA value
#'
#' @param input Raster file path or SpatRaster object (can be multi-layer).
#' @param output Output folder path (character) or vector of file paths (optional).
#'               If NULL, raster is returned without saving.
#' @param res_tar Target resolution c(xres, yres) (optional if target is provided).
#' @param target Target raster file path or SpatRaster (optional). Used for zonal/resampling/masking.
#' @param varname Variable name when writing NetCDF (default "band").
#' @param na_value Numeric value to treat as NA (optional).
#' @param if_zonal Logical, whether to use zonal aggregate (precise, no interpolation). Default FALSE.
#' @param if_aggregate Logical, whether to aggregate to target resolution. Default TRUE.
#' @param fun Aggregation function for downscaling (default: mean).
#' @param if_resample Logical, whether to resample to target raster grid. Default FALSE.
#' @param if_round_fact Logical, whether to round aggregation factors to integer. Default TRUE.
#' @param if_mask Logical, whether to mask raster to target raster extent. Default FALSE.
#' @param if_return_raster Logical, whether to return the processed raster. Default TRUE.
#'
#' @return A SpatRaster object (single or multi-layer) or output file paths.
#'
#' @examples
#' # Use zonal aggregate (recommended for precise aggregation)
#' raster_preprocess_save("input.tif", target = "target.tif",
#'                        if_zonal = TRUE, output = "out_folder/")
#'
#' # Use traditional aggregate (requires grid alignment)
#' raster_preprocess_save("input.tif", res_tar = c(1000, 1000),
#'                        if_aggregate = TRUE, output = "out_folder/")
raster_preprocess_save <- function(input,
                                   output = NULL,
                                   res_tar = NULL,
                                   target = NULL,
                                   varname = "band",
                                   na_value = NULL,
                                   if_zonal = FALSE,
                                   if_aggregate = TRUE,
                                   fun = mean,
                                   if_resample = FALSE,
                                   if_round_fact = TRUE,
                                   if_mask = FALSE,
                                   if_return_raster = TRUE) {

  # --- Load input raster ---
  r_in <- if (is.character(input)) terra::rast(input) else input

  # Replace specified NA value with proper NA
  if (!is.null(na_value)) r_in[r_in == na_value] <- NA

  # --- Get target raster and resolution ---
  if (!is.null(target)) {
    r_tar <- if (is.character(target)) terra::rast(target)[[1]] else target[[1]]
    res_tar <- c(terra::xres(r_tar), terra::yres(r_tar))
  }

  # --- Parameter validation ---
  if (if_zonal && if_aggregate) {
    warning("Both if_zonal and if_aggregate are TRUE. ",
            "Using if_zonal (priority), ignoring if_aggregate.")
  }

  if (if_zonal && is.null(target)) {
    stop("if_zonal = TRUE requires a 'target' raster.")
  }

  # ========== Aggregation method selection (mutually exclusive, zonal priority) ==========

  if (if_zonal && !is.null(target)) {
    # Method 1: Zonal Aggregate (precise, no interpolation, no alignment needed)
    message("Using ZONAL aggregate (high-res -> target grid)")

    if (if_aggregate) {
      message("  -> Note: if_aggregate = TRUE ignored because if_zonal = TRUE takes priority")
    }

    # Load or extract target raster
    r_tar <- if (is.character(target)) terra::rast(target)[[1]] else target[[1]]

    # Expand the input raster's spatial extent to align with the target grid
    # This ensures all target polygons have corresponding raster data
    # Areas outside original extent are set to NA
    # super important for fveg calculation!!!!
    r_in <- terra::extend(r_in, ext(r_tar))

    # Convert target raster to polygons (memory-efficient with chunked processing)
    target_polygons <- sf::st_as_sf(terra::as.polygons(r_tar,
                                                       dissolve = FALSE,
                                                       trunc = FALSE,
                                                       values = TRUE))

    # Efficient zonal extraction using exactextractr
    result <- exactextractr::exact_extract(
      r_in,
      target_polygons,
      fun = fun,
      progress = FALSE
    )

    # Create output raster with target's structure and new values
    r_out <- rast(r_tar)  # Create new raster from target's template
    terra::values(r_out) <- result  # Assign aggregated values

  } else if (if_aggregate && !is.null(res_tar)) {
    # Method 2: Traditional Aggregate (requires grid alignment)
    message("Using TRADITIONAL aggregate")

    # Calculate aggregation factors
    fact_x <- res_tar[1] / terra::xres(r_in)
    fact_y <- res_tar[2] / terra::yres(r_in)

    # Check if rounding is needed
    if (if_round_fact) {
      fact_x_rounded <- round(fact_x)
      fact_y_rounded <- round(fact_y)
      message(sprintf("  -> Factors: X = %.2f -> %d, Y = %.2f -> %d",
                      fact_x, fact_x_rounded, fact_y, fact_y_rounded))
      fact_x <- fact_x_rounded
      fact_y <- fact_y_rounded
    } else {
      message(sprintf("  -> Factors: X = %.2f, Y = %.2f", fact_x, fact_y))
    }

    message(sprintf("  -> Input raster dimensions: %d x %d",
                    terra::nrow(r_in), terra::ncol(r_in)))

    # Perform aggregation
    if (fact_x >= 1 && fact_y >= 1) {
      r_out <- terra::aggregate(r_in,
                                fact = c(fact_x, fact_y),
                                fun = fun,
                                na.rm = TRUE)
      message(sprintf("  -> Output raster dimensions: %d x %d",
                      terra::nrow(r_out), terra::ncol(r_out)))
    } else {
      warning("Aggregation factors < 1 — skipping aggregation.")
      r_out <- r_in
    }

  } else {
    # Method 3: No aggregation
    message("No aggregation performed")
    r_out <- r_in
  }

  # ========== Post-processing ==========
  # Note: resample is only applied when zonal is NOT used
  # because zonal already produces exact target grid alignment

  if (if_resample && !if_zonal && !is.null(target)) {
    message("Resampling to target grid")
    r_out <- terra::resample(r_out, r_tar, method = "bilinear")
  }

  if (if_mask && !is.null(target)) {
    message("Masking to target extent")
    r_out <- terra::mask(r_out, r_tar)
  }

  # ========== Output handling ==========
  n_layers <- terra::nlyr(r_out)

  if (!is.null(output)) {
    # If output is a folder path
    if (length(output) == 1 && (dir.exists(output) || !grepl("\\.", basename(output)))) {
      if (!dir.exists(output)) dir.create(output, recursive = TRUE)
      output <- sapply(seq_len(n_layers), function(i) {
        fname <- if (!is.null(names(r_out)[i]) && names(r_out)[i] != "") {
          names(r_out)[i]
        } else {
          paste0("lyr", i)
        }
        file.path(output, paste0(fname, ".nc"))
      })
    }

    # Validate output length
    if (length(output) != n_layers) {
      stop("Length of 'output' must match number of layers in raster.")
    }

    # Save each layer
    for (i in seq_len(n_layers)) {
      lyr <- r_out[[i]]
      out_i <- output[i]
      ext <- tools::file_ext(out_i)

      # Determine variable name
      varname_i <- if (!is.null(names(r_out)[i]) && names(r_out)[i] != "" && names(r_out)[i] != "lyr.1") {
        names(r_out)[i]
      } else {
        if (n_layers == 1) varname else paste0(varname, "_", i)
      }

      # Write based on file extension
      if (tolower(ext) %in% c("nc", "cdf")) {
        terra::writeCDF(lyr, out_i, overwrite = TRUE, varname = varname_i)
      } else if (tolower(ext) %in% c("tif", "tiff")) {
        terra::writeRaster(
          lyr,
          out_i,
          filetype  = "GTiff",
          gdal      = c("COMPRESS=LZW", "BIGTIFF=YES", "TILED=YES", "BLOCKXSIZE=256", "BLOCKYSIZE=256"),
          overwrite = overwrite,
          datatype  = "FLT4S",
          NAflag    = -9999
        )
      }
    }

    if (all(file.exists(output))) message("✅ Saved files:\n", paste("  ", output, collapse = "\n"))
  }

  # Clean up
  rm(r_in)
  if (exists("r_tar")) rm(r_tar)
  gc()

  if (if_return_raster) return(r_out) else return(output)
}
