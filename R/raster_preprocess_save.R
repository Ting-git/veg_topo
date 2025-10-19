
#' Preprocess Raster: Aggregate, Resample, Mask, and Save Multi-layer Rasters
#'
#' This function preprocesses raster data (single or multi-layer) by optionally:
#' - Aggregating to a target resolution
#' - Resampling to match a target raster
#' - Masking using a target raster
#' - Replacing a specified NA value
#'
#' It can save each layer to separate files:
#' - If `output` is a folder path, each layer is saved as a NetCDF file (.nc) using the layer name.
#' - If `output` is a vector of file paths, each layer is saved to the corresponding path.
#'
#' @param input Raster file path or SpatRaster object (can be multi-layer).
#' @param output Output folder path (character) or vector of file paths (optional).
#'               If NULL, raster is returned without saving.
#' @param res_tar Target resolution c(xres, yres) (optional if target is provided).
#' @param target Target raster file path or SpatRaster (optional). Used for resampling/masking.
#' @param varname Variable name when writing NetCDF (default "band").
#' @param if_aggregate Logical, whether to aggregate to target resolution (default TRUE).
#' @param if_resample Logical, whether to resample to target raster grid (default FALSE).
#' @param if_mask Logical, whether to mask raster to target raster extent (default FALSE).
#' @param na_value Numeric value to treat as NA (optional).
#' @param fun Aggregation function for downscaling (default: mean).
#' @param if_round_fact Logical, whether to round aggregation factors to integer (default TRUE).
#' @param if_return_raster Logical, whether to return the processed raster (default TRUE).
#'
#' @return A SpatRaster object (single or multi-layer) or output file paths.
#'
#' @examples
#' # Process without aggregation, only save layers
#' r <- raster_preprocess_save("input.tif", if_aggregate = FALSE, output = "out_folder")
#'
#' # Process multi-layer raster with aggregation + resampling
#' raster_preprocess_save("input_multilayer.tif",
#'                   output = "output_folder",
#'                   if_aggregate = TRUE,
#'                   if_resample = TRUE)
raster_preprocess_save <- function(input, output = NULL, res_tar = NULL, target = NULL,
                                   varname = "band", if_aggregate = TRUE,
                                   if_resample = FALSE, if_mask = FALSE,
                                   na_value = NULL, fun = mean,
                                   if_round_fact = TRUE,
                                   if_return_raster = TRUE) {

  # --- Load input raster ---
  r_in <- if (is.character(input)) terra::rast(input) else input

  # Replace specified NA value with proper NA
  if (!is.null(na_value)) r_in[r_in == na_value] <- NA

  # --- Handle target raster for resolution ---
  if (!is.null(target)) {
    r_tar <- if (is.character(target)) terra::rast(target)[[1]] else target[[1]]
    res_tar <- c(terra::xres(r_tar), terra::yres(r_tar))
  }

  # --- Aggregate if requested ---
  if (if_aggregate && !is.null(res_tar)) {
    # Calculate aggregation factors
    fact_x <- res_tar[1] / terra::xres(r_in)
    fact_y <- res_tar[2] / terra::yres(r_in)

    # Check if rounding is needed
    if (if_round_fact) {
      fact_x_rounded <- round(fact_x)
      fact_y_rounded <- round(fact_y)
      print(paste("Aggregation factors - X: from", fact_x, "rounded to", fact_x_rounded,
                  "Y: from", fact_y, "rounded to", fact_y_rounded))
      fact_x <- fact_x_rounded
      fact_y <- fact_y_rounded
    } else {
      print(paste("Aggregation factors - X:", fact_x, "Y:", fact_y))
    }

    print(paste("Input raster dimensions:", dim(r_in)[1], "x", dim(r_in)[2]))

    # Perform aggregation
    r_out <- if (fact_x >= 1 && fact_y >= 1) {
      terra::aggregate(r_in, fact = c(fact_x, fact_y), fun = fun, na.rm = TRUE)
    } else {
      warning("Aggregation factors < 1 — skipping aggregation.")
      r_in
    }

    print(paste("Output raster dimensions:", dim(r_out)[1], "x", dim(r_out)[2]))

  } else {
    r_out <- r_in
  }

  # --- Resample and mask if required ---
  if (if_resample && !is.null(target)) r_out <- terra::resample(r_out, r_tar, method = "bilinear")
  if (if_mask && !is.null(target)) r_out <- terra::mask(r_out, r_tar)

  n_layers <- terra::nlyr(r_out)

  # --- Output handling ---
  if (!is.null(output)) {
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

    if (length(output) != n_layers) stop("Length of 'output' must match number of layers in raster.")

    for (i in seq_len(n_layers)) {
      lyr <- r_out[[i]]
      out_i <- output[i]
      ext <- tools::file_ext(out_i)
      varname_i <- if (!is.null(names(r_out)[i]) && names(r_out)[i] != "" && names(r_out)[i] != "lyr.1") {
        names(r_out)[i]
      } else {
        if (n_layers == 1) varname else paste0(varname, "_", i)
      }

      if (tolower(ext) %in% c("nc", "cdf")) {
        terra::writeCDF(lyr, out_i, overwrite = TRUE, varname = varname_i)
      } else {
        terra::writeRaster(lyr, out_i, overwrite = TRUE)
      }
    }

    if (all(file.exists(output))) message("Saved files:\n", paste("  ", output, collapse = "\n"))
  }

  # Clean up
  rm(r_in)
  if (exists("r_tar")) rm(r_tar)
  gc()

  if (if_return_raster) return(r_out) else return(output)
}
