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
#'
#' @return A SpatRaster object (single or multi-layer).
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
                              na_value = NULL, fun = mean, if_return_raster = TRUE) {

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
    fact_x <- res_tar[1] / terra::xres(r_in)
    fact_y <- res_tar[2] / terra::yres(r_in)
    r_out <- if (fact_x >= 1 && fact_y >= 1) {
      terra::aggregate(r_in, fact = c(fact_x, fact_y), fun = fun, na.rm = TRUE)
    } else {
      r_in
    }
  } else {
    r_out <- r_in
  }

  # --- Resample and mask if required ---
  if (if_resample && !is.null(target)) r_out <- terra::resample(r_out, r_tar, method = "bilinear")
  if (if_mask && !is.null(target)) r_out <- terra::mask(r_out, r_tar)

  n_layers <- terra::nlyr(r_out)

  # --- Output handling ---
  if (!is.null(output)) {
    # Case: output is a folder path (auto-generate NetCDF filenames)
    if (length(output) == 1 && (dir.exists(output) || !grepl("\\.", basename(output)))) {
      if (!dir.exists(output)) dir.create(output, recursive = TRUE)  # Create folder if not exist
      output <- sapply(seq_len(n_layers), function(i) {
        fname <- if (!is.null(names(r_out)[i]) && names(r_out)[i] != "") {
          names(r_out)[i]
        } else {
          paste0("lyr", i)
        }
        file.path(output, paste0(fname, ".nc"))
      })
    }

    # Ensure output length matches number of layers
    if (length(output) != n_layers) {
      stop("Length of 'output' must match number of layers in raster.")
    }

    # --- Save each layer ---
    for (i in seq_len(n_layers)) {
      lyr <- r_out[[i]]
      out_i <- output[i]
      ext <- tools::file_ext(out_i)
      # if named list, then set the varnames
      varname_i <- if (!is.null(names(r_out)[i]) && names(r_out)[i] != "") {
        names(r_out)[i]
      } else {
        paste0(varname, "_", i)
      }

      if (tolower(ext) %in% c("nc", "cdf")) {
        terra::writeCDF(lyr, out_i, overwrite = TRUE, varname = varname_i)  # Save as NetCDF
      } else {
        terra::writeRaster(lyr, out_i, overwrite = TRUE)  # Save other formats
      }
    }

    message("Saved files:")
    for (f in output) message("  ", f)
  }

  # Clean up
  rm(r_in)
  if (exists("r_tar")) rm(r_tar)
  gc()

 if (if_return_raster) return(r_out) else return(output)
}
