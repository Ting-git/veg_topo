
# source the single-raster function
source(here::here("R/process_raster.R"))

#' Batch process rasters: align multiple rasters to a target
#'
#' @param raster_list Named list of SpatRaster objects or file paths.
#' @param target Target SpatRaster or file path to align to.
#' @param if_resample Logical, whether to resample to target.
#' @param if_mask Logical, whether to mask to target extent.
#' @param fun Aggregation function (default: mean)
#' @param na_value Numeric value to treat as NA
#' @param output_dir Optional directory to save outputs. Filenames will be names of raster_list.
#'
#' @return Named list of processed SpatRaster objects.
batch_process_rasters <- function(input_list, target,
                                  if_resample = TRUE, if_mask = FALSE,
                                  fun = mean, na_value = NULL,
                                  output_dir = NULL) {
  out_list <- list()
  for (name in names(input_list)) {
    r <- input_list[[name]]
    out_r <- process_raster(
      input = r,
      target = target,
      if_resample = if_resample,
      if_mask = if_mask,
      fun = fun,
      na_value = na_value,
      varname = name
    )

    if (!is.null(output_dir)) {
      out_path <- file.path(output_dir, paste0(name, ".tif"))
      terra::writeRaster(out_r, out_path, overwrite = TRUE)
    }

    out_list[[name]] <- out_r
  }
  return(out_list)
}
