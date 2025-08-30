# Batch process multiple rasters: align and save as NetCDF
# @param input_list Named list of SpatRaster objects or file paths
# @param res_tar Target resolution (optional if target is provided)
# @param target Target SpatRaster or file path for alignment
# @param if_resample Logical, resample to target grid
# @param if_mask Logical, mask by target extent
# @param fun Aggregation function (default = mean)
# @param na_value Value to treat as NA
# @param output_list Named character vector of .nc output file paths (names must match input_list)
# @return Named list of processed SpatRaster objects
batch_process_rasters <- function(input_list, res_tar = NULL, target = NULL,
                                  if_resample = TRUE, if_mask = FALSE,
                                  fun = mean, na_value = NULL,
                                  output_list = NULL) {
  out_list <- list()
  for (name in names(input_list)) {
    r <- input_list[[name]]
    out_r <- process_raster(
      input = r,
      res_tar = res_tar,
      target = target,
      if_resample = if_resample,
      if_mask = if_mask,
      fun = fun,
      na_value = na_value,
      varname = name
    )

    # Always write NetCDF if output paths are provided
    if (!is.null(output_list)) {
      if (!name %in% names(output_list)) {
        stop("output_list must have the same names as input_list")
      }
      out_path <- output_list[[name]]
      if (!grepl("\\.nc$", out_path, ignore.case = TRUE)) {
        stop("All output_list file paths must end with .nc")
      }
      terra::writeCDF(out_r, out_path, overwrite = TRUE, varname = name)
    }

    out_list[[name]] <- out_r
  }
  return(out_list)
}
