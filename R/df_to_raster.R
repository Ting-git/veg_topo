df_to_raster <- function(df_calc,
                         x = "lon",
                         y = "lat",
                         value = "rin",
                         template_raster = NULL,
                         output_file = NULL,
                         varname = "band",
                         overwrite = TRUE,
                         return_raster = TRUE) {

  # Build raster from xyz
  r_out <- terra::rast(df_calc[, c(x, y, value)],
                       type = "xyz",
                       crs = crs(template_raster))

  # Extend and mask
  r_out <- terra::extend(r_out, template_raster)

  # Save if output_file is provided
  if (!is.null(output_file)) {
    ext <- tools::file_ext(output_file)

    if (tolower(ext) %in% c("nc", "cdf", "nc4")) {
      terra::writeCDF(r_out, output_file, varnames = varname, overwrite = overwrite)
    } else if (tolower(ext) %in% c("tif", "tiff")) {
      terra::writeRaster(
        r_out,
        output_file,
        filetype = "GTiff",
        gdal = c("COMPRESS=LZW", "BIGTIFF=YES", "TILED=YES",
                 "BLOCKXSIZE=256", "BLOCKYSIZE=256"),
        overwrite = overwrite,
        datatype = "FLT4S",
        NAflag = -9999
      )
    } else {
      # Default to native raster format
      terra::writeRaster(r_out, output_file, overwrite = overwrite)
    }

    if (file.exists(output_file)) message("✅ Saved: ", output_file)
  }

  # Return based on option
  if (return_raster) return(invisible(r_out))
  return(invisible(output_file))
}
