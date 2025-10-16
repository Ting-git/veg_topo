mosaic_tiles_gdal <- function(input_dir,
                              output_file = NULL,
                              pattern = "*.nc",
                              overwrite = TRUE,
                              num_threads = "ALL_CPUS",
                              max_files_per_vrt = 1000) {  # Added batch processing

  message("🚀 Fast mosaic mode - using GTiff format only")

  tile_paths <- list.files(path = input_dir,
                           pattern = glob2rx(pattern),
                           full.names = TRUE,
                           recursive = TRUE)

  if (length(tile_paths) == 0)
    stop("No matching files found in: ", input_dir)

  message("📁 Found ", length(tile_paths), " files")

  # Handle very large file sets by batching
  if (length(tile_paths) > max_files_per_vrt) {
    message("📦 Large file set detected, using batch processing...")
    return(mosaic_large_dataset(tile_paths, output_file, max_files_per_vrt,
                                num_threads, overwrite))
  }

  temp_mode <- FALSE
  if (is.null(output_file)) {
    output_file <- tempfile(fileext = ".tif")
    temp_mode <- TRUE
  }

  if (file.exists(output_file) && overwrite) {
    unlink(output_file)
  }

  # Use file list to avoid command line length limits
  list_file <- tempfile(fileext = ".txt")
  writeLines(tile_paths, list_file)

  # Step 1: Build VRT with error handling
  vrt_file <- tempfile(fileext = ".vrt")

  message("🧱 Building VRT...")
  vrt_result <- system2("gdalbuildvrt",
                        c("-input_file_list", list_file,
                          "-resolution", "user",
                          "-r", "bilinear",
                          "-tap",
                          vrt_file),
                        stdout = TRUE, stderr = TRUE)

  # Check if VRT creation succeeded
  if (!file.exists(vrt_file) || file.size(vrt_file) == 0) {
    unlink(c(vrt_file, list_file))
    stop("VRT creation failed. GDAL output: ", paste(vrt_result, collapse = "\n"))
  }

  # Step 2: Convert to GTiff with error handling
  message("⚡ Creating GTiff mosaic...")
  translate_result <- system2("gdal_translate",
                              c("-of", "GTiff",
                                "-co", paste0("NUM_THREADS=", num_threads),
                                "-co", "COMPRESS=DEFLATE",
                                "-co", "PREDICTOR=2",
                                "-co", "BIGTIFF=IF_SAFER",
                                "-co", "TILED=YES",
                                vrt_file, output_file),
                              stdout = TRUE, stderr = TRUE)

  # Check if output file was created
  if (!file.exists(output_file)) {
    unlink(c(vrt_file, list_file))
    stop("GTiff creation failed. GDAL output: ", paste(translate_result, collapse = "\n"))
  }

  # Cleanup
  unlink(c(vrt_file, list_file))

  if (temp_mode) {
    message("📤 Returning GTiff raster object")
    return(terra::rast(output_file))
  } else {
    message("💾 GTiff saved to: ", output_file)
    return(invisible(output_file))
  }
}

# Helper function for large datasets
mosaic_large_dataset <- function(tile_paths, output_file, max_files_per_vrt,
                                 num_threads, overwrite) {

  # Create intermediate VRTs in batches
  n_batches <- ceiling(length(tile_paths) / max_files_per_vrt)
  message("🔨 Processing ", n_batches, " batches...")

  intermediate_vrts <- character(n_batches)

  for (i in seq_len(n_batches)) {
    message("⏳ Processing batch ", i, "/", n_batches)
    start_idx <- (i-1) * max_files_per_vrt + 1
    end_idx <- min(i * max_files_per_vrt, length(tile_paths))
    batch_files <- tile_paths[start_idx:end_idx]

    # Create list file for this batch
    list_file <- tempfile(fileext = ".txt")
    writeLines(batch_files, list_file)

    # Create VRT for this batch
    vrt_file <- tempfile(fileext = ".vrt")
    vrt_result <- system2("gdalbuildvrt",
                          c("-input_file_list", list_file, vrt_file),
                          stdout = TRUE, stderr = TRUE)

    if (!file.exists(vrt_file)) {
      stop("Batch VRT creation failed for batch ", i, ": ",
           paste(vrt_result, collapse = "\n"))
    }

    intermediate_vrts[i] <- vrt_file
    unlink(list_file)
  }

  # Create final mosaic from intermediate VRTs
  message("🔗 Combining batches into final mosaic...")
  final_list_file <- tempfile(fileext = ".txt")
  writeLines(intermediate_vrts, final_list_file)

  temp_mode <- is.null(output_file)
  if (temp_mode) {
    output_file <- tempfile(fileext = ".tif")
  }

  # Build final VRT from intermediate VRTs
  final_vrt <- tempfile(fileext = ".vrt")
  vrt_result <- system2("gdalbuildvrt",
                        c("-input_file_list", final_list_file,
                          "-resolution", "user",
                          "-r", "bilinear",
                          "-tap",
                          final_vrt),
                        stdout = TRUE, stderr = TRUE)

  # Convert to GTiff
  if (file.exists(final_vrt)) {
    translate_result <- system2("gdal_translate",
                                c("-of", "GTiff",
                                  "-co", paste0("NUM_THREADS=", num_threads),
                                  "-co", "COMPRESS=DEFLATE",
                                  "-co", "PREDICTOR=2",
                                  "-co", "BIGTIFF=IF_SAFER",
                                  "-co", "TILED=YES",
                                  final_vrt, output_file),
                                stdout = TRUE, stderr = TRUE)
  }

  # Cleanup intermediates
  unlink(intermediate_vrts)
  unlink(c(final_vrt, final_list_file))

  if (!file.exists(output_file)) {
    stop("Final mosaic creation failed: ", paste(translate_result, collapse = "\n"))
  }

  if (temp_mode) {
    message("📤 Returning GTiff raster object")
    return(terra::rast(output_file))
  } else {
    message("💾 GTiff saved to: ", output_file)
    return(invisible(output_file))
  }
}
