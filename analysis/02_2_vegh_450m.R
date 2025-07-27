# ------Set up------------------------------------------------------------------------

library(terra)
library(furrr)
library(stringr)

source(here::here("config.R"))
source(here::here("R/aggregate_byfile.R"))

# -------Configuration-----------------------------------------------------------------------

vegh_10m_tiles_path <- fs::dir_ls(path = vegh_10m_tiles_dir, glob = "*_Map.tif")
# vegh_10m_tiles_path_sub <- vegh_10m_tiles_path[1:10]

# -----Aggregation: workers=8, 97 min--------------------------------------------
# vegh 10m --> vegh 450m

# get the resolution information of target raster(ga2 TWI)
rast_tar <- rast(twi_450m_mosaic_clean_path)
xres_tar <- xres(rast_tar)
yres_tar <- yres(rast_tar)

rm(rast_tar)
gc()

# Set up parallel processing (adjust based on available CPU cores)

plan(multisession, workers = 8)
t0 <- Sys.time()

# Parallel processing of raster files with error handling
# results <- future_map(vegh_10m_tiles_path_sub, function(file) { # for test
results <- future_map(vegh_10m_tiles_path, function(file) {

  output_path <- paste0(
    vegh_450m_tiles_dir, "/",
    str_remove(basename(file), ".tif"),
    "_to450m.nc"
  )

  tryCatch({

    # Run the main aggregation function
    result <- aggregate_byfile(input_path = file,
                               output_path = output_path,
                               target_path =  twi_450m_mosaic_clean_path,
                               if_resample = FALSE)

    return(list(success = TRUE, out_file = result, error = NULL))

  }, error = function(e) {

    # On error, print and store the error message
    message(sprintf("❌ Error processing file: %s", file))
    message(sprintf("  → %s", e$message))
    return(list(success = FALSE, out_file = NULL, error = e$message))

  })
})

plan(sequential)
gc()

# print precess duration
message(sprintf("done [%.1fs]", difftime(Sys.time(), t0, units = "secs")))
saveRDS(results, file = here::here("data/results_vegh_aggregate.rds"))

# ------------Combination------10 min-------------------------------------------------

# Load rasters and merge into a mosaic
vegh_450m_tiles_path <- fs::dir_ls(path = vegh_450m_tiles_dir, glob = "*_to450m.nc")
vegh_450m_tiles_r <- lapply(vegh_450m_tiles_path, terra::rast)

vegh_450m_mosaic_r <- do.call(terra::merge, unname(vegh_450m_tiles_r))

# Save merged raster
terra::writeCDF(vegh_450m_mosaic_r, vegh_450m_mosaic_path, overwrite = TRUE)
message("✅ Saved successfully to: ", vegh_450m_mosaic_path)

rm(vegh_450m_mosaic_r, vegh_450m_tiles_r)
gc()

