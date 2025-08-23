# ------Set up------------------------------------------------------------------
# Load packages
library(terra)
library(furrr)
library(stringr)

source(here::here("config.R"))
source(here::here("R/aggregate_byfile.R"))

# -------Configuration----------------------------------------------------------

vegh_10m_tiles_path <- fs::dir_ls(path = vegh_10m_tiles_dir, glob = "*_Map.tif")
# vegh_10m_tiles_path_sub <- vegh_10m_tiles_path[1:10]

vegh_450m_tiles_dir <- file.path(veg_topo_extr_dir, "data/global_vegh_450m/3_3_deg")
if (!dir.exists(vegh_450m_tiles_dir)) {
  dir.create(vegh_450m_tiles_dir, recursive = TRUE)
}

# -----Aggregation: 10m -> 450m: workers=8, 97 min------------------------------

# target layer
twi_450m_r <- terra::rast(twi_450m_mosaic_clean_path)
twi_450m_r
xres_tar <- res(twi_450m_r)[1]
yres_tar <- res(twi_450m_r)[2]

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
    result <- aggregate_byfile(
      input_path = file,
      output_path = output_path,
      xres_tar = xres_tar,
      yres_tar = yres_tar,
      # na_value = 0, # ????????????
      if_resample = FALSE
    )

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

# ------------Combination------15 min-------------------------------------------

# Load rasters and merge into a mosaic
vegh_450m_tiles_path <- fs::dir_ls(path = vegh_450m_tiles_dir, glob = "*_to450m.nc")
vegh_450m_tiles_r <- lapply(vegh_450m_tiles_path, terra::rast)
vegh_450m_mosaic_r <- do.call(terra::merge, unname(vegh_450m_tiles_r))

# Resample using TWI data as target
vegh_450m_mosaic_rr <- terra::resample(vegh_450m_mosaic_r, twi_450m_r, method = "bilinear")

# Save merged raster
terra::writeCDF(vegh_450m_mosaic_rr, vegh_450m_mosaic_path, overwrite = TRUE, varname = "vegh")
if(file.exists(vegh_450m_mosaic_path)) message("✅ Saved successfully to: ", vegh_450m_mosaic_path)

# check vegh
vegh <- rast(vegh_450m_mosaic_path)
plot(vegh)

# ---------- Delete intermediate data ------------------------------------------
# files <- list.files(vegh_450m_tiles_dir, full.names = TRUE, recursive = TRUE)
# if (length(files) > 0) file.remove(files)

# r1 <- rast("/data_2/archive/vegheight_lang_2023/data/3deg_cogs/ETH_GlobalCanopyHeight_10m_2020_N21W003_Map.tif")
# plot(r1)
#
# r2 <- rast("/data_2/scratch/ting/veg_topo_data/data/global_vegh_450m/3_3_deg/ETH_GlobalCanopyHeight_10m_2020_N21W003_Map_to450m.nc")
# plot(r2)
