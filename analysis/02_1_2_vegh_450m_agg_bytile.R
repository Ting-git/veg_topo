
# ~ 128 min: UBELIX, 16 cores

# ------Set up------------------------------------------------------------------
# Load packages
library(terra)
library(furrr)
library(future)

source(here::here("config_ubelix.R"))
source(here::here("R/raster_preprocess_save.R")) # Set Na value and aggregation

# -------Configuration----------------------------------------------------------

# Input: read all 10m vegetation data paths (2651)
vegh_10m_tiles_path <- fs::dir_ls(path = vegh_10m_tiles_dir, glob = "*_Map.tif")
vegh_10m_tiles_path_sub <- vegh_10m_tiles_path[1:2651]  # for test

# Output: 2651 tiles with 450m resolution
vegh_450m_tiles_dir <- file.path(veg_topo_extr_dir, "data/global_vegh_450m/3_3_deg")
if (!dir.exists(vegh_450m_tiles_dir)) {
  dir.create(vegh_450m_tiles_dir, recursive = TRUE)
}

# -----Aggregation: 10m -> 450m ------------------------------------------------

# target layer
twi_450m_r <- terra::rast(twi_450m_mosaic_clean_path)
res_tar <- res(twi_450m_r)
rm(twi_450m_r ); gc()

# Set up parallel processing (adjust based on available CPU cores)
plan(multisession, workers = 16)
t0 <- Sys.time()

# Parallel processing of raster files with error handling
results <- future_map(

  vegh_10m_tiles_path_sub, # for test
  # vegh_10m_tiles_path,  # for all

  function(file) {
    tryCatch({

      # Set output file
      output <- file.path(
        vegh_450m_tiles_dir,
        paste0(sub("\\.tif$", "", basename(file)), "_to450m.nc")
      )

      # # Check if it has been processed
      # if (fs::file_exists(output) && fs::file_size(output) > 0) {
      #   return(list(success = TRUE, out_file = output, error = NULL))
      # }


      # Set 0 as NA value (0m canopy height represents not vegetated or water according to Lang et al. (2019))
      # Aggregates using TWI data from Marthews et al. (2015)
      # Return the saved path
      result <- raster_preprocess_save(
        input = file,
        output = output,
        res_tar = res_tar,
        na_value = 0,
        fun = mean,
        varname = "vegh",
        if_aggregate = TRUE,
        if_resample = FALSE,
        if_mask = FALSE,
        if_return_raster = FALSE
      )

      list(success = TRUE, out_file = result, error = NULL)
    }, error = function(e) {
      message(sprintf("❌ Error processing file: %s", file))
      message(sprintf("  → %s", e$message))
      list(success = FALSE, out_file = NULL, error = e$message)
    }, finally = { gc() })

  },
  .progress = FALSE,
  .options = furrr::furrr_options(seed = FALSE,
                                  globals = c("res_tar", "raster_preprocess_save", "vegh_450m_tiles_dir"),
                                  packages = c("terra", "fs"))
)

plan(sequential)
gc()

# print precess duration
message(sprintf("done [%.1fs]", difftime(Sys.time(), t0, units = "secs")))
saveRDS(results, file = here::here("data/results_vegh_aggregate.rds"))

