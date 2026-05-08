# ~ 3 h
# Total valid tiles:16378
# Start processing: 1:16378 (total 16378 Tiles)
# Successful: 16369 tiles; Failed: 9 tiles
# ❌ Error in tile 77N_121W: 'from' must be a finite number
# ❌ Error in tile 52N_177E: 'from' must be a finite number
# ❌ Error in tile 52N_178E: 'from' must be a finite number
# ❌ Error in tile 52N_179E: 'from' must be a finite number
# ❌ Error in tile 51N_177E: 'from' must be a finite number
# ❌ Error in tile 51N_178E: 'from' must be a finite number
# ❌ Error in tile 51N_179E: 'from' must be a finite number
# ❌ Error in tile 20S_177E: 'from' must be a finite number
# ❌ Error in tile 20S_178E: 'from' must be a finite number
# -------------------- Set Up --------------------------------------------------
library(terra)
library(dplyr)
library(tidyr)
library(arrow)
library(future)
library(furrr)

# Load custom functions
source(here::here("R/config.R"))
source(here::here("R/extent_to_tile_ids.R"))
source(here::here("R/raster_preprocess_save.R"))
source(here::here("R/aggregate_topography.R"))
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/helpers.R"))
source(here::here("R/calc_sw_in.R"))

# Set worker numbers for different system
if (hostname == "dash") workers = 2 else workers = 80
message("→ using ", workers, " workers")

# set output directory
if (!dir.exists(rf_sample_data_tiles_dir)) {
  dir.create(rf_sample_data_tiles_dir, recursive = TRUE)
  message("Directory created: ", rf_sample_data_tiles_dir)
}

# -------------------- Set Processed tile idx --------------------------------
rf_tiles_all <- arrow::read_parquet(rf_tiles_path)  # 16378 tiles in total
ntiles <- nrow(rf_tiles_all)  # Fixed: nrows() -> nrow()
message("Total valid tiles:", ntiles)  # 16378 tiles in total

start_idx <- 1
end_idx   <- ntiles

message(sprintf("Start processing: %d:%d (total %d Tiles)", start_idx, end_idx, nrow(rf_tiles_all)))  # Fixed: rf_tiles -> rf_tiles_all

rf_tiles <- rf_tiles_all[start_idx:end_idx, ]  # 16378 tiles in total

# random seed for sampling
set.seed(124563)
# -------------------- Tile Processing Function --------------------------------
# Function to process for each Lidar tile
sample_by_tile <- function(reg_id) {

  tryCatch({

    terra::terraOptions(progress = 0)

    # ----- Region info -----
    # Create alignment template (30m resolution)
    reg_row <- rf_tiles[rf_tiles$reg_id == reg_id, ]
    reg_id <- reg_row$reg_id
    reg_extent <- terra::ext(reg_row$xmin, reg_row$xmax, reg_row$ymin, reg_row$ymax)
    out_file <- file.path(rf_sample_data_tiles_dir, sprintf("tile_%s.parquet", reg_id))

    # message("⭐️⭐️⭐️ Processing:", reg_id, " ⭐️⭐️⭐️")
    # ----- Skip if already processed -----
    if (file.exists(out_file)) {
      # message(sprintf("Region %s completed", reg_id))
      return(TRUE)
    }

    # ----- High-res valid window mask -----
    valid_win_hr <- terra::crop(rast(valid_win_path), reg_extent) |>
      terra::disagg(fact = 200)  # 0.05° / 0.00025°

    # ----- Load environmental layers -----
    # Topographic Wetness Index
    twi <- rast(twi_30m_path) |> terra::crop(reg_extent)
    twi <- twi / 100  # Scale to original values

    # Vegetation height
    vegh <- suppressMessages({
      extent_to_tile_ids( reg_extent, tile_size = 3, return_raster = TRUE, source = "lang_vegh_10m", tiles_dir = vegh_10m_tiles_dir) |>
      # Set 0 as NA value (0m canopy height represents not vegetated or water according to Lang et al. (2019))
      raster_preprocess_save(
        na_value = 0,
        fun = mean,
        target = twi,
        if_aggregate = TRUE,
        if_round_fact = TRUE,
        if_resample = TRUE,
        if_mask = FALSE,
        if_return_raster = TRUE
        )
    })

    # DEM /Elevation
    dem <- extent_to_tile_ids(reg_extent,tile_size = 1, return_raster = TRUE, source = "copernicus_dem_30m", tiles_dir = dem_30m_copernicus_dir)

    # Calculate slope and aspect, resample to TWI resolution
    aligned <- suppressMessages({aggregate_topography(
        dem,
        res_tar = NULL,
        target = twi,
        if_aggregate = FALSE,
        if_resample = TRUE
      )})

    # Climate data
    mat <- rast(mat_1km_file) |> terra::crop(reg_extent) |> terra::resample(twi, method = "bilinear")
    map <- rast(map_1km_file) |> terra::crop(reg_extent) |> terra::resample(twi, method = "bilinear")
    srad <- rast(srad_1km_file) |> terra::crop(reg_extent) |> terra::resample(twi, method = "bilinear")

    # ----- Stack and mask all layers -----
    # Stack all layers and apply valid window mask
    stacked <- c(twi, vegh, aligned[["dem"]], aligned[["slope"]],
                 aligned[["aspect"]], mat, map, srad) |>
      terra::mask(valid_win_hr, maskvalues = FALSE)

    # ----- Sample 5 points per 0.05° window -----
    # Some sampling failed due to no Worldclim data (MAT,MAP,SRAD), like "78N_121W" !!!!!!!!!!!!!!!!!!
    df_samp <- as.data.frame(stacked, xy = TRUE, na.rm = TRUE) |>
      create_spatial_windows(
        coord_vars = c("lon", "lat"),
        value_vars = c("twi", "vegh", "elv", "slope", "aspect", "mat", "map", "srad"),
        dwin = 0.05
      ) |>
      tidyr::drop_na() |>
      group_by(lon_mid, lat_mid) |>
      slice_sample(n = 5) |>
      ungroup()

    # ----- Calculate Radiation Index (Rin) -----
    df_samp <- df_samp |>
      mutate(
        sw_in_uneven = calc_sw_in(lat, slope, aspect, year = 2020),
        sw_in_flat = calc_sw_in(lat, 0, 0, year = 2020),
        rin = sw_in_uneven / sw_in_flat
      ) |>
      dplyr::select(lon, lat, vegh, elv, twi, rin, mat, map, srad)

    # ----- Save results -----
    arrow::write_parquet(df_samp, out_file)
    # if(!file.exists(out_file)) message("✅ Saved: ", out_file)

    return(TRUE)

  }, error = function(e) {
    message("\n❌ Error in tile ", reg_id, ": ", e$message)
    return(FALSE)
  })
}

# -------------------- Parallel Execution --------------------------------------

# Set up cluster plan
plan(cluster, workers = workers)
tictoc::tic("⏱️ Total processing time")

results <- future_map(
  rf_tiles$reg_id,
  sample_by_tile,
  .progress = FALSE,
  .options = furrr_options(seed = TRUE)
)

plan(sequential)
tictoc::toc()

# -------------------- Summary -------------------------------------------------
success_count <- sum(unlist(results))
fail_count <- length(results) - success_count

message("\n", paste(rep("=", 50), collapse = ""))
message("PROCESSING SUMMARY")
message(paste(rep("=", 50), collapse = ""))
message(sprintf("Successful: %d tiles", success_count))
message(sprintf("Failed: %d tiles", fail_count))
message(sprintf("Output directory: %s", rf_sample_data_tiles_dir))
