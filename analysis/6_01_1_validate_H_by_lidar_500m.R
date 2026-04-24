# ～ 12h on UBELIX
# ❌ Error in validation for "35WNT": [mosaic] resolution does not match

library(terra)
library(dplyr)
library(sf)
library(tidyr)
library(furrr)
library(future)
library(ggplot2)
library(patchwork)

source(here::here("R/config.R"))
source(here::here("R/get_lonlat_extent.R")) # in create_aligned_template() to process lidar raster
source(here::here("R/create_aligned_template.R"))
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/calculate_correlation_bywin.R"))
source(here::here("R/raster_preprocess_save.R"))
source(here::here("R/df_to_raster.R"))
source(here::here("R/aggregate_topography.R"))
source(here::here("R/extent_to_tile_ids.R"))
source(here::here("R/helpers.R"))
source(here::here("R/calc_sw_in.R"))

# Set worker numbers for different system
if (hostname == "dash") {
  batch_size <- 10000
  workers <- 1
} else {
  batch_size <- 1000000
  workers <- 2
}
message("→ using ", workers, " workers")
message("→ batch size: ", batch_size)

# All ALS_MAX data
file_bases <- c("30SWH","30TWN","30UVD","31UFT","32TMS",
                "32TMT", "34WFS", "35VMF","35VNL","35WMR","35WNT",
                "06VXR", "08WNA", "12VUN", "16PHS", "32MPC", "32MQE",
                "06WVT", "10TER", "13UEA", "16SGE", "32MPE", "32NNF")

# Function to process for each Lidar tile
validate_vegetation_heights <- function(file_base) {

  # ============================================================================
  # 1. INITIALIZATION
  # ============================================================================
  message("⭐️⭐️⭐️ Processing:", file_base, " ⭐️⭐️⭐️")

  # Create output directory
  if (!dir.exists(h_validation_dir)) dir.create(h_validation_dir, recursive = TRUE)

  # ----- Check if input file exists -----
  # LiDAR input path and alignment grid
  lidar_path <- file.path(lidar_asl_dir, paste0(file_base, ".tif"))
  lidar_path <- if (file.exists(lidar_path)) lidar_path else file.path(lidar_lvis_dir, paste0(file_base, ".tif"))
  if (!file.exists(lidar_path)) return(message("❌ LiDAR file not found"))

  # ----- Region info -----
  # Create alignment template (30m resolution)
  align_r <- create_aligned_template( lidar_path , res_out = 0.00025)
  reg_extent <- ext(align_r)
  message("Extent:",reg_extent[1], ", ", reg_extent[2],", ",reg_extent[3],", ",reg_extent[4], " (xmin,xmax,ymin,ymax)")

  # ----- Output file paths -----
  hlidar_file <- file.path(h_validation_dir, paste0(file_base, "_hlidar.tif"))
  hlang_file <- file.path(h_validation_dir, paste0(file_base, "_hlang.tif"))
  twi_file <- file.path(h_validation_dir, paste0(file_base, "_twi.tif"))
  dem_file <- file.path(h_validation_dir, paste0(file_base, "_dem.tif"))
  slope_file <- file.path(h_validation_dir, paste0(file_base, "_slope.tif"))
  aspect_file <- file.path(h_validation_dir, paste0(file_base, "_aspect.tif"))
  rin_file <- file.path(h_validation_dir, paste0(file_base, "_rin.tif"))

  r_hlidar_twi_file <- file.path(h_validation_dir, paste0(file_base, "_r_hlidar_twi.tif"))
  p_hlidar_twi_file <- file.path(h_validation_dir, paste0(file_base, "_p_hlidar_twi.tif"))
  r_hlang_twi_file <- file.path(h_validation_dir, paste0(file_base, "_r_hlang_twi.tif"))
  p_hlang_twi_file <- file.path(h_validation_dir, paste0(file_base, "_p_hlang_twi.tif"))

  r_hlidar_rin_file <- file.path(h_validation_dir, paste0(file_base, "_r_hlidar_rin.tif"))
  p_hlidar_rin_file <- file.path(h_validation_dir, paste0(file_base, "_p_hlidar_rin.tif"))
  r_hlang_rin_file <- file.path(h_validation_dir, paste0(file_base, "_r_hlang_rin.tif"))
  p_hlang_rin_file <- file.path(h_validation_dir, paste0(file_base, "_p_hlang_rin.tif"))

  # ============================================================================
  # 2. DATA PREPARATION - HEIGHT AND TOPOGRAPHY
  # ============================================================================
  message("📊 Data preparation (H, Topo)...")

  tryCatch({
    # --- 2.1 LiDAR height (reference) ---
    lidar <- rast(lidar_path) |> clamp(lower = 0, upper = 80, values = FALSE) # Clean

    # Project LiDAR to template
    vegh_lidar <- project(lidar, align_r, method = "average")
    terra::writeRaster(vegh_lidar, filename = hlidar_file, overwrite = TRUE,
                       filetype = "GTiff", datatype = "FLT4S")
    message("✅ Saved: ", hlidar_file)

    # --- 2.2 TWI (Topographic Wetness Index) ---
    twi_raw <- terra::crop(terra::rast(twi_30m_path), reg_extent) / 100
    twi_rc <- raster_preprocess_save(
      input = twi_raw,
      output = twi_file,
      target = vegh_lidar,
      varname = "twi",
      if_aggregate = FALSE,
      if_resample = FALSE,
      if_mask = TRUE,
      if_return_raster = TRUE
    )

    # --- 2.3 Lang et al. height ---
    vegh_lang_raw <- extent_to_tile_ids(
      reg_extent,
      tile_size = 3,
      return_raster = TRUE,
      source = "lang_vegh_10m",
      tiles_dir = vegh_10m_tiles_dir
    )

    vegh_lang <- raster_preprocess_save(
      input = vegh_lang_raw,
      output = hlang_file,
      target = vegh_lidar,
      na_value = 0,
      fun = mean,
      varname = "vegh_lang",
      if_aggregate = TRUE,
      if_resample = TRUE,
      if_mask = TRUE,
      if_return_raster = TRUE
    )

    # --- 2.4 DEM, slope, aspect ---
    dem_raw <- extent_to_tile_ids(
      reg_extent,
      tile_size = 1,
      return_raster = TRUE,
      source = "copernicus_dem_30m",
      tiles_dir = dem_30m_copernicus_dir
    )

    aligned <- aggregate_topography(
      dem_raw,
      res_tar = NULL,
      target = align_r,
      if_aggregate = FALSE,
      if_resample = TRUE
    )

    # Save DEM
    dem <- raster_preprocess_save(
      input = aligned[["dem"]],
      output = dem_file,
      target = vegh_lidar,
      varname = "dem",
      if_aggregate = FALSE,
      if_resample = FALSE,
      if_mask = TRUE,
      if_return_raster = TRUE
    )

    # Save slope
    slope <- raster_preprocess_save(
      input = aligned[["slope"]],
      output = slope_file,
      target = vegh_lidar,
      varname = "slope",
      if_aggregate = FALSE,
      if_resample = FALSE,
      if_mask = TRUE,
      if_return_raster = TRUE
    )

    # Save aspect
    aspect <- raster_preprocess_save(
      input = aligned[["aspect"]],
      output = aspect_file,
      target = vegh_lidar,
      varname = "aspect",
      if_aggregate = FALSE,
      if_resample = FALSE,
      if_mask = TRUE,
      if_return_raster = TRUE
    )

    # ============================================================================
    # 3. RADIATION (Rin) CALCULATION
    # ============================================================================
    message("☀️ Data preparation (Rin)...")

    # Extract topography data to data frame
    df_topo <- as.data.frame(dem, xy = TRUE) |>
      left_join(as.data.frame(slope, xy = TRUE), by = c("x", "y")) |>
      left_join(as.data.frame(aspect, xy = TRUE), by = c("x", "y")) |>
      tidyr::drop_na()

    names(df_topo) <- c("lon", "lat", "dem", "slope", "aspect")

    if (nrow(df_topo) == 0) {
      warning("No valid cells after drop_na")
      return(FALSE)
    }

    # Calculate solar radiation in batches
    n_rows <- nrow(df_topo)
    n_batches <- ceiling(n_rows / batch_size)

    sw_in_uneven <- numeric(n_rows)
    sw_in_flat <- numeric(n_rows)

    for (i in seq(1, n_rows, by = batch_size)) {
      idx <- i:min(i + batch_size - 1, n_rows)

      sw_in_uneven[idx] <- calc_sw_in(
        df_topo$lat[idx],
        df_topo$slope[idx],
        df_topo$aspect[idx],
        year = 2020
      )

      sw_in_flat[idx] <- calc_sw_in(
        df_topo$lat[idx],
        rep(0, length(idx)),
        rep(0, length(idx)),
        year = 2020
      )
    }

    # Compute radiation index (Rin)
    df_calc <- df_topo |> mutate(rin = sw_in_uneven / sw_in_flat)

    # Convert to raster and save
    rin <- df_to_raster(df_calc, "lon", "lat", "rin",
                        template_raster = vegh_lidar,
                        output_file = rin_file,
                        varname = "rin",
                        overwrite = TRUE,
                        return_raster = TRUE
    )

    # Clean up
    rm(aligned, sw_in_uneven, sw_in_flat)
    gc()

    # ============================================================================
    # 4. CORRELATION ANALYSIS (500m windows)
    # ============================================================================

    message("\n📈 Calculating correlations...")

    twi_rc <- rast(twi_file)
    rin <- rast(rin_file)
    vegh_lang <- rast(hlang_file)
    vegh_lidar <- rast(hlidar_file)

    # Stack all rasters
    stacked <- c(twi_rc, rin, vegh_lang, vegh_lidar)

    message("\n🪟 Create spatial windows...")
    # Create spatial windows
    df_win <- create_spatial_windows(
      stacked,
      value_vars = c("twi", "rin", "vegh_lang", "vegh_lidar"),
      dwin = 0.005
    )

    # ----------------------------------------------------------------------------
    # 4.1 Correlation with TWI
    # ----------------------------------------------------------------------------
    message("\n🔗 Correlation H-TWI...")
    df_cor_lang_twi <- calculate_correlation_bywin(df_win, x = "twi", y = "vegh_lang")
    df_cor_lidar_twi <- calculate_correlation_bywin(df_win, x = "twi", y = "vegh_lidar")

    df_combined_twi <- merge(
      df_cor_lang_twi, df_cor_lidar_twi,
      by = c("lon_mid", "lat_mid"),
      suffixes = c("_lang", "_lidar")
    )

    r_temp_500m <- create_aligned_template(vegh_lidar, res_out = 0.005)

    # Save TWI correlation rasters
    r_hlidar_twi <- df_to_raster(
      df_combined_twi, "lon_mid", "lat_mid", "correlation_lidar",
      template_raster = r_temp_500m,
      output_file = r_hlidar_twi_file,
      varname = "r_hlidar_twi",
      overwrite = TRUE,
      return_raster = TRUE
    )

    p_hlidar_twi <- df_to_raster(
      df_combined_twi, "lon_mid", "lat_mid", "cor_pval_lidar",
      template_raster = r_temp_500m,
      output_file = p_hlidar_twi_file,
      varname = "p_hlidar_twi",
      overwrite = TRUE,
      return_raster = TRUE
    )

    r_hlang_twi <- df_to_raster(
      df_combined_twi, "lon_mid", "lat_mid", "correlation_lang",
      template_raster = r_temp_500m,
      output_file = r_hlang_twi_file,
      varname = "r_hlang_twi",
      overwrite = TRUE,
      return_raster = TRUE
    )

    p_hlang_twi <- df_to_raster(
      df_combined_twi, "lon_mid", "lat_mid", "cor_pval_lang",
      template_raster = r_temp_500m,
      output_file = p_hlang_twi_file,
      varname = "p_hlang_twi",
      overwrite = TRUE,
      return_raster = TRUE
    )

    # ----------------------------------------------------------------------------
    # 4.2 Correlation with Rin
    # ----------------------------------------------------------------------------
    message("\n🔗 Correlation H-Rin...")
    df_cor_lang_rin <- calculate_correlation_bywin(df_win, x = "rin", y = "vegh_lang")
    df_cor_lidar_rin <- calculate_correlation_bywin(df_win, x = "rin", y = "vegh_lidar")

    df_combined_rin <- merge(
      df_cor_lang_rin, df_cor_lidar_rin,
      by = c("lon_mid", "lat_mid"),
      suffixes = c("_lang", "_lidar")
    )

    # Save Rin correlation rasters
    r_hlidar_rin <- df_to_raster(
      df_combined_rin, "lon_mid", "lat_mid", "correlation_lidar",
      template_raster = r_temp_500m,
      output_file = r_hlidar_rin_file,
      varname = "r_hlidar_rin",
      overwrite = TRUE,
      return_raster = TRUE
    )

    p_hlidar_rin <- df_to_raster(
      df_combined_rin, "lon_mid", "lat_mid", "cor_pval_lidar",
      template_raster = r_temp_500m,
      output_file = p_hlidar_rin_file,
      varname = "p_hlidar_rin",
      overwrite = TRUE,
      return_raster = TRUE
    )

    r_hlang_rin <- df_to_raster(
      df_combined_rin, "lon_mid", "lat_mid", "correlation_lang",
      template_raster = r_temp_500m,
      output_file = r_hlang_rin_file,
      varname = "r_hlang_rin",
      overwrite = TRUE,
      return_raster = TRUE
    )

    p_hlang_rin <- df_to_raster(
      df_combined_rin, "lon_mid", "lat_mid", "cor_pval_lang",
      template_raster = r_temp_500m,
      output_file = p_hlang_rin_file,
      varname = "p_hlang_rin",
      overwrite = TRUE,
      return_raster = TRUE
    )

    # ============================================================================
    # 5. SUMMARY STATISTICS
    # ============================================================================

    # --- 5.1 TWI correlation summary ---
    cat("\n=== H-TWI Summary ===\n")
    summary_twi <- data.frame(
      Variable = c("correlation_lang", "correlation_lidar"),
      Mean = c(mean(df_combined_twi$correlation_lang, na.rm = TRUE),
               mean(df_combined_twi$correlation_lidar, na.rm = TRUE)),
      SD = c(sd(df_combined_twi$correlation_lang, na.rm = TRUE),
             sd(df_combined_twi$correlation_lidar, na.rm = TRUE)),
      Min = c(min(df_combined_twi$correlation_lang, na.rm = TRUE),
              min(df_combined_twi$correlation_lidar, na.rm = TRUE)),
      Max = c(max(df_combined_twi$correlation_lang, na.rm = TRUE),
              max(df_combined_twi$correlation_lidar, na.rm = TRUE))
    )
    print(summary_twi)

    df_combined_twi$cor_diff <- df_combined_twi$correlation_lang - df_combined_twi$correlation_lidar
    cor_between_twi <- cor(
      df_combined_twi$correlation_lang,
      df_combined_twi$correlation_lidar,
      use = "complete.obs"
    )
    cat("Correlation between r(H_lang, TWI) and r(H_lidar, TWI):", cor_between_twi, "\n")

    # --- 5.2 Rin correlation summary ---
    cat("\n=== H-Rin Summary ===\n")
    summary_rin <- data.frame(
      Variable = c("correlation_lang", "correlation_lidar"),
      Mean = c(mean(df_combined_rin$correlation_lang, na.rm = TRUE),
               mean(df_combined_rin$correlation_lidar, na.rm = TRUE)),
      SD = c(sd(df_combined_rin$correlation_lang, na.rm = TRUE),
             sd(df_combined_rin$correlation_lidar, na.rm = TRUE)),
      Min = c(min(df_combined_rin$correlation_lang, na.rm = TRUE),
              min(df_combined_rin$correlation_lidar, na.rm = TRUE)),
      Max = c(max(df_combined_rin$correlation_lang, na.rm = TRUE),
              max(df_combined_rin$correlation_lidar, na.rm = TRUE))
    )
    print(summary_rin)

    df_combined_rin$cor_diff <- df_combined_rin$correlation_lang - df_combined_rin$correlation_lidar
    cor_between_rin <- cor(
      df_combined_rin$correlation_lang,
      df_combined_rin$correlation_lidar,
      use = "complete.obs"
    )
    cat("Correlation between r(H_lang, Rin) and r(H_lidar, Rin):", cor_between_rin, "\n")

    message("\n🎉 Validation completed successfully for: ", file_base)
    return(TRUE)

  }, error = function(e) {
    message("\n❌ Error in validation for ", file_base, ": ", e$message)
    return(FALSE)
  })
}

# ----------------- Parallel execution for all tiles-----------------
# Set up cluster plan
plan(cluster, workers = workers)

# Run in parallel
tictoc::tic("🚀 Parallel processing of tiles")

results <- future_map(
  file_bases,
  validate_vegetation_heights,
  .progress = FALSE,
  .options = furrr_options(seed=TRUE)
)

plan(sequential)
tictoc::toc()

# Summarize results
success_count <- sum(unlist(results))
fail_count <- length(results) - success_count
message(sprintf("✅ Completed: %d succeeded, ❌ %d failed.", success_count, fail_count))

# ------ Single tile check (optional) ---------------------------------------
# validate_vegetation_heights(file_bases[1])
# validate_vegetation_heights(file_bases[11])
