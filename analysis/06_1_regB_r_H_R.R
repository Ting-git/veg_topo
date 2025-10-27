
# ~98 min : UBELIX, 50 cores
# ------Load required libraries-------------------------------------------------

library(terra)     # For handling raster data
library(furrr)
library(dplyr)
library(ggmap)
library(tidyterra)
library(doParallel)    # inner parallelism
library(foreach)

library(khroma)
library(RColorBrewer)
library(ggplot2)
library(patchwork)
library(rnaturalearth)
library(sf)

# ------Load configuration and helper functions---------------------------------

# Automatically select configuration file
hostname <- trimws(tolower(system("hostname", intern = TRUE)))
if (hostname == "dash") {
  message("💻 Detected Worksation: dash → using config.R")
  source(here::here("config.R"))
  workers = 2
} else {
  message("🖥️ Detected HPC environment (", hostname, ") → using config_ubelix.R")
  source(here::here("config_ubelix.R"))
  workers = 5
}

source(here::here("R/create_spatial_windows.R"))
source(here::here("R/calculate_correlation_bywin.R"))
source(here::here("R/mosaic_tiles.R"))
source(here::here("R/raster_preprocess_save.R"))
source(here::here("R/aggregate_topography.R"))
source(here::here("R/extent_to_tile_ids.R"))

source(here::here("R/helpers.R")) # SPLASH
source(here::here("R/calc_sw_in.R")) # SPLASH

source(here::here("R/plot_dem.R"))
source(here::here("R/plot_aspect.R"))
source(here::here("R/plot_slope.R"))
source(here::here("R/plot_vegh.R"))
source(here::here("R/plot_r_H_R.R"))
source(here::here("R/plot_sw_in.R"))
source(here::here("R/plot_rin.R"))
source(here::here("R/plot_hex_scatter.R"))
source(here::here("R/plot_single_sample_location.R"))
source(here::here("R/plot_google_img.R"))
source(here::here("R/plot_fused.R"))

# ------ File Configuration ---------------------------------------------

if (!dir.exists(regB_r_R_H_dir)) {
  dir.create(regB_r_R_H_dir, recursive = TRUE)
  message("Directory created: ", regB_r_R_H_dir)
}

# --------------- Main Processing Function -------------------------------------

process_regB_500m <- function(regB_row,
                              output_dir = regB_r_R_H_dir,
                              text_size = 12,
                              fig_width = 14,
                              fig_height = 18) {
  tryCatch({

    # --- reg info ---

    regB_id <- paste0(regB_row$strata_B_label, "_", regB_row$sample_id)
    regB_extent <- terra::ext(regB_row$xmin, regB_row$xmax, regB_row$ymin, regB_row$ymax)
    regB_xmid <- (regB_row$xmin +regB_row$xmax) / 2
    regB_ymid <- (regB_row$ymin +regB_row$ymax) / 2

    # Start Processing
    tictoc::tic(paste0("Processing tile: ", regB_id))
    t0 <- Sys.time()

    # --- TWI Raster ---
    twi_rc <- terra::rast(twi_30m_path) |> terra::crop(regB_extent)
    twi_nc_path <- file.path(output_dir, paste0("regB_tile_", regB_id, "_twi_450m.nc"))
    terra::writeCDF(twi_rc, twi_nc_path, overwrite = TRUE)
    rm(twi_rc); gc()
    twi_rc <- terra::rast(twi_nc_path)
    names(twi_rc) <- "twi"
    message("Saved: ", twi_nc_path)

    # --- Vegetation Height Raster ---
    vegh_rc <- extent_to_tile_ids(regB_extent, tile_size = 3, return_raster = TRUE,
                                  source = "lang_vegh_10m", tiles_dir = vegh_10m_tiles_dir)

    # Vegetation Height Raster: crop and store in temporary file
    tmp_vegh <- tempfile(fileext = ".nc")
    terra::writeCDF(vegh_rc, tmp_vegh, varnames = "vegh", overwrite = TRUE)
    rm(vegh_rc); gc()
    vegh_rc <- terra::rast(tmp_vegh)
    names(vegh_rc) <- "vegh"

    # Set 0 as NA value (0m canopy height represents not vegetated or water according to Lang et al. (2019))
    # Aggregate and resample using TWI data from Ho et al. (2025)
    vegh_rc <- raster_preprocess_save(
      input = vegh_rc,
      target = twi_rc,
      na_value = 0,
      fun = mean,
      varname = "vegh",
      if_aggregate = TRUE,
      if_resample = TRUE,
      if_mask = TRUE,
      if_return_raster = TRUE
    )
    vegh_nc_path <- file.path(output_dir, paste0("regB_tile_", regB_id, "_vegh_450m.nc"))
    terra::writeCDF(vegh_rc, vegh_nc_path, varnames="vegh", overwrite = TRUE)
    rm(vegh_rc); gc()
    vegh_rc <- terra::rast(vegh_nc_path)
    message("Saved: ", vegh_nc_path)

    # --- Elevation and radiation Raster ---
    dem_rc <- extent_to_tile_ids(regB_extent, tile_size = 1, return_raster = TRUE,
                                 source = "copernicus_dem_30m", tiles_dir = dem_30m_copernicus_dir)

    # Aggregate DEM and calculate slope/aspect
    aligned <- aggregate_topography(
      dem_rc,
      res_tar = NULL,
      target = twi_rc,
      if_resample = TRUE
    )

    # --- Data frame Prepare ---

    # Extract + join
    df <- as.data.frame(aligned[["dem"]], xy = TRUE) |>
      left_join(as.data.frame(aligned[["slope"]], xy = TRUE), by = c("x", "y")) |>
      left_join(as.data.frame(aligned[["aspect"]], xy = TRUE), by = c("x", "y")) |>
      tibble::as_tibble() |>
      drop_na()
    names(df) <- c("lon", "lat", "dem", "slope", "aspect")

    if (nrow(df) == 0) {
      warning(sprintf("No valid cells after drop_na for %s", file))
      return(FALSE)
    }

    # --- Inner Paralell ---

    # clean old cluster
    try({
      if (exists("cl")) stopCluster(cl)
      closeAllConnections()
    }, silent = TRUE)

    # Inner parallelism
    num_cores <- 49
    cl <- makeCluster(num_cores)
    registerDoParallel(cl)

    # Chunk Processing
    chunk_size <- 5000  # rows per chunk, adjust based on memory
    chunks <- split(df, ceiling(seq_len(nrow(df)) / chunk_size))

    # Parallel Calculation for Each Chunk - Direct assignment
    df_calc <- foreach(
      chunk = chunks,
      .combine = bind_rows,
      .packages = c("dplyr"),
      .export   = c(
        "calc_sw_in_daily",
        "calc_sw_in",
        "julian_day",
        "berger_tls",
        "dcos",
        "dsin"
      )
    ) %dopar% {
      # Calculate sw_in_uneven and sw_in_flat for entire chunk
      sw_in_uneven <- calc_sw_in(chunk$lat, chunk$slope, chunk$aspect, year = 2020)
      sw_in_flat <- calc_sw_in(chunk$lat, rep(0, nrow(chunk)), rep(0, nrow(chunk)), year = 2020)

      # Combine results back to dataframe
      chunk |>
        mutate(
          sw_in_uneven = sw_in_uneven,
          sw_in_flat = sw_in_flat,
          rin = sw_in_uneven / sw_in_flat
        )

    }

    # Stop cluster
    stopCluster(cl)
    registerDoSEQ()

    # --- Build rasters ---
    crs_out <- terra::crs(aligned[["dem"]])
    sw_in_uneven <- terra::rast(df_calc[, c("lon", "lat", "sw_in_uneven")], type = "xyz", crs = crs_out)
    sw_in_flat <- terra::rast(df_calc[, c("lon", "lat", "sw_in_flat")], type = "xyz", crs = crs_out)
    rin <- terra::rast(df_calc[, c("lon", "lat", "rin")], type = "xyz", crs = crs_out)
    rin <- terra::resample(rin, twi_rc, method = "near")

    # --- Stack and Correalation---
    stacked <- c(rin, vegh_rc)
    names(stacked) <- c("rin", "vegh")
    df_win <- create_spatial_windows(stacked, value_vars = c("rin", "vegh"), dwin = 0.005)
    df_cor <- calculate_correlation_bywin(df_win, x = "rin", y = "vegh")

    # --- Save correlation ---
    cor_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "correlation")], type = "xyz", crs = "EPSG:4326")
    names(cor_r) <- "r_H_R"
    cor_nc_path <- file.path(output_dir, paste0("regB_", regB_id, "_r_H_R_500m.nc"))
    terra::writeCDF(cor_r, cor_nc_path, overwrite = TRUE)
    message("Saved: ", cor_nc_path)

    # --- Save p-value ---
    pval_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "cor_pval")], type = "xyz", crs = "EPSG:4326")
    names(pval_r) <- "pval_r_H_R"
    pval_nc_path <- file.path(output_dir, paste0("regB_", regB_id, "_pval_r_H_R_500m.nc"))
    terra::writeCDF(pval_r, pval_nc_path, overwrite = TRUE)
    message("Saved: ", pval_nc_path)

    # --- Save Rin as NetCDF ---
    rin_nc_path <- file.path(output_dir, paste0("regB_", regB_id, "_rin_30m.nc"))
    terra::writeCDF(rin, rin_nc_path, overwrite = TRUE)
    message("Saved: ", rin_nc_path)

    # --- Save DEM as NetCDF ---
    dem_nc_path <- file.path(output_dir, paste0("regB_", regB_id, "_dem_30m.nc"))
    terra::writeCDF(aligned[["dem"]], dem_nc_path, overwrite = TRUE)
    message("Saved: ", dem_nc_path)

    # --- Save Slope as NetCDF ---
    slope_nc_path <- file.path(output_dir, paste0("regB_", regB_id, "_slope_30m.nc"))
    terra::writeCDF(aligned[["slope"]], slope_nc_path, overwrite = TRUE)
    message("Saved: ", slope_nc_path)

    # --- Save Aspect as NetCDF ---
    aspect_nc_path <- file.path(output_dir, paste0("regB_", regB_id, "_aspect_30m.nc"))
    terra::writeCDF(aligned[["aspect"]], aspect_nc_path, overwrite = TRUE)
    message("Saved: ",  aspect_nc_path)


    # ---- Generate plots ----
    p_dem <- plot_dem(aligned[["dem"]], extent = regB_extent, text_size = text_size, x_step = 0.5, y_step = 0.5) + ggplot2::theme(aspect.ratio = 1)
    p_slope <- plot_slope(aligned[["slope"]], extent = regB_extent, text_size = text_size, x_step = 0.5, y_step = 0.5) + ggplot2::theme(aspect.ratio = 1)
    p_aspect <- plot_aspect(aligned[["aspect"]], extent = regB_extent, text_size = text_size, x_step = 0.5, y_step = 0.5) + ggplot2::theme(aspect.ratio = 1)
    p_vegh <- plot_vegh(vegh_rc, extent = regB_extent, text_size = text_size, x_step = 0.5, y_step = 0.5) + ggplot2::theme(aspect.ratio = 1)
    p_rin <- plot_rin(rin, extent = regB_extent, text_size = text_size, x_step = 0.5, y_step = 0.5) + ggplot2::theme(aspect.ratio = 1)
    p_r_H_R <- plot_r_H_R(cor_r, extent = regB_extent, title_text = "500m: Pearson's r (H ~ R)", text_size = text_size, x_step = 0.5, y_step = 0.5) + ggplot2::theme(aspect.ratio = 1)
    p_r_H_R2 <- plot_r_H_R(r_H_R_5km_path, extent = regB_extent, title_text = "5km: Pearson's r (H ~ R)", text_size = text_size, x_step = 0.5, y_step = 0.5) + ggplot2::theme(aspect.ratio = 1)
    p_google <- plot_google_img(extent = regB_extent) + ggplot2::theme(aspect.ratio = 1)
    p_fused <- plot_fused(fused_5km_file, extent = regB_extent, text_size = text_size, x_step = 0.5, y_step = 0.5) + ggplot2::theme(aspect.ratio = 1)
    p_scatter <- plot_hex_scatter(df_win, x_var = "rin", y_var = "vegh", x_text = "Radiation index", y_text = "Vegetation height (m)", text_size = text_size) + ggplot2::theme(aspect.ratio = 1)
    p_location <- plot_single_sample_location(regB_xmid, regB_ymid, regB_id, text_size = text_size) + ggplot2::theme(aspect.ratio = 1)
    # ---- Combine plots ----
    final_plot <- ((p_dem + p_slope + p_aspect) /
                     (p_r_H_R + p_vegh + p_rin) /
                     (p_r_H_R2 + p_location + p_scatter) /
                     (p_google + p_fused)) +
      plot_annotation(title = regB_id) +
      plot_layout(heights = c(1, 1, 1, 1))

    # ---- Save plot ----
    out_file <- here::here(file.path(paste0("data/figures/06_regB_", regB_id, "_win_500m_plots.png")))
    ggsave(filename = out_file, plot = final_plot, width = fig_width, height = fig_height, dpi = 600)

    # ---- Memory cleanup ----
    # list all objects need to be remove
    rm(twi_rc, vegh_rc, dem_rc, aligned,
       df, chunks, df_calc, stacked, df_win, df_cor, cor_r,pval_r,
       p_dem, p_slope, p_aspect, p_vegh, p_rin, p_r_H_R,
       p_r_H_R2, p_google, p_fused, p_scatter, p_location,
       final_plot)
    gc(verbose = FALSE)

    # --- Print proccessed time ---
    elapsed_mins <- difftime(Sys.time(), t0, units = "mins")
    message(sprintf("Region %s completed [%.1f mins]", regB_id, elapsed_mins))
    tictoc::toc()

    return(TRUE)

  }, error = function(e) {
    regB_id <- paste0(regB_row$strata_B_label, "_", regB_row$sample_id)
    elapsed_mins <- difftime(Sys.time(), t0, units = "mins")
    message(sprintf("❌ Tile %s failed after %.1f mins: %s", regB_id, elapsed_mins, e$message))
    return(FALSE)
  })
}

# ----------------- Process -----------------

# load sample regions info
regB_info <- readRDS(regB_sample_info_path) |>
  select(ends_with("label"), ends_with("min"), ends_with("max"), sample_id)

for (i in seq_len(nrow(regB_info))) {
  process_regB_500m(regB_info[i, ])
}

# # ----------------- Parallel execution -----------------
# # load sample regions info
# regB_info <- readRDS(here::here("data/df_samples_B.rds")) |>
#   select(ends_with("label"), ends_with("min"), ends_with("max"), sample_id)
#
# # Set up cluster plan
# plan(cluster, workers = workers)
#
# # Run in parallel
# tictoc::tic("🚀 Parallel processing of tiles")
# results <- future_map(seq_len(nrow(regB_info)),
#                       function(i) process_regB_500m(regB_info[i, ]),
#                       .progress = FALSE,
#                       .options = furrr_options(seed=TRUE))
# plan(sequential)
# tictoc::toc()
#
# # Summarize results
# success_count <- sum(unlist(results))
# fail_count <- length(results) - success_count
# message(sprintf("✅ Completed: %d succeeded, ❌ %d failed.", success_count, fail_count))

# # ----------- Test on single regions -----------------------------
#
# # load sample regions info
# regB_info <- readRDS(here::here("data/df_samples_B.rds")) |>
#   select(ends_with("label"), ends_with("min"), ends_with("max"), sample_id)
#
# process_regB_500m(regB_info[1, ])


# # ----------- Test on smaller regions -----------------------------
# regB_info <- data.frame(
#   strata_B_label = c("Aletsch_glacier"),
#   ymin = c(46.9),
#   ymax = c(47),
#   xmin = c(7.9),
#   xmax = c(8),
#   sample_id = c(1)
# )
#
# regB_row <- regB_info[1, ]
# output_dir = regB_r_R_H_dir
# text_size = 12
# fig_width = 14
# fig_height = 20


