# ~ UBELIX with 8 workers: 8.5 min for 34 sample regions

# ------Load required libraries-------------------------------------------------------------

library(terra)
library(dplyr)
library(furrr)

library(ggplot2)
library(tidyterra)
library(patchwork)
library(ggmap)
library(khroma)
library(RColorBrewer)
library(rnaturalearth)
library(sf)

# ------ Load configuration file and custom functions -------------------------------------------------
# Automatically select configuration file
hostname <- trimws(tolower(system("hostname", intern = TRUE)))
if (hostname == "dash") {
  message("💻 Detected Worksation: dash → using config.R")
  source(here::here("config.R"))
  workers = 4
} else {
  message("🖥️ Detected HPC environment (", hostname, ") → using config_ubelix.R")
  source(here::here("config_ubelix.R"))
  workers = 8
}

# Load cuntom functions
source(here::here("R/mosaic_tiles.R"))
source(here::here("R/extent_to_tile_ids.R"))
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/calculate_correlation_bywin.R"))
source(here::here("R/raster_preprocess_save.R"))

source(here::here("R/plot_dem.R"))
source(here::here("R/plot_vegh.R"))
source(here::here("R/plot_twi.R"))
source(here::here("R/plot_cor_twi_vegh.R"))
source(here::here("R/plot_hex_scatter.R"))
source(here::here("R/plot_single_sample_location.R"))
source(here::here("R/plot_google_img.R"))
source(here::here("R/plot_fused.R"))
source(here::here("R/plot_cor_pval.R"))
# source(here::here("R/plot_biomes.R"))

# --------------- Main Processing Function -------------------------------------

#' Process a single region for TWI and vegetation height correlation analysis
#'
#' @param reg_row Region Information about the reg_id,sample_id, xmix, xmax, ymin, ymax
#' @param sample_id Sample identifier
#' @param output_dir Directory to save output NetCDF files
#' @param dwin Window size for spatial analysis (in degrees)
#'
#' @return Returns TRUE if successful, FALSE otherwise
process_regA_500m <- function(regA_row, output_dir = regA_cor_twi_vegh_dir,
                              text_size = 12, fig_width = 14, fig_height = 14) {

  tryCatch({

    # --- Region info ---
    regA_id <- paste0(regA_row$strata_A_label, "_", regA_row$sample_id)
    regA_extent <- terra::ext(regA_row$xmin, regA_row$xmax, regA_row$ymin, regA_row$ymax)
    regA_xmid <- (regA_row$xmin +regA_row$xmax) / 2
    regA_ymid <- (regA_row$ymin +regA_row$ymax) / 2

    # Start Processing
    tictoc::tic(paste0("Processing tile: ", regA_id))
    t0 <- Sys.time()

    # --- TWI Raster ---
    twi_rc <- terra::rast(twi_30m_path) |> terra::crop(regA_extent)
    twi_nc_path <- file.path(output_dir, paste0("tile_", regA_id, "_twi_450m.nc"))
    terra::writeCDF(twi_rc, twi_nc_path, overwrite = TRUE)
    rm(twi_rc); gc()
    twi_rc <- terra::rast(twi_nc_path)
    names(twi_rc) <- "twi"
    message("Saved: ", twi_nc_path)

    # --- Vegetation Height Raster ---
    vegh_rc <- extent_to_tile_ids(regA_extent, tile_size = 3, return_raster = TRUE,
                                  source = "lang_vegh_10m", tiles_dir = vegh_10m_tiles_dir)

    # Vegetation Height Raster: crop and store in temporary file
    tmp_vegh <- tempfile(fileext = ".nc")          # Create temporary NetCDF file
    terra::writeCDF(vegh_rc, tmp_vegh, varnames = "vegh", overwrite = TRUE)  # Write cropped raster
    rm(vegh_rc); gc()                               # Remove large in-memory object
    vegh_rc <- terra::rast(tmp_vegh)               # Reload raster from temporary file
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
    vegh_nc_path <- file.path(output_dir, paste0("tile_", regA_id, "_vegh_450m.nc"))
    terra::writeCDF(vegh_rc, vegh_nc_path, varnames="vegh", overwrite = TRUE)
    rm(vegh_rc); gc()
    vegh_rc <- terra::rast(vegh_nc_path)
    message("Saved: ", vegh_nc_path)

    # --- Stack and correlation ---
    stacked <- c(twi_rc, vegh_rc)
    df_win <- create_spatial_windows(stacked, value_vars = c("twi", "vegh"), dwin = 0.005)
    df_cor <- calculate_correlation_bywin(df_win, x = "twi", y = "vegh")

    # --- Save correlation ---
    cor_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "correlation")], type="xyz", crs="EPSG:4326")
    names(cor_r) <- "r_H_TWI"
    cor_nc_path <- file.path(output_dir, paste0("tile_", regA_id, "_r_H_TWI_500m_map.nc"))
    terra::writeCDF(cor_r, cor_nc_path, overwrite = TRUE)
    message("Saved: ", cor_nc_path)

    # --- Save p-value ---
    pval_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "cor_pval")], type="xyz", crs="EPSG:4326")
    names(pval_r) <- "pval_r_H_TWI"
    pval_nc_path <- file.path(output_dir, paste0("tile_", regA_id, "_r_H_TWI_500m_pval.nc"))
    terra::writeCDF(pval_r, pval_nc_path, overwrite = TRUE)
    message("Saved: ", pval_nc_path)

    # --- Elevation Raster ---
    dem_rc <- extent_to_tile_ids(regA_extent, tile_size = 1, return_raster = TRUE,
                                 source = "copernicus_dem_30m", tiles_dir = dem_30m_copernicus_dir)
    dem_rr <- terra::resample(dem_rc, twi_rc, method = "bilinear")
    dem_nc_path <- file.path(output_dir, paste0("regA_", regA_id, "_dem_30m.nc"))
    terra::writeCDF(dem_rr, dem_nc_path, varnames = "dem", overwrite = TRUE)
    rm(dem_rc, dem_rr); gc()
    dem_rc <- terra::rast(dem_nc_path)
    names(dem_rc) <- "dem"
    message("Saved: ", dem_nc_path)

    # --- Plotting ---
    p_dem <- plot_dem(dem_rc, extent = regA_extent, text_size = text_size, x_step = 0.5, y_step = 0.5)  + ggplot2::theme(aspect.ratio = 1)
    p_vegh <- plot_vegh(vegh_rc, extent = regA_extent, text_size = text_size, x_step = 0.5, y_step = 0.5)  + ggplot2::theme(aspect.ratio = 1)
    p_twi <- plot_twi(twi_rc, extent = regA_extent, text_size = text_size, x_step = 0.5, y_step = 0.5)  + ggplot2::theme(aspect.ratio = 1)
    p_r <- plot_cor_twi_vegh(cor_r, extent = regA_extent,  title_text = "500m: Pearson's r (H~TWI)", text_size = text_size, x_step = 0.5, y_step = 0.5)  + ggplot2::theme(aspect.ratio = 1)
    p_r2 <- plot_cor_twi_vegh(cor_twi_vegh_mosaic_file, extent = regA_extent, title_text = "5km: Pearson's r (H~TWI)", text_size = text_size, x_step = 0.5, y_step = 0.5)  + ggplot2::theme(aspect.ratio = 1)
    p_google <- plot_google_img(extent = regA_extent) + ggplot2::theme(aspect.ratio = 1)
    p_fused <- plot_fused(fused_5km_file, extent = regA_extent, text_size = text_size, x_step = 0.5, y_step = 0.5)  + ggplot2::theme(aspect.ratio = 1)

    p_scatter <- plot_hex_scatter(df_win, x_var = "twi", y_var = "vegh",
                                  x_text = "Topographic Wetness Index", y_text = "Vegetation height (m)",
                                  text_size = text_size)  + ggplot2::theme(aspect.ratio = 1)
    source(here::here("R/plot_single_sample_location.R"))
    p_location <- plot_single_sample_location(regA_xmid, regA_ymid,  regA_id, text_size = text_size)  + ggplot2::theme(aspect.ratio = 1)

    # ---- Combine plots ----
    final_plot <- ((p_dem + p_twi + p_vegh) /
                     (p_r + p_r2 + p_fused) /
                     (p_google + p_location + p_scatter)) +
      plot_annotation(title = regA_id) +
      plot_layout(heights = c(1, 1, 1))

    # ---- Save plot ----
    out_file <- here::here(file.path(paste0("data/figures/04_regA_", regA_id, "_win_500m_plots.png")))
    ggsave(filename = out_file, plot = final_plot, width = fig_width, height = fig_height, dpi = 600)


    # --- Cleanup ---
    rm(twi_rc, vegh_rc, stacked, df_win, df_cor, cor_r,
       pval_r, dem_rc, dem_rr, p_dem, p_vegh, p_twi, p_r,
       p_r2, p_google, p_fused, p_scatter, p_location); gc(verbose = FALSE)

    # --- Print proccessed time ---
    elapsed_mins <- difftime(Sys.time(), t0, units = "mins")
    message(sprintf("Region %s completed [%.1f mins]", regA_id, elapsed_mins))
    tictoc::toc()

    return(TRUE)

  }, error = function(e) {
    regA_id <- paste0(regA_row$strata_A_label, "_", regA_row$sample_id)
    elapsed_mins <- difftime(Sys.time(), t0, units = "mins")
    message(sprintf("❌ Tile %s failed after %.1f mins: %s", regA_id, elapsed_mins, e$message))
    return(FALSE)
  })
}

# ----------------- Parallel execution -----------------
# load sample regions info
regA_info <- readRDS(regA_sample_info_path) |>
  select(ends_with("label"), ends_with("min"), ends_with("max"), sample_id)

# Set up cluster plan
plan(cluster, workers = workers)

# Run in parallel
tictoc::tic("🚀 Parallel processing of tiles")
results <- future_map(seq_len(nrow(regA_info)),
                      function(i) process_regA_500m(regA_info[i, ]),
                      .progress = FALSE,
                      .options = furrr_options(seed=TRUE))
plan(sequential)
tictoc::toc()

# Summarize results
success_count <- sum(unlist(results))
fail_count <- length(results) - success_count
message(sprintf("✅ Completed: %d succeeded, ❌ %d failed.", success_count, fail_count))

# # ----------- Test on single regions -----------------------------
#
# # load sample regions info
# regA_info <- readRDS(here::here("data/df_samples_A.rds")) |>
#   select(ends_with("label"), ends_with("min"), ends_with("max"), sample_id)
#
# process_regA_500m(regA_info[1, ])


# # # ----------- Test on smaller regions -----------------------------
# regA_info <- data.frame(
#   strata_A_label = c("Aletsch_glacier"),
#   ymin = c(46.9),
#   ymax = c(47),
#   xmin = c(7.9),
#   xmax = c(8),
#   sample_id = c(1)
# )
#
# # center location
# regA_info$xmid <- (regA_info$xmax + regA_info$xmin) / 2
# regA_info$ymid <- (regA_info$ymax + regA_info$ymin) / 2
#
# # process_regA_500m(regA_info[1, ])
#
# regA_row <- regA_info[1, ]
# output_dir = regA_cor_twi_vegh_dir
# text_size = 12
# fig_width = 14
# fig_height = 14
# dwin = 0.005



