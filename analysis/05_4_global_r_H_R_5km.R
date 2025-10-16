# ~84 min: UBELIX, 20 cores, 500G

# ------Load required libraries-------------------------------------------------

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
  workers = 1 # don't do it on worksation
} else {
  message("🖥️ Detected HPC environment (", hostname, ") → using config_ubelix.R")
  source(here::here("config_ubelix.R"))
  workers = 17
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
source(here::here("R/plot_cor_twi_vegh.R"))
source(here::here("R/plot_cor_pval.R"))
source(here::here("R/plot_sw_in.R"))
source(here::here("R/plot_rin.R"))
source(here::here("R/plot_hex_scatter.R"))
source(here::here("R/plot_single_sample_location.R"))
source(here::here("R/plot_google_img.R"))
source(here::here("R/plot_fused.R"))

# ------ File Configuration ---------------------------------------------

if (!dir.exists(r_H_R_tiles_dir)) {
  dir.create(r_H_R_tiles_dir, recursive = TRUE)
  message("Directory created: ", r_H_R_tiles_dir)
}

# ------------ function to process single tile----------------------------------
process_r_H_R_5km <- function(tile_row, output_dir = r_H_R_tiles_dir,
                                text_size = 12, fig_width = 14, fig_height = 18) {

  tryCatch({


    # --- Tile info ---
    tile_id <- tile_row$tile_id
    tile_extent <- terra::ext(tile_row$xmin, tile_row$xmax, tile_row$ymin, tile_row$ymax)
    tile_xmid <- (tile_row$xmin + tile_row$xmax)/2
    tile_ymid <- (tile_row$ymin + tile_row$ymax)/2

    tictoc::tic(paste0("Processing tile: ", tile_id))
    t0 <- Sys.time()

    # --- TWI Raster ---
    twi_rc <- terra::rast(twi_450m_mosaic_clean_path) |> terra::crop(tile_extent)
    twi_tmp_path <- file.path(tempdir(), paste0("tile_", tile_id, "_twi_450m.nc"))
    terra::writeCDF(twi_rc, twi_tmp_path, varnames = "twi", overwrite = TRUE)
    rm(twi_rc); gc()
    twi_rc <- terra::rast(twi_tmp_path)
    names(twi_rc) <- "twi"
    message("Saved temporary TWI raster: ", twi_tmp_path)


    # --- Vegetation Height Raster ---
    vegh_rc <- terra::rast(vegh_450m_mosaic_path) |> terra::crop(tile_extent)
    vegh_tmp_path <- file.path(tempdir(), paste0("tile_", tile_id, "_vegh_450m.nc"))
    terra::writeCDF(vegh_rc, vegh_tmp_path, varnames = "vegh",overwrite = TRUE)
    rm(vegh_rc); gc()
    vegh_rc <- terra::rast(vegh_tmp_path)
    names(vegh_rc) <- "vegh"
    message("Saved temporary vegetation height raster: ", vegh_tmp_path)

    # --- Radiation index of terrain effect ---
    rin_rc <- terra::rast(sw_in_terrain_effect_450m_path) |> terra::crop(tile_extent)
    terrain_tmp_path <- file.path(tempdir(), paste0("tile_", tile_id, "_terrain_effect_450m.nc"))
    terra::writeCDF(rin_rc, terrain_tmp_path, varnames = "rin", overwrite = TRUE)
    rm(rin_rc); gc()
    rin_rc <- terra::rast(terrain_tmp_path)
    names(rin_rc) <- "rin"
    message("Saved temporary radiation index raster: ", terrain_tmp_path)

    # # --- Elevation and radiation Raster ---
    # dem_rc <- extent_to_tile_ids(tile_extent, tile_size = 1, return_raster = TRUE,
    #                              source = "copernicus_dem_30m", tiles_dir = dem_30m_copernicus_dir)
    #
    # # Aggregate DEM and calculate slope/aspect
    # aligned <- aggregate_topography(
    #   dem_rc,
    #   res_tar = NULL,
    #   target = twi_rc,
    #   if_resample = TRUE
    # )
    # rm(dem_rc); gc()

    # --- Stack and correlation ---
    stacked <- c(rin_rc, vegh_rc)
    df_win <- create_spatial_windows(stacked, value_vars = c("rin", "vegh"), dwin = 0.05)
    df_cor <- calculate_correlation_bywin(df_win, x = "rin", y = "vegh")

    # --- Save correlation ---
    cor_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "correlation")], type="xyz", crs="EPSG:4326")
    names(cor_r) <- "r_H_R"
    cor_nc_path <- file.path(output_dir, paste0("tile_", tile_id, "_r_H_R_5km_map.nc"))
    terra::writeCDF(cor_r, cor_nc_path, overwrite = TRUE)
    message("Saved: ", cor_nc_path)

    # --- Save p-value ---
    pval_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "cor_pval")], type="xyz", crs="EPSG:4326")
    names(pval_r) <- "pval_r_H_R"
    pval_nc_path <- file.path(output_dir, paste0("tile_", tile_id, "_r_H_R_5km_pval.nc"))
    terra::writeCDF(pval_r, pval_nc_path, overwrite = TRUE)
    message("Saved: ", pval_nc_path)

    # ---- Generate plots ----
    p_dem <- plot_dem(dem_450m_mosaic_path, extent = tile_extent, text_size = text_size) + ggplot2::theme(aspect.ratio = 1)
    p_slope <- plot_slope(slope_450m_mosaic_path, extent = tile_extent, text_size = text_size) + ggplot2::theme(aspect.ratio = 1)
    p_aspect <- plot_aspect(aspect_450m_mosaic_path, extent = tile_extent, text_size = text_size) + ggplot2::theme(aspect.ratio = 1)
    p_vegh <- plot_vegh(vegh_rc, extent = tile_extent, text_size = text_size)  + ggplot2::theme(aspect.ratio = 1)
    p_rin <- plot_rin(rin_rc, extent = tile_extent, text_size = text_size)  + ggplot2::theme(aspect.ratio = 1)
    p_r_H_R <- plot_r_H_R(cor_r, extent = tile_extent, title_text = "5km: Pearson's r (H~R)", text_size = text_size)  + ggplot2::theme(aspect.ratio = 1)
    p_p <- plot_cor_pval(pval_r, extent = tile_extent,  title_text = "5km: Pearson's p value (H~R)", text_size = text_size) + ggplot2::theme(aspect.ratio = 1)
    p_r_H_TWI <- plot_cor_twi_vegh(cor_twi_vegh_mosaic_file, extent = tile_extent, title_text = "5km: Pearson's r (H~TWI)", text_size = text_size)  + ggplot2::theme(aspect.ratio = 1)
    p_google <- plot_google_img(extent = tile_extent) + ggplot2::theme(aspect.ratio = 1)
    p_fused <- plot_fused(fused_5km_file, extent = tile_extent, text_size = text_size) + ggplot2::theme(aspect.ratio = 1)
    p_scatter <- plot_hex_scatter(df_win, x_var = "rin", y_var = "vegh", x_text = "Radiation index", y_text = "Vegetation height (m)", text_size = text_size) + ggplot2::theme(aspect.ratio = 1)
    p_location <- plot_single_sample_location(tile_xmid, tile_ymid, tile_id, text_size = text_size) + ggplot2::theme(aspect.ratio = 1)
    # ---- Combine plots ----
    final_plot <- (
      (p_dem + p_slope + p_aspect) /
        (p_rin + p_vegh + p_fused) /
        (p_r_H_R + p_p + p_r_H_TWI )  /
        (p_google + p_location + p_scatter)) +
      plot_annotation(title = tile_id) +
      plot_layout(heights = c( 1, 1, 1, 1))

    # ---- Save plot ----
    out_file <- here::here(file.path(paste0("data/figures/05_tile_", tile_id, "_win_5km_plots.png")))
    ggsave(filename = out_file, plot = final_plot, width = fig_width, height = fig_height, dpi = 600)

    # ---- Memory cleanup ----
    # list all objects need to be remove
    rm(
      p_dem, p_slope, p_aspect, p_p,
      twi_rc, vegh_rc, rin_rc,
      stacked, df_win, df_cor, cor_r,pval_r,
      p_vegh, p_rin, p_r_H_R,
      p_r_H_TWI, p_google, p_fused, p_scatter, p_location,
      final_plot)
    gc(verbose = FALSE)

    # --- Print proccessed time ---
    elapsed_mins <- difftime(Sys.time(), t0, units = "mins")
    message(sprintf("Region %s completed [%.1f mins]", tile_id, elapsed_mins))
    tictoc::toc()

    return(TRUE)


  }, error = function(e) {
    elapsed_mins <- difftime(Sys.time(), t0, units = "mins")
    message(sprintf("❌ Tile %s failed after %.1f mins: %s", tile_row$tile_id, elapsed_mins, e$message))
    return(FALSE)
  })
}


# ----------------- Parallel execution -----------------
# Set up cluster plan
plan(cluster, workers = workers)

# load tiles info
tiles_info <- readRDS(valid_tiles_info_path)

# Run in parallel
tictoc::tic("🚀 Parallel processing of tiles")
results <- future_map(seq_len(nrow(tiles_info)),
                      function(i) process_r_H_R_5km(tiles_info[i, ]),
                      .progress = FALSE,
                      .options = furrr_options(seed=TRUE))
plan(sequential)
tictoc::toc()

# Summarize results
success_count <- sum(unlist(results))
fail_count <- length(results) - success_count
message(sprintf("✅ Completed: %d succeeded, ❌ %d failed.", success_count, fail_count))

# -------- Combination ---------------------------------------------------------

# mosacing the r(H~TWI) map
mosaic_tiles(
  input_dir   = r_H_R_tiles_dir,
  output_file = r_H_R_5km_path,
  pattern = "*_map.nc",
  varname = "r_H_R")

# mosacing the pval ofr(H~TWI) map
mosaic_tiles(
  input_dir   = r_H_R_tiles_dir,
  output_file = pval_r_H_R_5km_path,
  pattern = "*_pval.nc",
  varname = "pval_r_H_R")

# # ---------- Delete intermediate data ------------------------------------------
# # List all files in the directory r_H_R_tiles_dir that match "*.nc"
# # If there are any files found, delete them
#
# cor_5km_tiles_path <- fs::dir_ls(path = r_H_R_tiles_dir, glob = "*.nc")
# if (length(cor_5km_tiles_path) > 0) file.remove(cor_5km_tiles_path)

# ------ Single tile test ---------------------------------------------

# test for 1 tile
# tiles_info <- readRDS(valid_tiles_info_path)
# process_r_H_R_5km(tiles_info[35,])

# # ------ Smaller region  test ---------------------------------------------
#
# regA_info <- data.frame(
#   tile_id = c("Aletsch_glacier"),
#   ymin = c(46.8),
#   ymax = c(47),
#   xmin = c(7.8),
#   xmax = c(8),
#   sample_id = c(1)
# )
#
# # center location
# regA_info$xmid <- (regA_info$xmax + regA_info$xmin) / 2
# regA_info$ymid <- (regA_info$ymax + regA_info$ymin) / 2
#
# tile_row <- regA_info[1, ]
#
# output_dir = r_H_R_tiles_dir
# text_size = 12
# fig_width = 14
# fig_height = 18


