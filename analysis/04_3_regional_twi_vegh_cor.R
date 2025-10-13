# ~ Worker = 8, 4.5 min for 18 sample regions

# ------Load required libraries-------------------------------------------------------------
library(terra)     # For handling raster data
library(dplyr)
library(ggmap)
library(tidyterra)
library(furrr)
library(future)

library(khroma)
library(RColorBrewer)
library(ggplot2)
library(patchwork)
library(rnaturalearth)
library(sf)


source(here::here("config.R"))
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

    # --- reg info ---
    regA_id <- paste0(regA_row$strata_A_label, "_", regA_row$sample_id)
    regA_extent <- terra::ext(regA_row$xmin, regA_row$xmax, regA_row$ymin, regA_row$ymax)
    regA_xmid <- (regA_row$xmin +regA_row$xmax) / 2
    regA_ymid <- (regA_row$ymin +regA_row$ymax) / 2

    # Start Processing
    tictoc::tic(paste0("Processing tile: ", regA_id))
    t0 <- Sys.time()

    # --- Load TWI Raster ---
    # Load TWI Raster ---
    twi_r <- terra::rast(twi_30m_path)
    twi_rc <- crop(twi_r, regA_extent, snap = "out")
    names(twi_rc) <- "twi"
    # remove large data
    rm(twi_r);gc()

    # --- Load and Preprocess Vegetation Height Raster ---
    vegh_rc <- extent_to_tile_ids(regA_extent, tile_size = 3, return_raster = TRUE,
                                  source = "lang_vegh_10m", tiles_dir = vegh_10m_tiles_dir)
    names(vegh_rc) <- "vegh"

    # Set 0 as NA value (0m canopy height represents not vegetated or water according to Lang et al. (2019))
    # Aggregates using TWI data from Ho et al. (2015)
    # Return the saved path
    vegh_rr <- raster_preprocess_save(
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

    # --- Stack Rasters for Correlation ---
    stacked <- c(twi_rc, vegh_rr)
    names(stacked) <- c("twi", "vegh")

    # ---- Spatial windows and correlation ----
    df_win <- create_spatial_windows(stacked, value_vars = c("twi", "vegh"), dwin = 0.005)
    df_cor <- calculate_correlation_bywin(df_win, x = "twi", y = "vegh")

    cor_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "correlation")], type = "xyz", crs = "EPSG:4326")
    pval_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "pval")], type = "xyz", crs = "EPSG:4326")

    # --- Save r_H_TWI and P value data as NetCDF ---
    cor_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "correlation")], type = "xyz", crs = "EPSG:4326")
    names(cor_r) <- "r_H_TWI"
    cor_nc_path <- file.path(output_dir, paste0("regA_", regA_id, "_r_H_TWI_500m.nc"))
    terra::writeCDF(cor_r, cor_nc_path, overwrite = TRUE)
    message("Saved: ", cor_nc_path)

    pval_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "pval")], type = "xyz", crs = "EPSG:4326")
    names(pval_r) <- "pval_r_H_TWI"
    pval_nc_path <- file.path(output_dir, paste0("regA_", regA_id, "_pval_r_H_TWI_500m.nc"))
    terra::writeCDF(pval_r, pval_nc_path, overwrite = TRUE)
    message("Saved: ", pval_nc_path)

    # --- Save Vegetation Height as NetCDF ---
    vegh_nc_path <- file.path(output_dir, paste0("regA_", regA_id, "_vegh_30m.nc"))
    terra::writeCDF(vegh_rr, vegh_nc_path, overwrite = TRUE)
    message("Saved: ", vegh_nc_path)

    # --- Save TWI as NetCDF ---
    twi_nc_path <- file.path(output_dir, paste0("regA_", regA_id, "_twi_30m.nc"))
    terra::writeCDF(twi_rc, twi_nc_path, overwrite = TRUE)
    message("Saved: ", twi_nc_path)

    # --- Load and Save DEM as NetCDF ---
    dem_rc <- extent_to_tile_ids(regA_extent, tile_size = 1, return_raster = TRUE,
                                  source = "copernicus_dem_30m", tiles_dir = dem_30m_copernicus_dir)
    names(dem_rc) <- "dem"
    dem_rr <- terra::resample(dem_rc, twi_rc, method="bilinear" )

    dem_nc_path <- file.path(output_dir, paste0("regA_", regA_id, "_dem_30m.nc"))
    terra::writeCDF(dem_rr, dem_nc_path, overwrite = TRUE)
    message("Saved: ", dem_nc_path)

    # --- Plotting ---
    p_dem <- plot_dem(dem_rr, extent = regA_extent, text_size = text_size, x_step = 0.5, y_step = 0.5)
    p_vegh <- plot_vegh(vegh_rr, extent = regA_extent, text_size = text_size, x_step = 0.5, y_step = 0.5)
    p_twi <- plot_twi(twi_rc, extent = regA_extent, text_size = text_size, x_step = 0.5, y_step = 0.5)
    p_r <- plot_cor_twi_vegh(cor_r, extent = regA_extent,  title_text = "500m: Pearson's r (H~TWI)", text_size = text_size, x_step = 0.5, y_step = 0.5)
    p_r2 <- plot_cor_twi_vegh(cor_twi_vegh_mosaic_file, extent = regA_extent, title_text = "5km: Pearson's r (H~TWI)", text_size = text_size, x_step = 0.5, y_step = 0.5)
    p_google <- plot_google_img(extent = regA_extent)
    p_fused <- plot_fused(fused_5km_file, extent = regA_extent, text_size = text_size, x_step = 0.5, y_step = 0.5)

    p_scatter <- plot_hex_scatter(df_win, x_var = "twi", y_var = "vegh",
                                  x_text = "Topographic Wetness Index", y_text = "Vegetation height (m)",
                                  text_size = text_size)
    p_location <- plot_single_sample_location(regA_xmid, regA_ymid,  regA_id, text_size = text_size)
    # ---- Combine plots ----
    final_plot <- ((p_dem + p_vegh + p_twi) /
                     (p_r + p_r2 + p_fused) /
                     (p_google + p_location + p_scatter)) +
      plot_annotation(title = regA_id) +
      plot_layout(heights = c(1, 1, 1))

    # ---- Save plot ----
    out_file <- here::here(file.path(paste0("data/figures/05_regA_", regA_id, "_win_500m_plots.png")))
    ggsave(filename = out_file, plot = final_plot, width = fig_width, height = fig_height, dpi = 600)


    # --- Cleanup ---
    rm(twi_rc, vegh_rc, vegh_rr, stacked, df_win, df_cor, cor_r,
       pval_r, dem_rc, dem_rr, p_dem, p_vegh, p_twi, p_r,
       p_r2, p_google, p_fused, p_scatter, p_location); gc(verbose = FALSE)

    # --- Print proccessed time ---
    elapsed_mins <- difftime(Sys.time(), t0, units = "mins")
    message(sprintf("Region %s completed [%.1f mins]", regA_id, elapsed_mins))
    tictoc::toc()

    return(TRUE)

  }, error = function(e) {
    msg <- sprintf("Region %s_%s failed: %s", regA_id, conditionMessage(e))
    message("❌ ", msg)
    return(FALSE)
  })
}


# --- Load Region Info ---
regA_info <- readRDS(here::here("data/df_samples_A.rds")) |>
  select(ends_with("label"), ends_with("min"), ends_with("max"), sample_id)

process_regA_500m(regA_info[1, ])

# for (i in seq_len(nrow(regA_info))) {
#   process_regA_500m(regA_info[i, ])
# }

