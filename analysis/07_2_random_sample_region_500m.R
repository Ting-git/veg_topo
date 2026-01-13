# ~ UBELIX with 8 workers: 8.5 min for 34 sample regions
# ------ Load required libraries -------------------------------------------------------------
library(terra)          # For raster data
library(dplyr)
library(furrr)
library(doParallel)     # For parallelization
library(foreach)

library(ggplot2)
library(tidyterra)
library(patchwork)
library(ggmap)
library(khroma)
library(RColorBrewer)
library(rnaturalearth)
library(sf)

# ------ Load configuration file and custom functions -----------------------------------------

hostname <- trimws(tolower(system("hostname", intern = TRUE)))

if (hostname == "dash") {
  message("💻 Detected Workstation: dash → using config.R")
  source(here::here("config.R"))
  workers <- 16
} else {
  message("🖥️ Detected HPC environment (", hostname, ") → using config_ubelix.R")
  source(here::here("config_ubelix.R"))
  workers <- 50
}

# ------ Load helper and custom functions -----------------------------------------------------
source(here::here("R/mosaic_tiles.R"))
source(here::here("R/extent_to_tile_ids.R"))
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/calculate_correlation_bywin.R"))
source(here::here("R/raster_preprocess_save.R"))
source(here::here("R/aggregate_topography.R"))

source(here::here("R/helpers.R"))        # SPLASH
source(here::here("R/calc_sw_in.R"))     # SPLASH

# ------ Load plotting functions --------------------------------------------------------------
source(here::here("R/plot_dem.R"))
source(here::here("R/plot_vegh.R"))
source(here::here("R/plot_twi.R"))
source(here::here("R/plot_cor_twi_vegh.R"))
source(here::here("R/plot_hex_scatter.R"))
source(here::here("R/plot_single_sample_location.R"))
source(here::here("R/plot_google_img.R"))
source(here::here("R/plot_cor_pval.R"))
source(here::here("R/plot_kg_class.R"))
source(here::here("R/plot_r_H_R.R"))
source(here::here("R/plot_rin.R"))
source(here::here("R/plot_fused.R"))

# Optional (uncomment if needed)

# source(here::here("R/plot_aspect.R"))
# source(here::here("R/plot_slope.R"))
# source(here::here("R/plot_sw_in.R"))


# ------ File Configuration ---------------------------------------------

if (!dir.exists(reg_correlation_dir)) {
  dir.create(reg_correlation_dir, recursive = TRUE)
  message("Directory created: ", reg_correlation_dir)
}

# --------------- Main Processing Function -------------------------------------

#' Process a single region for TWI and vegetation height correlation analysis
#'
#' @param reg_row Region Information about the reg_id,sample_id, xmix, xmax, ymin, ymax
#' @param output_dir Directory to save output NetCDF files
#'
#' @return Returns TRUE if successful, FALSE otherwise
process_reg_500m <- function(reg_row, output_dir = reg_correlation_dir,
                             text_size = 12, x_step = 0.5, y_step = 0.5) {

  tryCatch({

    # --- Region info ---
    reg_id <- paste0(reg_row$strata_label, "_", reg_row$sample_id)
    reg_extent <- terra::ext(reg_row$xmin, reg_row$xmax, reg_row$ymin, reg_row$ymax)
    reg_xmid <- (reg_row$xmin +reg_row$xmax) / 2
    reg_ymid <- (reg_row$ymin +reg_row$ymax) / 2

    # Start Processing
    tictoc::tic(paste0("Processing tile: ", reg_id))
    t0 <- Sys.time()

    # --- TWI Raster ---
    twi_rc <- terra::rast(twi_30m_path) |> terra::crop(reg_extent)
    twi_nc_path <- file.path(output_dir, paste0("tile_", reg_id, "_twi_30m.nc"))
    terra::writeCDF(twi_rc, twi_nc_path, overwrite = TRUE)
    rm(twi_rc); gc()
    twi_rc <- terra::rast(twi_nc_path)
    names(twi_rc) <- "twi"
    message("Saved: ", twi_nc_path)

    # --- Vegetation Height Raster ---
    vegh_rc <- extent_to_tile_ids(reg_extent, tile_size = 3, return_raster = TRUE,
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
    vegh_nc_path <- file.path(output_dir, paste0("tile_", reg_id, "_vegh_30m.nc"))
    terra::writeCDF(vegh_rc, vegh_nc_path, varnames="vegh", overwrite = TRUE)
    rm(vegh_rc); gc()
    vegh_rc <- terra::rast(vegh_nc_path)
    message("Saved: ", vegh_nc_path)

    # --- Stack and correlation ---
    stacked <- c(twi_rc, vegh_rc)
    df_win <- create_spatial_windows(stacked, value_vars = c("twi", "vegh"), dwin = 0.005)
    df_cor <- calculate_correlation_bywin(df_win, x = "twi", y = "vegh")

    # --- Save correlation ---
    corA_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "correlation")], type="xyz", crs="EPSG:4326")
    names(corA_r) <- "r_H_TWI"
    corA_nc_path <- file.path(output_dir, paste0("tile_", reg_id, "_r_H_TWI_500m_map.nc"))
    terra::writeCDF(corA_r, corA_nc_path, overwrite = TRUE)
    message("Saved: ", corA_nc_path)

    # --- Save p-value ---
    pval_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "cor_pval")], type="xyz", crs="EPSG:4326")
    names(pval_r) <- "pval_r_H_TWI"
    pval_nc_path <- file.path(output_dir, paste0("tile_", reg_id, "_r_H_TWI_500m_pval.nc"))
    terra::writeCDF(pval_r, pval_nc_path, overwrite = TRUE)
    message("Saved: ", pval_nc_path)

    rm(df_win, df_cor, pval_r);gc()

    # --- Elevation and radiation Raster ---
    dem_rc <- extent_to_tile_ids(reg_extent, tile_size = 1, return_raster = TRUE,
                                 source = "copernicus_dem_30m", tiles_dir = dem_30m_copernicus_dir)


    # Calculate slope/aspect and resample to twi_rc
    aligned <- aggregate_topography(
      dem_rc,
      res_tar = NULL,
      target = twi_rc,
      if_aggregate = FALSE,
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
    cl <- makeCluster(workers)
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
    rin <- terra::rast(df_calc[, c("lon", "lat", "rin")], type = "xyz", crs = crs_out)
    rin <- terra::extend(rin, twi_rc)

    # --- Stack and Correalation---
    stacked <- c(rin, vegh_rc)
    names(stacked) <- c("rin", "vegh")
    df_win <- create_spatial_windows(stacked, value_vars = c("rin", "vegh"), dwin = 0.005)
    df_cor <- calculate_correlation_bywin(df_win, x = "rin", y = "vegh")

    # --- Save correlation ---
    corB_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "correlation")], type = "xyz", crs = "EPSG:4326")
    names(corB_r) <- "r_H_R"
    corB_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_r_H_R_500m.nc"))
    terra::writeCDF(corB_r, corB_nc_path, overwrite = TRUE)
    message("Saved: ", corB_nc_path)

    # --- Save p-value ---
    pval_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "cor_pval")], type = "xyz", crs = "EPSG:4326")
    names(pval_r) <- "pval_r_H_R"
    pval_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_pval_r_H_R_500m.nc"))
    terra::writeCDF(pval_r, pval_nc_path, overwrite = TRUE)
    message("Saved: ", pval_nc_path)

    # --- Save Rin as NetCDF ---
    rin_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_rin_30m.nc"))
    terra::writeCDF(rin, rin_nc_path, overwrite = TRUE)
    message("Saved: ", rin_nc_path)

    # --- Save DEM as NetCDF ---
    dem_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_dem_30m.nc"))
    terra::writeCDF(aligned[["dem"]], dem_nc_path, overwrite = TRUE)
    message("Saved: ", dem_nc_path)

    # --- Save Slope as NetCDF ---
    slope_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_slope_30m.nc"))
    terra::writeCDF(aligned[["slope"]], slope_nc_path, overwrite = TRUE)
    message("Saved: ", slope_nc_path)

    # --- Save Aspect as NetCDF ---
    aspect_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_aspect_30m.nc"))
    terra::writeCDF(aligned[["aspect"]], aspect_nc_path, overwrite = TRUE)
    message("Saved: ",  aspect_nc_path)

    # --- reset theme for plots ---

    re_theme <- list(
      guides(fill = guide_colorbar(barwidth = 0.8, barheight = 12)),
      ggplot2::theme(
        aspect.ratio = 1,
        legend.position = "right",
        legend.text = ggplot2::element_text(size = text_size * 0.9, angle = 90,
                                            hjust = 0.5, vjust = 0.5,
                                            margin = margin(r = 0, l = 2)),
        legend.title = ggplot2::element_text(size = text_size,
                                             angle = 90,   hjust = 0, vjust = 0.5 ),
        legend.margin = margin(0, 0, 0, 0),
        legend.box.margin = margin(0, 2, 0, -8),
        axis.title.x = ggplot2::element_blank(),
        axis.title.y = ggplot2::element_blank(),
        axis.text.x = ggplot2::element_text(
          size = text_size * 0.8,
          hjust = 0.5,
          vjust = 0.5,
          margin = margin(t = 2, b = 2),
        ),
        axis.text.y = ggplot2::element_text(
          size = text_size * 0.8,
          hjust = 0.5,
          vjust = 0.5,
          margin = margin(r = 2, l = -15) # important set to reduce the space between 2 plot
        ),
        panel.spacing = unit(0, "cm"),
        panel.border = ggplot2::element_rect(linewidth = 0.3, fill = NA),
        plot.margin = margin(0, 0, 0, 0),
        plot.title = ggplot2::element_text(
          size = text_size * 1.2,
          face = "plain",
          margin = margin(b = 0)
        ),
        plot.title.position = "panel"
      )
    )

    # The left indentation of the y-axis label should be removed from the first image in each row;
    # otherwise, the y-axis text will be clipped.
    re_theme_left <- ggplot2::theme(axis.text.y = ggplot2::element_text(
      margin = margin(r = 2, l = 0) # important set to left plot to show full y_text
    ))

    # --- Plotting, change the layout for better visualization ---

    p_twi <- plot_twi(twi_rc, extent = reg_extent, title_text = "30 m: TWI", text_size = text_size, x_step = x_step, y_step = y_step)  +
      re_theme + re_theme_left
    p_dem <- plot_dem(dem_rc, extent = reg_extent, title_text = "30 m: Elevation", text_size = text_size, x_step = x_step, y_step = y_step)  + re_theme
    p_rin <- plot_rin(rin, extent = reg_extent, title_text = "30 m: Radiation index",  text_size = text_size, x_step = x_step, y_step = y_step) + re_theme


    p_rA <- plot_cor_twi_vegh(corA_r, extent = reg_extent,  title_text <- bquote("500 m: Pearson's " * r[.("H")*","*.("TWI")]), text_size = text_size, x_step = x_step, y_step = y_step)  +
      re_theme  + re_theme_left
    p_vegh <- plot_vegh(vegh_rc, extent = reg_extent, title_text = expression("30 m: " * italic(H)[veg]), text_size = text_size, x_step = x_step, y_step = y_step)  + re_theme
    p_rB <- plot_r_H_R(corB_r, extent = reg_extent, title_text = bquote("500 m: Pearson's " * r[.("H")*","*.("Rᵢₙ")]), text_size = text_size, x_step = x_step, y_step = y_step) + re_theme


    p_rA2 <- plot_cor_twi_vegh(cor_twi_vegh_mosaic_file, extent = reg_extent, title_text = bquote("5 km: Pearson's " * r[.("H")*","*.("TWI")]), text_size = text_size, x_step = x_step, y_step = y_step)  +
      re_theme  + re_theme_left
    p_fused <- plot_fused(fused_5km_file, extent = reg_extent, text_size = text_size, x_step = x_step, y_step = y_step)  + re_theme
    p_rB2 <- plot_r_H_R(r_H_R_5km_path, extent = reg_extent, title_text = bquote("5 km: Pearson's " * r[.("H")*","*.("Rᵢₙ")]), text_size = text_size, x_step = x_step, y_step = y_step) + re_theme

    p_google <- plot_google_img(extent = reg_extent) + ggplot2::theme(aspect.ratio = 1) + ggplot2::theme( aspect.ratio = 1)
    p_location <- plot_single_sample_location(reg_xmid, reg_ymid,  reg_id, text_size = text_size) + ggplot2::theme( aspect.ratio = 1)
    p_kg <- plot_kg_class(kg_present_0p0083_file, kg_legend_file, extent = reg_extent, text_size = text_size, x_step = x_step, y_step = y_step) + ggplot2::theme( aspect.ratio = 1)

    # p_scatter <- plot_hex_scatter(df_win, x_var = "twi", y_var = "vegh", title_text = "H vs TWI",
    #                               x_text = "Topographic Wetness Index", y_text = "Vegetation height (m)",
    #                               text_size = text_size)  + ggplot2::theme(aspect.ratio = 1)

    # ---- Combine plots 1----
    final_plot1 <- ((p_twi + p_dem + p_rin) /
                      (p_rA + p_vegh + p_rB)) +
      plot_layout(heights = c(1, 1))

    out_file1 <- here::here(file.path(paste0("data/figures/07_reg_", reg_id, "_6plots.png")))
    ggsave(filename = out_file1, plot = final_plot1, width = 14, height = 9.2, dpi = 600)

    # ---- Combine plots 2 ----
    final_plot2 <- ((p_twi + p_dem + p_rin) /
                      (p_rA + p_vegh + p_rB) /
                      (p_rA2 + p_fused + p_rB2) /
                      (p_google + p_location + p_kg)) +
      plot_annotation(title = reg_id) +
      plot_layout(heights = c(1, 1, 1, 1))

    out_file2 <- here::here(file.path(paste0("data/figures/07_reg_", reg_id, "_9plots.png")))
    ggsave(filename = out_file2, plot = final_plot2, width = 14, height = 19.2, dpi = 600)

    # # --- Cleanup ---
    # rm(twi_rc, vegh_rc, dem_rc, aligned,
    #    df, chunks, df_calc, stacked, df_win, df_cor, corA_r, pval_r, corB_r,
    #    p_dem, p_vegh, p_twi, p_rin, p_rB, p_rA, p_google, p_kg, p_location,
    #    p_rA2, p_fused, p_rB2,
    #    final_plot1, final_plot2)
    # gc(verbose = FALSE)

    # --- Print proccessed time ---
    elapsed_mins <- difftime(Sys.time(), t0, units = "mins")
    message(sprintf("Region %s completed [%.1f mins]", reg_id, elapsed_mins))
    tictoc::toc()

    return(TRUE)

  }, error = function(e) {
    reg_id <- paste0(reg_row$strata_label, "_", reg_row$sample_id)
    elapsed_mins <- difftime(Sys.time(), t0, units = "mins")
    message(sprintf("❌ Tile %s failed after %.1f mins: %s", reg_id, elapsed_mins, e$message))
    return(FALSE)
  })
}

# ----------- Test on single regions -----------------------------

# load sample regions info
reg_info <- readRDS(reg_sample_info_path) |>
  select(ends_with("label"), ends_with("min"), ends_with("max"), sample_id)

# test on single regions
# process_reg_500m(reg_info[11, ])

for (i in seq_len(nrow(reg_info))) {
  process_reg_500m(reg_info[i, ])
}

# # ----------- Test on smaller regions -----------------------------
# reg_info <- data.frame(
#   strata_label = c("test_region"),
#   ymin = c(71.0),
#   ymax = c(71.2),
#   xmin = c(-179),
#   xmax = c(-178.8),
#   sample_id = c("")
# )
#
# # center location
# reg_info$xmid <- (reg_info$xmax + reg_info$xmin) / 2
# reg_info$ymid <- (reg_info$ymax + reg_info$ymin) / 2
#
# # process_reg_500m(reg_info[1, ])
#
# reg_row <- reg_info[1, ]
# output_dir = reg_correlation_dir
# text_size = 12
# x_step = 0.1
# y_step = 0.1




