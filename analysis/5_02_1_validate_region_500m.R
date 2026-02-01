# ~ UBELIX: 8.5 min for 34 sample regions
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
source(here::here("R/plot_kmeans_map.R"))
source(here::here("R/plot_mi.R"))
source(here::here("R/plot_cci_land_cover.R"))
source(here::here("R/plot_scatter_r_validation.R"))
# Optional (uncomment if needed)

source(here::here("R/plot_aspect.R"))
source(here::here("R/plot_slope.R"))
source(here::here("R/plot_sw_in.R"))


# ------ File Configuration ---------------------------------------------

if (!dir.exists(reg_validate_dir)) {
  dir.create(reg_validate_dir, recursive = TRUE)
  message("Directory created: ", reg_validate_dir)
}

# --------------- Main Processing Function -------------------------------------

#' Process a single region for TWI and vegetation height correlation analysis
#'
#' @param reg_row Region Information about the reg_id,sample_id, xmix, xmax, ymin, ymax
#' @param output_dir Directory to save output NetCDF files
#'
#' @return Returns TRUE if successful, FALSE otherwise
process_reg_500m <- function(reg_row, output_dir = reg_validate_dir,
                             text_size = 14) {

  tryCatch({
    t0 <- Sys.time()

    # --- Region info ---
    reg_id <- reg_row$strata_label
    reg_extent <- terra::ext(reg_row$xmin, reg_row$xmax, reg_row$ymin, reg_row$ymax)
    reg_xmid <- (reg_row$xmin +reg_row$xmax) / 2
    reg_ymid <- (reg_row$ymin +reg_row$ymax) / 2

    x_step <- reg_row$xmax - reg_row$xmin
    y_step <-  reg_row$ymax - reg_row$ymin

    # Start Processing
    tictoc::tic(paste0("Processing tile: ", reg_id))
    t0 <- Sys.time()

    # --- TWI Raster ---
    twi_rc <- terra::rast(twi_30m_path) |> terra::crop(reg_extent)
    twi_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_twi_30m.nc"))
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
    vegh_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_vegh_30m.nc"))
    terra::writeCDF(vegh_rc, vegh_nc_path, varnames="vegh", overwrite = TRUE)
    rm(vegh_rc); gc()
    vegh_rc <- terra::rast(vegh_nc_path)
    message("Saved: ", vegh_nc_path)

    # --- Stack and correlation using 30-m input data and 500m window---
    stacked <- c(twi_rc, vegh_rc)
    df_win <- create_spatial_windows(stacked, value_vars = c("twi", "vegh"), dwin = 0.005)
    df_cor <- calculate_correlation_bywin(df_win, x = "twi", y = "vegh")

    p_H_TWI_30 <- plot_hex_scatter(df_win,x_var="twi",y_var = "vegh", x_text = "Topographic wetness index", y_text = "Vegetation height (m)", text_size = text_size, title_text="TWI vs H at 30 m")

    # --- Save correlation ---
    corA_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "correlation")], type="xyz", crs="EPSG:4326")
    names(corA_r) <- "r_H_TWI"
    corA_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_r_H_TWI_30m_500m_map.nc"))
    terra::writeCDF(corA_r, corA_nc_path, overwrite = TRUE)
    message("Saved: ", corA_nc_path)

    # --- Save p-value ---
    pval_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "cor_pval")], type="xyz", crs="EPSG:4326")
    names(pval_r) <- "pval_r_H_TWI"
    pval_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_r_H_TWI_30m_500m_pval.nc"))
    terra::writeCDF(pval_r, pval_nc_path, overwrite = TRUE)
    message("Saved: ", pval_nc_path)

    # --- Stack and correlation using 30-m input data and 5km window---
    # stacked <- c(twi_rc, vegh_rc)
    df_win <- create_spatial_windows(stacked, value_vars = c("twi", "vegh"), dwin = 0.05)
    df_cor <- calculate_correlation_bywin(df_win, x = "twi", y = "vegh")

    # --- Save correlation ---
    corA_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "correlation")], type="xyz", crs="EPSG:4326")
    names(corA_r) <- "r_H_TWI"
    corA_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_r_H_TWI_30m_5000m_map.nc"))
    terra::writeCDF(corA_r, corA_nc_path, overwrite = TRUE)
    message("Saved: ", corA_nc_path)

    # --- Save p-value ---
    pval_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "cor_pval")], type="xyz", crs="EPSG:4326")
    names(pval_r) <- "pval_r_H_TWI"
    pval_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_r_H_TWI_30m_5000m_pval.nc"))
    terra::writeCDF(pval_r, pval_nc_path, overwrite = TRUE)
    message("Saved: ", pval_nc_path)

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

    # --- Stack and Correalation using 30-m input data and 0.005-degree window---
    stacked <- c(rin, vegh_rc)
    names(stacked) <- c("rin", "vegh")
    df_win <- create_spatial_windows(stacked, value_vars = c("rin", "vegh"), dwin = 0.005)
    df_cor <- calculate_correlation_bywin(df_win, x = "rin", y = "vegh")

    p_H_R_30 <- plot_hex_scatter(df_win,x_var="rin",y_var = "vegh", x_text = "Topographic radaition index", y_text = "Vegetation height (m)", text_size = text_size, title_text="Rin vs H at 30 m")

    # --- Save correlation ---
    corB_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "correlation")], type = "xyz", crs = "EPSG:4326")
    names(corB_r) <- "r_H_R"
    corB_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_r_H_R_30m_500m_map.nc"))
    terra::writeCDF(corB_r, corB_nc_path, overwrite = TRUE)
    message("Saved: ", corB_nc_path)

    # --- Save p-value ---
    pval_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "cor_pval")], type = "xyz", crs = "EPSG:4326")
    names(pval_r) <- "pval_r_H_R"
    pval_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_r_H_R_30m_500m_pval.nc"))
    terra::writeCDF(pval_r, pval_nc_path, overwrite = TRUE)
    message("Saved: ", pval_nc_path)

    # --- Stack and Correalation using 30-m input data and 0.05-degree window---
    # stacked <- c(rin, vegh_rc)
    # names(stacked) <- c("rin", "vegh")
    df_win <- create_spatial_windows(stacked, value_vars = c("rin", "vegh"), dwin = 0.05)
    df_cor <- calculate_correlation_bywin(df_win, x = "rin", y = "vegh")

    # --- Save correlation ---
    corB_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "correlation")], type = "xyz", crs = "EPSG:4326")
    names(corB_r) <- "r_H_R"
    corB_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_r_H_R_30m_5000m_map.nc"))
    terra::writeCDF(corB_r, corB_nc_path, overwrite = TRUE)
    message("Saved: ", corB_nc_path)

    # --- Save p-value ---
    pval_r <- terra::rast(df_cor[, c("lon_mid", "lat_mid", "cor_pval")], type = "xyz", crs = "EPSG:4326")
    names(pval_r) <- "pval_r_H_R"
    pval_nc_path <- file.path(output_dir, paste0("reg_", reg_id, "_r_H_R_30m_5000m_pval.nc"))
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

    rm(stacked, df_win, df_calc, df_cor, corA_r, corB_r);gc

    # --- reset theme for plots, change the layout for better visualization ---
    re_theme0 <- list(
      ggplot2::theme(
        aspect.ratio = 1,
        legend.position = "bottom",
        panel.background = element_rect(fill = NA, color = NA),
        legend.background = element_rect(fill = NA, color = NA),
        legend.box.background = element_rect(fill = NA, color = NA),

        legend.text = ggplot2::element_text(size = text_size * 0.9,
                                            angle = 90,
                                            hjust = 0.5, vjust = 0.5,
                                            margin = margin(r = 0, b = 0, l = 0)),
        legend.title = ggplot2::element_text(size = text_size,
                                             angle = 0,   hjust = 0, vjust = 1 ),
        legend.margin = margin(0, 0, 0, 0),
        legend.box.margin = margin(-45, 0, 0, 0),
        axis.text.x = ggplot2::element_text(
          angle = 90,
          size = text_size * 0.8,
          hjust = 0.5,
          vjust = 0.5,
          margin = margin(t = 0, b = 0),
        ),
        axis.text.y = ggplot2::element_text(
          angle = 90,
          size = text_size * 0.8,
          hjust = 0.5,
          vjust = 0.5,
          margin = margin(r = 2, l = 0)
        ),
        panel.spacing = unit(0, "cm"),
        panel.border = ggplot2::element_rect(linewidth = 0.3, fill = NA),
        plot.title = ggplot2::element_text(
          size = text_size * 1.2,
          face = "plain",
          margin = margin(b = 0)
        ),
        plot.title.position = "panel"
      )
    )

    re_theme <- list(
      guides(fill = guide_colorbar(barwidth = 8, barheight = 0.8)),
      ggplot2::theme(  axis.title.x = ggplot2::element_blank(),
                       axis.title.y = ggplot2::element_blank()),
      re_theme0
    )

    # re_theme_margin_r for the first fig in each rows
    # re_theme_margin_r <- ggplot2::theme( plot.margin = margin(t = 0, r = 20, b = 0, l = 0))

    # --- Plotting ---
    p_location <- plot_single_sample_location(reg_xmid, reg_ymid, title_text = "   Location", text_size = text_size) +
      labs(tag = "a)") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.13, 1)
      ) + ggplot2::theme(aspect.ratio = 1, plot.margin = margin(t = 0, r = 20, b = 4, l = 0))

    p_google <- plot_google_img(extent = reg_extent, title_text = "   Google Satellite Map", text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "b)") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.06, 1)) + re_theme0 +  ggplot2::theme(plot.margin = margin(t = 0, r = 0, b = 4, l = 0))

    p_dem <- plot_dem(file.path(output_dir, paste0("reg_", reg_id, "_dem_30m.nc")), extent = reg_extent, title_text = "   30-m Elevation", text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "c)") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.12, 1)
      ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 0, r = 20, b = 4, l = 0))

    p_vegh <- plot_vegh(file.path(output_dir, paste0("reg_", reg_id, "_vegh_30m.nc")), extent = reg_extent, title_text = expression("   30-m " * italic(H)), text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "d)") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.06, 1)
      ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 0, r = 0, b = 4, l = 0))

    p_twi <- plot_twi(file.path(output_dir, paste0("reg_", reg_id, "_twi_30m.nc")), extent = reg_extent, title_text = "   30-m TWI", text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "e)") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.13, 1)
      ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 4, r = 20, b = 4, l = 0))

    p_rA <- plot_cor_twi_vegh(file.path(output_dir, paste0("reg_", reg_id, "_r_H_TWI_30m_500m_map.nc")), extent = reg_extent,  title_text <- bquote("   500-m Pearson's " * r[.("H")*","*.("TWI")]), text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "f)") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.06, 1)
      ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 4, r = 0, b = 4, l = 0))

    p_rin <- plot_rin(file.path(output_dir, paste0("reg_", reg_id, "_rin_30m.nc")), extent = reg_extent, title_text = "   30-m Rᵢₙ",  text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "g)") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.12, 1)
      ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 4, r = 20, b = 4, l = 0))

    p_rB <- plot_r_H_R(file.path(output_dir, paste0("reg_", reg_id, "_r_H_R_30m_500m_map.nc")), extent = reg_extent, title_text = bquote("   500-m Pearson's " * r[.("H")*","*.("Rᵢₙ")]), text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "h)") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.06, 1)
      ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 4, r = 0, b = 4, l = 0))

    p_validA <- plot_scatter_r_validation(file.path(output_dir, paste0("reg_", reg_id, "_r_H_TWI_30m_5000m_map.nc")), cor_twi_vegh_mosaic_file, title_text = bquote("   Comparison " * r[.("H")*","*.("TWI")]), text_size = text_size) +
      labs(tag = "i)") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.13, 1)
      ) + re_theme0 + ggplot2::theme(legend.position = "none", plot.margin = margin(t = 4, r = 20, b = 0, l = 0))

    p_rA2 <- plot_cor_twi_vegh(cor_twi_vegh_mosaic_file, extent = reg_extent, title_text = bquote("   5-km Pearson's " * r[.("H")*","*.("TWI")]), text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "j)") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.06, 1)
      ) + re_theme + ggplot2::theme(legend.box.margin = margin(-75, 0, 0, 0), plot.margin = margin(t = 4, r = 10, b = 0, l = 0))

    p_validB <- plot_scatter_r_validation(file.path(output_dir, paste0("reg_", reg_id, "_r_H_R_30m_5000m_map.nc")), r_H_R_5km_path, title_text = bquote("   Comparison " * r[.("H")*","*.("Rᵢₙ")]), text_size = text_size) +
      labs(tag = "k)") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.12, 1)
      ) + re_theme0 + ggplot2::theme(legend.position = "none", plot.margin = margin(t = 4, r = 0, b = 0, l = 0))

    p_rB2 <- plot_r_H_R(r_H_R_5km_path, extent = reg_extent, title_text = bquote("   5-km Pearson's " * r[.("H")*","*.("Rᵢₙ")]), text_size = text_size, x_step = x_step, y_step = y_step)+
      labs(tag = "l)") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.06, 1)
      ) + re_theme  +  ggplot2::theme(legend.box.margin = margin(-75, 0, 0, 0), plot.margin = margin(t = 4, r = 0, b = 0, l = 0))

    final_plot1 <- patchwork::wrap_plots(
      p_location, p_google,  p_dem, p_vegh,
      p_twi, p_rA, p_rin, p_rB,
      p_validA, p_rA2,  p_validB, p_rB2,
      ncol = 4, nrow = 3
    ) &
      theme(
        panel.background = element_rect(fill = "white", color = NA),
        plot.background  = element_blank(),
        legend.background = element_blank(),
        legend.box.background = element_blank()
      )

    out_file1 <- here::here(file.path(paste0("data/figures/5_02_validate_", reg_id, "_12plots.png")))
    ggsave(filename = out_file1, plot = final_plot1, width = 14, height = 12.1, dpi = 600)

    out_file1 <- here::here(file.path(paste0("data/figures/5_02_validate_", reg_id, "_12plots.svg")))
    ggsave(filename = out_file1, plot = final_plot1, width = 14, height = 12.1, device = "svg", bg = "transparent")

    # --- additional plots ---
    p_vegh450 <- plot_vegh(vegh_450m_mosaic_path, extent = reg_extent, title_text = expression("   450-m " * italic(H)), text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.06, 1)
      ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 0, r = 0, b = 4, l = 0))

    p_twi450 <- plot_twi(twi_450m_mosaic_clean_path, extent = reg_extent, title_text = "   450-m TWI", text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.13, 1)
      ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 4, r = 20, b = 4, l = 0))

    p_slope30 <- plot_slope(file.path(output_dir, paste0("reg_", reg_id, "_slope_30m.nc")), extent = reg_extent, title_text = "   30-m slope (°)", text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.13, 1)
      ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 4, r = 20, b = 4, l = 0))

    p_slope450 <- plot_slope(slope_450m_mosaic_path, extent = reg_extent,  title_text <- "450-m slope (°)", text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.06, 1)
      ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 4, r = 0, b = 4, l = 0))

    p_aspect30 <- plot_aspect(file.path(output_dir, paste0("reg_", reg_id, "_aspect_30m.nc")), extent = reg_extent,  title_text <- "30-m aspect (°)", text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.06, 1)
      ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 4, r = 0, b = 4, l = 0))

    p_aspect450 <- plot_aspect(aspect_450m_mosaic_path, extent = reg_extent,  title_text <- "450-m aspect (°)", text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.06, 1)
      ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 4, r = 0, b = 4, l = 0))

    p_rin450 <- plot_rin(sw_in_terrain_effect_450m_path, extent = reg_extent,  title_text <- "450-m Rin", text_size = text_size, x_step = x_step, y_step = y_step) +
      labs(tag = "") +
      theme(
        plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
        plot.tag.position = c(0.06, 1)
      ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 4, r = 0, b = 4, l = 0))

    final_plot2 <- patchwork::wrap_plots(
      p_vegh450, p_twi450, p_rin450, patchwork::plot_spacer(),
      p_slope30, p_slope450, p_aspect30, p_aspect450,
      ncol = 4, nrow = 2
    ) &
      theme(
        panel.background = element_rect(fill = "white", color = NA),
        plot.background  = element_blank(),
        legend.background = element_blank(),
        legend.box.background = element_blank(),
      )

    out_file2 <- here::here(file.path(paste0("data/figures/5_02_validate_", reg_id, "_7plots.png")))
    ggsave(filename = out_file2, plot = final_plot2, width = 14, height = 8, dpi = 600)

    out_file2 <- here::here(file.path(paste0("data/figures/5_02_validate_", reg_id, "_7plots.svg")))
    ggsave(filename = out_file2, plot = final_plot2, width = 14, height = 8, device = "svg", bg = "transparent")


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

# ------------ validation region define ----------------------------------------
# all samples regions from 5_01
reg_info_all_samples <- readRDS(reg_sample_info_path)
print(reg_info_all_samples )

# select 6 samples from 5_01 combined all MI bins and abs_lat bins
reg_info1 <- readRDS(reg_sample_info_path) |>
  select(ends_with("label"), ends_with("min"), ends_with("max"), -starts_with("dem")) |>
  slice(c(1, 3, 5, 9, 13, 15))
print(reg_info1)

reg_info2 <- tribble(
  ~strata_label,                        ~ymin,   ~ymax,   ~xmin,     ~xmax,
  "hyper_arid_low_lat_rugged_relief", -9,  -8.5,      38,      38.5,
  "b1_Loetschental",                    46.4,    46.5,      7.8,       7.9,
  "b3_equatorial_rainforest_CongoBasin",-1.0,    -0.5,     17.0,      17.5,
  "a1_waterlogged_pantanal",            -17.5,   -16.5,   -57.5,     -56.5,
  "b25_Finland",                        67.5,    68.5,     25,        26,
  "b26_Monte_Alen_Guiana",          1.2,       1.7,      9.8,        10.3,
  "a19_arctic_tundra_alaska",           68.5,    69,   -146,    -145.5,

  # ------------------ Subsurface flow validation (Fan et al., 2019) -----------
  "3a_desert_riparian",                 31,       32,      -110.5,   -109.5,
  "3b_mediterranean_california",        37,       38,      -122.5,   -121.5,
  "3c_forest_savanah_east_congo",       -5.5,     -4.5,     28,        29,
  "3d_waterlogged_pantanal",            -17.8,    -16.8,   -58,       -57,
  "3e_white_sand_amazon",               3,         4,      -67.7,     -66.7,
  "3f_cool_wet_denmark",                56.7,     57.0,     10.0,      10.5,

  # ------------------ Additional validation from global results -----------

  "a2_amazon_floodplain_colombia",       0.5,     1.5,    -70,       -69,
  "a3_peru_western_amazon_terra_firme",  0,       1,      -71.5,     -70.5,
  "a4_forest_savanna_congo_mayombe",    -5.2,    -4.2,     17,        18,
  "a5_miombo_savanna_angola",           -8,      -7,       16.3,      17.3,
  "a6_zambezi_riparian_zambia",        -12,     -11,       18.5,      19.5,
  "a7_madagascar_highland_mosaic",     -21,     -20,       46.5,      47.5,
  "a8_hindu_kush_steppe_afghanistan",   34,      35,       66.5,      67.5,
  "a9_korean_temperate_forest",         35.7,    36.7,    127.6,     128.6,
  "a10_siberian_taiga_russia",          61,      62,      142.7,     143.7,
  "a11_borneo_lowland_rainforest",       0.2,     1.2,    112.8,     113.8,
  "a12_great_plains_grassland_usa",     38.5,    39.5,    -99,       -98,
  "a13_swiss_plateau_mixed_forest",     46.8,    47.8,      7,         8,
  "a14_french_alps_montane_forest",     44.5,    45.5,      6,         7,
  "a15_southwest_france_temperate",     43.6,    44.6,      2,         3,
  "a16_basque_coastal_forest_spain",    41.5,    42.5,     -2,        -1,
  "a17_yungas_cloud_forest_bolivia",   -23.3,   -22.3,    -65.3,     -64.3,
  "a18_gran_chaco_dry_savanna",        -26.6,   -25.6,    -60,       -59,
  "a20_jiangxi_lowlands_china",         28,      29,      114,       115,
  "a21_fujian_jiangxi_hills_china",     25.5,    26.5,    115.3,     116.3,

  # ------------------ Slope aspect validation (Fan et al., 2019) --------------
  # --- North America (Arid & Temperate regions) ---
  "2a_arid_western_Texas",              31.5,    32.5,   -105.0,    -104.5,
  "2b_seasonal_arid_California_foothills",37.5,  38.5,   -120.5,    -119.5,
  "2c_seasonal_arid_Idaho_Basin",       43.0,    44.0,   -115.8,    -114.8,
  "2d_winter_snowpacks_Mount_Cramer",   43.8,    44.8,   -115.5,    -114.5,

  # --- Northern high-latitude regions ---
  "2e_high_latitude_southwest_Yukon",   61.5,    62.5,   -138.5,    -137.5,
  "2f_boreal_forest_Alaska",            64.0,    65.0,   -148.0,    -147.0,

  # ------------------ Additional validation from global results ----------------

  "b2_Parc_National_Suisse",            46.5,    46.8,     10.1,      10.4,

  "b4_WesternAlps_MontBlanc_massif",    45.0,    45.5,      6.5,       7.0,
  "b5_subtropical_Yunnan_region",       35.5,    36.0,     97.0,      97.5,
  "b6_Orinoco_transition_forest",       -2.5,    -1.5,    120,       121,
  "b7_borneo_lowland_rainforest",        1,       2,      112.8,     113.8,
  "b8_victoria_coastal_forest_australia",-38,    -37,     146.2,     147.2,
  "b9_equatorial_forest_congo",         -0.5,     0.5,     16,        17,
  "b10_lesotho_mountain_grassland",    -30,     -29,      27.5,      28.5,
  "b11_ethiopian_highlands",             7.5,     8.5,     39,        40,
  "b12_bolivia_andes_cloud_forest",    -18.8,   -17.8,    -66.1,     -65.1,
  "b13_pantanal_wetland_brazil",       -19,     -18,     -57,       -56,
  "b14_patagonian_steppe_chile",       -47,     -46,     -69,       -68,
  "b15_altiplano_bolivia_peru",        -18,     -17,     -69.5,     -68.5,
  "b16_amazon_rainforest_brazil",       -1.5,    -0.5,    -62.7,     -61.7,
  "b17_arctic_tundra_alaska",           68.5,    69.5,   -146,      -145,
  "b18_boreal_forest_yukon",            62.6,    63.6,   -142.4,    -141.4,
  "b19_boreal_forest_nwt_canada",       63.5,    64.5,   -130.5,    -129.5,
  "b20_mississippi_river_valley_usa",   38.5,    39.5,    -89,       -88,
  "b21_western_anatolia_turkey",        37,      38,      28,        29,
  "b22_khorat_plateau_thailand",        15.5,    16.5,    103,       104,
  "b23_nepal_middle_hills",             27.2,    28.2,     81.8,      82.8,
  "b24_tianshan_mountains_xinjiang",    42.5,    43.5,     86,        87

)

reg_info <- bind_rows(reg_info1, reg_info2)

# ----------- Process sample regionsand other validation regions-----------------------------
# process_reg_500m(reg_info[8, ]) # smallest one, best for single region test

for (i in 1:6) {
  process_reg_500m(reg_info[i, ])
}

for (i in 7:13) {
  process_reg_500m(reg_info[i, ])
}

for (i in 14:66) {
  process_reg_500m(reg_info[i, ])
}

# # ----------- Test on smaller regions -----------------------------
# reg_info <- data.frame(
#   strata_label = c("b1_Loetschental"),
#   ymin = c(46.4),
#   ymax = c(46.5),
#   xmin = c(7.8),
#   xmax = c(7.9),
#   sample_id = c(1)
# )
#
# # center location
# reg_info$xmid <- (reg_info$xmax + reg_info$xmin) / 2
# reg_info$ymid <- (reg_info$ymax + reg_info$ymin) / 2
#
# # process_reg_500m(reg_info[1, ])
#
# reg_row <- reg_info[1, ]
# output_dir = reg_validate_dir
# text_size = 14


# # print the plots one by one
# print(p_location)
# print(p_google)
# print(p_dem)
# print(p_vegh)
# print(p_twi)
# print(p_rA)
# print(p_rin)
# print(p_rB)
# print(p_validA)
# print(p_rA2)
# print(p_validB)
# print(p_rB2)
# print(p_aspect30)
# print(p_aspect450)
# print(p_slope30)
# print(p_slope450)
# print(p_H_R_30)
# print(p_H_TWI_30)
#

