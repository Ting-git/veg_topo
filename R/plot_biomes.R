plot_biomes <- function(ecoregions_path, extent = NULL, title_text = "Biomes",
                        show_legend = FALSE, text_size = 12, x_step = 10, y_step = 10) {

  # ---- Load ecoregions shapefile ----
  suppressMessages(ecoregions <- sf::st_read(ecoregions_path, quiet = TRUE))

  # ---- Fix invalid geometries ----
  ecoregions <- sf::st_make_valid(ecoregions)

  # ---- Extract extent boundaries ----
  xmin <- terra::xmin(extent)
  xmax <- terra::xmax(extent)
  ymin <- terra::ymin(extent)
  ymax <- terra::ymax(extent)

  # ---- Build the plot ----
  p <- ggplot(data = ecoregions) +
    geom_sf(aes(fill = BIOME_NAME), color = NA) +
    scale_fill_manual(
      values = setNames(ecoregions$COLOR_BIO, ecoregions$BIOME_NAME)
    ) +
    ggplot2::labs(
      title = title_text,
      x = "Longitude",
      y = "Latitude",
      fill = NULL
    ) +
    scale_x_continuous(
      limits = c(xmin, xmax),
      breaks = seq(xmin, xmax, by = x_step),
      expand = c(0, 0)
    ) +
    scale_y_continuous(
      limits = c(ymin, ymax),
      breaks = seq(ymin, ymax, by = y_step),
      expand = c(0, 0)
    ) +
    theme_bw(base_size = text_size) +
    theme(
      legend.position = ifelse(show_legend, "right", "none"),
      legend.text = ggplot2::element_text(size = text_size * 0.9),
      legend.title = ggplot2::element_text(size = text_size),
      axis.title = ggplot2::element_text(size = text_size),
      axis.text = ggplot2::element_text(size = text_size  * 0.9),
      plot.title = ggplot2::element_text(size = text_size * 1.2, face = "bold"),
      plot.title.position = "panel"
    )


  return(p)
}
