plot_biomes_by_extent <- function(ecoregions_path, xmin, xmax, ymin, ymax, x_breaks = 5, y_breaks = 5) {
  # Load the ecoregions shapefile
  suppressMessages(ecoregions <- sf::st_read(ecoregions_path, quiet = TRUE))

  # Fix invalid geometries
  ecoregions <- sf::st_make_valid(ecoregions)

  # Build the plot (no cropping, just setting visible extent)
  p <- ggplot(data = ecoregions) +
    geom_sf(aes(fill = BIOME_NAME), color = NA) +
    scale_fill_manual(
      values = setNames(ecoregions$COLOR_BIO, ecoregions$BIOME_NAME)
    ) +
    scale_x_continuous(
      name = "Longitude",
      expand = c(0, 0),
      limits = c(xmin, xmax),
      breaks = seq(xmin, xmax, by = x_breaks)
    ) +
    scale_y_continuous(
      name = "Latitude",
      expand = c(0, 0),
      limits = c(ymin, ymax),
      breaks = seq(ymin, ymax, by = y_breaks)
    ) +
    labs(title = "Biomes", fill = "Biome") +
    theme_classic() +
    theme(
      legend.position = "none",
      plot.title = element_text(face = "bold"),
      aspect.ratio = (ymax - ymin) / (xmax - xmin)
    )

  return(p)
}
