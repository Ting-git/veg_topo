plot_kmeans_map <- function(raster, k, palette_name = "Paired", title_text = NULL, highlight_cluster = NULL) {

  fill_colors <- RColorBrewer::brewer.pal(k, palette_name)
  names(fill_colors) <- as.character(1:k)

  alpha_values <- rep(1, k)
  names(alpha_values) <- as.character(1:k)

  if (!is.null(highlight_cluster)) {
    alpha_values[] <- 0.1
    alpha_values[as.character(highlight_cluster)] <- 1
  }

  raster_factor <- as.factor(raster)

  ggplot() +
    tidyterra::geom_spatraster(
      data = raster_factor,
      aes(
        fill = after_stat(factor(value)),  # explicitly map fill by cluster
        alpha = after_stat(factor(value))  # match alpha to cluster too
      ),
      maxcell = Inf
    ) +
    # tidyterra::geom_spatraster(data = raster_factor, aes(alpha = factor(after_stat(value))), maxcell = Inf) +
    geom_sf(data = coast, colour = 'black', linewidth = 0.1) +
    scale_fill_manual(
      values = fill_colors,
      name = "Cluster",
      na.value = NA,
      guide = guide_legend(
        title.position = "left",
        label.position = "bottom",
        nrow = 1
      )
    ) +
    scale_alpha_manual(
      values = alpha_values,
      guide = "none"
    ) +
    labs(title = title_text) +
    scale_x_continuous(
      expand = c(0, 0),
      breaks = seq(-180, 180, by = 30)
    ) +
    scale_y_continuous(
      expand = c(0, 0),
      limits = c(-60, 85),
      breaks = seq(-60, 90, by = 30)
    ) +
    theme_bw() +
    theme(
      plot.title = element_text(size = 24, face = "bold", hjust = 0, margin = margin(b = 5)),
      plot.title.position = "panel",
      axis.title = element_text(size = 18),
      axis.text = element_text(size = 14),
      legend.position = "bottom",
      legend.box = "horizontal",
      legend.text = element_text(size = 14),
      legend.title = element_text(size = 16, face = "bold", margin = margin(r = 10))
    )
}
