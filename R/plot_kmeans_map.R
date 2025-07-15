# Define function for plotting k-means classification map
plot_kmeans_map <- function(raster, k, palette_name = "Paired", title_text = NULL) {
  n_clusters <- length(unique(na.omit(values(raster))))
  fill_colors <- RColorBrewer::brewer.pal(n_clusters, palette_name)

  ggplot() +
    tidyterra::geom_spatraster(data = as.factor(raster), maxcell = Inf) +
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
