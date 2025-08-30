#' Plot K-means clustering result on a map using ggplot2 and tidyterra
#'
#' This function visualizes a classified raster (e.g., result of k-means clustering)
#' with user-defined color palette, optional cluster highlighting, and coastlines.
#'
#' @param raster A classified raster (SpatRaster) to be plotted.
#' @param fill_colors A named or unnamed vector of colors (length = number of unique values in raster).
#' @param title_text Optional title for the plot.
#' @param highlight_cluster Optional cluster number to highlight (others are faded).
plot_kmeans_map <- function(raster, fill_colors, title_text = NULL, highlight_cluster = NULL) {

  # Determine number of clusters from unique values in raster
  k <- length(fill_colors)

  # Set names for fill_colors if not already named
  if (is.null(names(fill_colors))) {
    names(fill_colors) <- as.character(1:k)
  }

  # Set alpha (transparency) values
  alpha_values <- rep(1, k)
  names(alpha_values) <- as.character(1:k)

  if (!is.null(highlight_cluster)) {
    alpha_values[] <- 0.2
    alpha_values[as.character(highlight_cluster)] <- 1
  }

  # Convert raster values to factor for discrete mapping
  raster_factor <- as.factor(raster)

  ggplot() +
    tidyterra::geom_spatraster(
      data = raster_factor,
      aes(
        fill = after_stat(factor(value)),
        alpha = after_stat(factor(value))
      ),
      maxcell = Inf
    ) +
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
