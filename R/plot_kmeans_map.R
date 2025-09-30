#' Plot K-means clustering result on a map using ggplot2 and tidyterra
#'
#' This function visualizes a classified raster (SpatRaster) with custom colors
#' and optional cluster highlighting. The raster should already be classified
#' (numeric cluster IDs). If cluster labels are provided, they must be a **sorted named vector**,
#' where the names correspond to cluster values and the order determines legend order.
#'
#' @param raster A classified SpatRaster (numeric values corresponding to cluster IDs).
#' @param fill_colors Vector of colors for each cluster (should follow the same order as \code{cluster_labels} if provided).
#' @param title_text Optional title for the plot.
#' @param highlight_cluster Optional cluster ID to highlight (others will be faded).
#' @param cluster_labels Optional sorted named character vector of cluster labels.
#'        Names must match cluster values in the raster.
#'
#' @return A ggplot object showing the raster with cluster coloring, optional highlighting, and legend.
#'
#' @examples
#' \dontrun{
#' library(terra)
#' library(tidyterra)
#'
#' # Example raster (numeric clusters)
#' raster_factor <- cluster_raster
#'
#' # Define sorted cluster labels (names = raster values, values = label)
#' cluster_labels <- c("2"="Arid", "3"="Semi-arid", "1"="Sub-humid", "4"="Humid")
#' fill_colors <- c("#228B22", "#1E90FF", "#B22222", "#FFD700")
#'
#' # Plot with highlighting cluster 2
#' plot_kmeans_map(raster_factor, fill_colors,
#'                 highlight_cluster = 2,
#'                 cluster_labels = cluster_labels)
#'
#' # Plot without cluster labels
#' plot_kmeans_map(raster_factor, fill_colors)
#' }
#'
#' @export
plot_kmeans_map <- function(raster, fill_colors, title_text = NULL,
                            highlight_cluster = NULL, cluster_labels = NULL) {

  # Remove names from colors
  fill_colors <- unname(fill_colors)

  # Determine final cluster levels
  raster <- as.factor(raster)
  if (!is.null(cluster_labels)) {
    # Assign sorted levels using user-provided labels
    levels_df <- data.frame(value = as.numeric(names(cluster_labels)),   # must be numeric
                            category = unname(cluster_labels))
    levels(raster) <- levels_df
    final_levels <- as.character(levels(raster)[[1]]$category)
  } else {
    # Use raster unique values directly if no labels provided
    final_levels <- sort(unique(values(raster)))
    final_levels <- as.character(final_levels)
  }

  # Assign colors and alpha following final_levels
  names(fill_colors) <- final_levels
  alpha_values <- if (!is.null(highlight_cluster)) {
    ifelse(final_levels == as.character(highlight_cluster), 1, 0.2)
  } else {
    rep(1, length(final_levels))
  }
  names(alpha_values) <- final_levels

  # Legend labels
  legend_labels <- if (!is.null(cluster_labels)) {
    cluster_labels[final_levels]
  } else {
    final_levels
  }

  text_size = 18

  # Create ggplot
  ggplot() +
    tidyterra::geom_spatraster(
      data = raster,
      aes(fill = after_stat(as.factor(value)), alpha = after_stat(as.factor(value))),
      maxcell = Inf
    ) +
    scale_fill_manual(
      values = fill_colors,
      labels = legend_labels,
      name = "Cluster",
      na.value = NA,
      guide = guide_legend(
        title.position = "left",
        label.position = "bottom",
        nrow = 1
      )
    ) +
    scale_alpha_manual(values = alpha_values, guide = "none") +
    labs(title = title_text) +
    scale_x_continuous(expand = c(0, 0), breaks = seq(-180, 180, by = 30)) +
    scale_y_continuous(expand = c(0, 0), limits = c(-60, 85), breaks = seq(-60, 90, by = 30)) +
    theme_bw(base_size = text_size) +
    theme(
      plot.title = ggplot2::element_text(size = text_size * 1.2, face = "bold"),
      plot.title.position = "panel",
      axis.title = ggplot2::element_text(size = text_size),
      axis.text = ggplot2::element_text(size = text_size * 0.9),
      legend.position = "bottom",
      legend.box = "horizontal",
      legend.text = ggplot2::element_text(size = text_size),
      legend.title = ggplot2::element_text(size = text_size, face = "bold")
    )

}
