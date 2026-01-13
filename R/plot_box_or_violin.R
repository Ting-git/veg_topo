#' Plot Boxplot or Violin Plot for Clustered Data
#'
#' This function generates a boxplot or violin plot for a specified numeric variable
#' grouped by a categorical variable (cluster). It allows custom fill colors,
#' optional horizontal reference line for correlation, and control over legend display.
#'
#' @param data Data frame containing the data.
#' @param xvar Character. Name of the categorical variable (cluster) for x-axis.
#' @param yvar Character. Name of the numeric variable to plot on y-axis.
#' @param type Character. Type of plot: "boxplot" or "violin". Default is "boxplot".
#' @param ylab Character. Y-axis label. Default is NULL.
#' @param fill_colors Vector of colors for each cluster. Default is NULL.
#' @param show_legend Logical. If TRUE, show legend; otherwise hide legend. Default is FALSE.
#'
#' @return A ggplot object of the requested plot.
#'
#' @examples
#' p <- plot_box_or_violin(df, "cluster8c", "mi", "violin", "MI", fill_colors, show_legend = TRUE)
#' print(p)
plot_box_or_violin <- function(data, xvar, yvar, type = "boxplot", ylab = NULL, show_legend = FALSE, text_size = 18) {

  # fill_color for dry to wet cluster
  fill_colors <- setNames(
    c(
      "#E78AC3", # Pink - Arid
      "#FC8D62", # Orange - Semi-arid
      "#FFD92F", # Yellow - Semi-arid
      "#E5C494", # Light brown - Dry-sub-humid
      "#B3B3B3", # Gray - Humid
      "#66C2A5", # Blue-green - Humid
      "#8DA0CB", # Blue - Humid
      "#A6D854"   # Green - Humid
    ),
    cluster_labels)

  # Choose geom type based on 'type' argument
  geom_fun <- if (type == "boxplot")
    geom_boxplot(width = 0.9, linewidth = 0.2, outlier.size = 0.3, outlier.shape = 1, outlier.alpha = 0.6)
  else
    geom_violin(width = 0.9, linewidth = 0.2)

  # Optional plot title for correlation variable
  # title_text <- if (yvar == "cor" && type == "boxplot") "Boxplot" else
  #   if (yvar == "cor" && type == "violin") "Violin Distribution" else NULL
  title_text <- NULL

  ggplot(data, aes(x = .data[[xvar]], y = .data[[yvar]], fill = .data[[xvar]])) +
    geom_fun +  # Add the chosen geom
    list(if (yvar == "cor") geom_hline(yintercept = 0, linetype = "dashed", color = "red")) +  # Optional reference line for correlation
    scale_fill_manual(values = fill_colors, name = "Cluster", guide = if (show_legend) "legend" else "none") +  # Conditional legend
    labs(title = title_text, y = ylab, x = NULL) +  # Labels
    scale_x_discrete(drop = TRUE, expand = c(0, 0)) +  # Adjust x-axis spacing
    scale_y_continuous(position = "right") +
    theme_bw() +
    theme(
      strip.text = element_text(size = text_size),             # Facet label size
      axis.text.x = element_blank(),                    # Remove x-axis text
      axis.ticks.x = element_blank(),                   # Remove x-axis ticks
      axis.text = element_text(size = text_size * 0.8),             # Axis text size
      plot.title = element_text(hjust = 0.5)           # Center plot title
    )
}
