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
plot_box_or_violin <- function(data, xvar, yvar, type = "boxplot", ylab = NULL, fill_colors = NULL, show_legend = FALSE) {

  # Choose geom type based on 'type' argument
  geom_fun <- if (type == "boxplot") geom_boxplot(width = 0.9, linewidth = 0.5) else geom_violin(width = 0.9, linewidth = 0.5)

  # Optional plot title for correlation variable
  title_text <- if (yvar == "cor" && type == "boxplot") "Boxplot" else
    if (yvar == "cor" && type == "violin") "Violin Distribution" else NULL

  ggplot(data, aes(x = .data[[xvar]], y = .data[[yvar]], fill = .data[[xvar]])) +
    geom_fun +  # Add the chosen geom
    list(if (yvar == "cor") geom_hline(yintercept = 0, linetype = "dashed", color = "red")) +  # Optional reference line for correlation
    scale_fill_manual(values = fill_colors, name = "Cluster", guide = if (show_legend) "legend" else "none") +  # Conditional legend
    labs(title = title_text, y = ylab, x = NULL) +  # Labels
    scale_x_discrete(drop = TRUE, expand = c(0, 0)) +  # Adjust x-axis spacing
    theme_bw() +
    theme(
      strip.text = element_text(size = 12),             # Facet label size
      axis.text.x = element_blank(),                    # Remove x-axis text
      axis.ticks.x = element_blank(),                   # Remove x-axis ticks
      axis.text = element_text(size = 10),             # Axis text size
      plot.title = element_text(hjust = 0.5)           # Center plot title
    )
}
