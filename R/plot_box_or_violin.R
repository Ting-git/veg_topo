plot_box_or_violin <- function(data, xvar, yvar, type = "boxplot", ylab = NULL, fill_colors = NULL) {

  # data[[xvar]] <- factor(data[[xvar]], levels = unique(data[[xvar]]))
  data[[xvar]] <- factor(data[[xvar]], levels = sort(unique(data[[xvar]])))

  geom_fun <- if (type == "boxplot") geom_boxplot(width = 0.9, size = 0.5) else geom_violin(width = 0.9, size = 0.5)


  title_text <- if (yvar == "cor" && type == "boxplot") "Boxplot" else
    if (yvar == "cor" && type == "violin") "Violin Distribution" else NULL

  ggplot(data, aes(x = .data[[xvar]], y = .data[[yvar]], fill = .data[[xvar]])) +
    geom_fun +
    list(if (yvar == "cor") geom_hline(yintercept = 0, linetype = "dashed", color = "red")) +
    scale_fill_manual(values = fill_colors, name = "Cluster") +
    labs(
      title = title_text,
      y = ylab,
      x = NULL
    ) +
    scale_x_discrete(drop = TRUE, expand = c(0, 0)) +
    theme_bw() +
    theme(
      strip.text = element_text(size = 12),
      axis.text = element_text(size = 10),
      plot.title = element_text(hjust = 0.5),
      legend.position = "none"
    )
}
