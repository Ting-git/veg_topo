plot_density_by_cluster <- function(data, xvar, cluster_var, mean_df, mean_col, title,
                                    facet_ncol = 1, scales = "fixed") {
  ggplot(data, aes(x = .data[[xvar]], fill = .data[[cluster_var]])) +
    geom_density(alpha = 0.5) +
    scale_fill_manual(values = brewer.pal(12, "Paired"), name = "Cluster") +
    facet_wrap(as.formula(paste("~", cluster_var)), ncol = facet_ncol, scales = scales) +
    geom_vline(data = mean_df, aes(xintercept = .data[[mean_col]]),
               color = "red", linetype = "dashed") +
    theme_bw() +
    labs(title = title) +
    theme(
      strip.text = element_text(size = 12),
      axis.text = element_text(size = 10),
      plot.title = element_text(hjust = 0.5),
      legend.position = "none"
    )
}
