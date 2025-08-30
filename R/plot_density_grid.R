

# Function to create combined density plots with adjustable rows
plot_density_grid <- function(data, columns, nrow = 1, main_title = "Density distributions", width = 12, height = 4, save_path = NULL) {

  # Generate density plots without titles or individual y labels
  plot_list <- lapply(columns, function(col) {
    ggplot(data, aes_string(x = col)) +
      geom_density(fill = "#69b3a2",      # nicer fill color
                   color = "#1f3552",     # darker outline for contrast
                   alpha = 0.6,           # slightly transparent
                   size = 0.3) +
      theme_bw(base_size = 6) +           # clean theme with bigger font
      labs(title = NULL, y = NULL) +
      theme(
        panel.grid.major = element_line(color = "gray90"),
        panel.grid.minor = element_blank(),
        axis.title.y = element_blank()
      )
  })


  # Combine plots with adjustable number of rows and shared y-axis label
  combined_plot <- wrap_plots(plot_list, nrow = nrow) &
    plot_layout(guides = "collect") &
    theme(plot.margin = margin(5, 5, 5, 5)) &
    ylab("Density") &
    plot_annotation(title = main_title)

  # Save if path is provided
  if (!is.null(save_path)) {
    ggsave(
      filename = save_path,
      plot = combined_plot,
      width = width,
      height = height,
      dpi = 300,
      units = "in"
    )
  }

  return(combined_plot)
}

# Example usage
# cols <- c("ai", "cluster8c", "cor", "fused")
# combined_plot <- plot_density_grid(df, cols, nrow = 2, save_path = here::here("data/figures/03_4_kmeans_ds.png"))
