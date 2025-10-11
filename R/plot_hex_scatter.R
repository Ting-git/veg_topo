plot_hex_scatter <- function(df,
                             x_var = "twi",
                             y_var = "vegh",
                             x_text = "Topographic Wetness Index (TWI, unitless)",
                             y_text = "Vegetation Height (H, m)",
                             text_size = 6,
                             bins = 50,
                             add_lm = TRUE) {
  # basic elements
  p <- ggplot(df, aes(x = .data[[x_var]], y = .data[[y_var]])) +
    geom_hex(bins = bins, show.legend = TRUE) +
    khroma::scale_fill_batlowW(trans = "log", reverse = TRUE) +
    labs(
      title = NULL,
      x = x_text,
      y = y_text,
      fill = "Density"
    ) +
    theme_bw(base_size = text_size) +
    theme(
      axis.title = element_text(size = text_size),
      axis.text = element_text(size = text_size * 0.9),
      plot.title = element_text(size = text_size * 1.2, face = "bold"),
      plot.title.position = "panel",
      legend.position = "right"
    )

  # opptional
  if (add_lm) {
    p <- p + geom_smooth(method = "lm", color = "red", linewidth = 0.5)
  }

  return(p)
}
