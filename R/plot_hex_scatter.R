plot_hex_scatter <- function(df,
                             x_var = "twi",
                             y_var = "vegh",
                             x_text = "Topographic Wetness Index",
                             y_text = "Vegetation Height (m)",
                             text_size = 6,
                             title_text = "TWI vs H",
                             bins = 50,
                             add_lm = TRUE ) {

  # Keep only the x_var and y_var columns, remove NA rows
  df <- df[, c(x_var, y_var)]
  df <- df[complete.cases(df), ]

  # ---- Compute hex bin counts ----
  hex_data <- ggplot2::ggplot_build(
    ggplot(df, aes(x = .data[[x_var]], y = .data[[y_var]])) +
      geom_hex(bins = bins)
  )$data[[1]]

  # Determine min/max counts
  min_count <- 1  # log scale cannot have 0
  max_count <- max(hex_data$count, na.rm = TRUE)

  # Compute powers of 10 in range
  log_min <- floor(log10(min_count))
  log_max <- ceiling(log10(max_count))
  legend_breaks <- 10^(log_min:log_max)

  # ---- Basic plot ----
  p <- ggplot(df, aes(x = .data[[x_var]], y = .data[[y_var]])) +
    geom_hex(bins = bins, show.legend = TRUE) +
    khroma::scale_fill_batlowW(
      trans = "log",
      reverse = TRUE,
      breaks = legend_breaks,
      labels = scales::math_format(10^.x)
    ) +
    guides(fill = guide_colorbar(barwidth = 0.8, barheight = 6)) +
    labs(
      title = title_text,
      x = x_text,
      y = y_text,
      fill = NULL
    ) +
    theme_bw(base_size = text_size) +
    theme(
      legend.position = "right",
      legend.text = ggplot2::element_text(size = text_size * 0.9),
      legend.title = ggplot2::element_text(size = text_size),
      axis.title = ggplot2::element_text(size = text_size),
      axis.text = ggplot2::element_text(size = text_size  * 0.9),
      plot.title = ggplot2::element_text(size = text_size * 1.2, face = "bold"),
      plot.title.position = "panel"
    )

  # Optional linear regression line
  if (add_lm) {
    p <- p + geom_smooth(method = "lm", color = "red", linewidth = 0.5)
  }

  return(p)
}
