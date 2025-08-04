plot_vegh_with_twi_bydf <- function(df, text_size = 6) {

  p <- ggplot(df, aes(x = twi, y = vegh)) +
    geom_hex(bins = 50) +  # 调整 bins 以控制六边形大小
    scale_fill_scico(palette = "batlow", name = "Pixel Count") +
    geom_smooth(method = "lm", color = "blue", linewidth = 1) +
    labs(
      title = NULL,
      x = "Topographic Wetness Index (TWI)",
      y = "Vegetation Height (H)"
    ) +
    ggplot2::theme_bw(base_size = text_size) +
    ggplot2::theme(
      legend.position = "right",
      legend.text = ggplot2::element_text(size = text_size),
      legend.title = ggplot2::element_text(size = text_size),
      axis.title = ggplot2::element_text(size = text_size),
      axis.text = ggplot2::element_text(size = text_size * 0.9),
      plot.title = ggplot2::element_text(size = text_size * 1.2, face = "bold"),
      plot.title.position = "panel"
    )


  return(p)
}
