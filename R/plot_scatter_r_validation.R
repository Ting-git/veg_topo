plot_scatter_r_validation <- function(r_30_5000,
                                      r_450_5000,
                                      text_size = 6,
                                      title_text = "Comparison r",
                                      x_text = expression(r[30]),
                                      y_text = expression(r[450]),
                                      add_lm = FALSE) {

  # ---- Load raster ----
  if (is.character(r_30_5000)) r_30_5000 <- terra::rast(r_30_5000)
  if (!inherits(r_30_5000, "SpatRaster")) stop("r_30_5000 must be a SpatRaster or valid file path.")

  if (is.character(r_450_5000)) r_450_5000 <- terra::rast(r_450_5000)
  if (!inherits(r_450_5000, "SpatRaster")) stop("r_450_5000 must be a SpatRaster or valid file path.")

  # ---- Handle extent and resampling ----
  r_450_5000 <- terra::resample(r_450_5000, r_30_5000, method = "near")

  # ---- Create dataframe ----
  stacked <- c(r_30_5000, r_450_5000)
  df <- as.data.frame(stacked, xy = FALSE, na.rm = FALSE)
  colnames(df) <- c("r_30_5000", "r_450_5000")
  df_clean <- na.omit(df)  # 移除NA值

  df_clean
  # ---- Create plot ----
  p <- ggplot2::ggplot(df_clean) +
    # 1:1参考线 (红色实线)
    ggplot2::geom_abline(intercept = 0, slope = 1,
                         color = "red",
                         linetype = "dashed",
                         linewidth = 0.5) +
    # 散点
    ggplot2::geom_point(ggplot2::aes(x = r_30_5000, y = r_450_5000),
                        alpha = 0.5,
                        size = 0.8) +
    # 标签
    ggplot2::labs(
      title = title_text,
      x = x_text,
      y = y_text
    ) +
    ggplot2::theme_bw(base_size = text_size) +
    ggplot2::theme(
      legend.position = "none",
      axis.title = ggplot2::element_text(size = text_size),
      axis.text = ggplot2::element_text(size = text_size * 0.9),
      plot.title = ggplot2::element_text(size = text_size * 1.2, face = "bold",
                                         margin = ggplot2::margin(b = 3)),
      plot.title.position = "panel"
    )

  # 添加回归线（如果需要）
  if (add_lm) {
    p <- p +
      ggplot2::geom_smooth(ggplot2::aes(x = r_30_5000, y = r_450_5000),
                           method = "lm",
                           color = "black",
                           linetype = "dashed",
                           linewidth = 0.5,
                           se = FALSE)
  }

  return(p)
}
