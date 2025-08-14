plot_n_pixel_bywin <- function(windowed_data) {
  window_counts <- windowed_data |>
    group_by(window_id) |>
    summarise(pixel_count = n(), .groups = "drop")

  # 计算 144 的比例
  pct_144 <- mean(window_counts$pixel_count == 144) * 100

  # 绘图
  p <- ggplot(window_counts, aes(x = pixel_count)) +
    geom_histogram(binwidth = 5, fill = "skyblue", color = "white", alpha = 0.8) +
    labs(
      title = "Distribution of Pixel Counts Across Windows",
      x = "Number of Pixels per Window",
      y = "Frequency (Number of Windows)"
    ) +
    theme_classic()

  # 标注 144 处的占比
  if (pct_144 > 80) {
    p <- p + annotate("text", x = 144, y = max(table(window_counts$pixel_count)),
                      label = paste0(round(pct_144, 1), "% at 144"),
                      color = "red", vjust = -1.5)
  }

  return(p)
}
