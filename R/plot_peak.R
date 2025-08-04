plot_peak <- function(correlation_df_peak) {
  ggplot(correlation_df_peak, aes(x = lon_mid, y = lat_mid, fill = factor(peak))) +
    geom_tile() +
    scale_fill_manual(
      values = c("0" = "lightblue", "1" = "darkred", "NA" = "grey"),
      na.translate = TRUE,
      name = "Peak"
    ) +
    coord_equal() +
    theme_classic() +
    labs(title = "Peak Distribution", x = "Lontitude", y = "Latitut")
}
