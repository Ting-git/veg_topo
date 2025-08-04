# plot correlation (mark NA) with window size (how many pixels in it)
plot_cor_with_n_pixel <- function(correlation_df) {

  # data clean
  df <- correlation_df |>
    dplyr::select(n_obs, correlation)

  # Separate the rows where the correlation is NA
  na_data <- df |> filter(is.na(correlation))
  data_valid <- df |> filter(!is.na(correlation))

  ggplot() +
    # none NA point density
    geom_pointdensity(data = data_valid, aes(x = n_obs, y = correlation), adjust = 1.5) +
    scale_color_viridis_c() +

    # Red Cross NA Point
    geom_point(data = na_data, aes(x = n_obs, y = 0),
               shape = 4, color = "red", size = 3, stroke = 1.2) +

    labs(
      title = "Density-colored Scatter Plot \n(with NA correlations shown)",
      x = "Pixel Count per Window",
      y = "Correlation Coefficient",
      color = "Local Density of Window"
    ) +
    theme_classic() +
    theme(legend.position = "right",
          axis.title = element_text(size = 12),
          plot.title = element_text(size = 14, face = "bold"))
}
