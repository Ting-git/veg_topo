plot_vegh_with_twi_in_window <- function(correlation_results, seed = 123) {
  set.seed(seed)

  valid_windows <- correlation_results |>
    filter(!is.na(correlation), n_obs >= 100)

  if (nrow(valid_windows) < 3) {
    stop("The number of available windows is less than 3, please check the data.")
  }

  selected_windows <- sample(valid_windows$window_id, 2)

  plots <- purrr::map(selected_windows, function(wid) {
    row <- valid_windows |> filter(window_id == wid)

    df <- row$data[[1]] |> filter(complete.cases(twi, vegh))

    corr <- round(row$correlation, 3)
    pval <- signif(row$cor_pval, 3)
    lon <- round(row$lon_mid, 4)
    lat <- round(row$lat_mid, 4)

    ggplot(df, aes(x = twi, y = vegh)) +
      geom_point(alpha = 0.6) +
      geom_smooth(method = "lm", color = "blue", linewidth = 1) +
      ggtitle(
        paste0("Window ", wid,
               "\nLon: ", lon, ", Lat: ", lat,
               "\nR = ", corr, ", p = ", pval)
      ) +
      labs(
        x = "Topographic Wetness Index (TWI)",
        y = "Vegetation Height (VEGH)"
      ) +
      theme_classic()


  })

  return(plots)
}
