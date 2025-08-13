#' Plot vegetation height vs. TWI using hexbin and linear regression
#'
#' This function creates a hexbin plot of vegetation height (`vegh`)
#' against topographic wetness index (`twi`), with an optional linear regression line.
#'
#' @param df A data frame containing the input data.
#' @param x_var The name of the x-axis variable (default = "twi").
#' @param y_var The name of the y-axis variable (default = "vegh").
#' @param x_text X-axis label (default = "Topographic Wetness Index (TWI)").
#' @param y_text Y-axis label (default = "Vegetation Height (H)").
#' @param text_size Base text size for plot theme.
#'
#' @return A ggplot object.
plot_hex_vegh_twi <- function(df,
                                    x_var = "twi",
                                    y_var = "vegh",
                                    x_text = "Topographic Wetness Index (TWI)",
                                    y_text = "Vegetation Height (H)",
                                    text_size = 6) {

  ## use trans = "log" to achieve better visual balance of density
  ## and reverse = TRUE to have light colors for low density (more intuitive)
  p <- ggplot(df, aes(x = .data[[x_var]], y = .data[[y_var]])) +
    geom_hex(bins = 50, show.legend = FALSE) +
    khroma::scale_fill_batlowW(trans = "log", reverse = TRUE) +  # <- add '+' here
    geom_smooth(method = "lm", color = "red", linewidth = 0.5) +
    labs(
      title = NULL,
      x = x_text,
      y = y_text
    ) +
    ggplot2::theme_bw(base_size = text_size) +
    ggplot2::theme(
      axis.title = ggplot2::element_text(size = text_size),
      axis.text = ggplot2::element_text(size = text_size * 0.9),
      plot.title = ggplot2::element_text(size = text_size * 1.2, face = "bold"),
      plot.title.position = "panel"
    )

  return(p)
}
