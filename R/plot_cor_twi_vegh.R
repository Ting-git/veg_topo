#' Plot Correlation between VEGH and TWI
#'
#' @param input A SpatRaster or raster file path
#' @param extent Optional terra::ext() extent
#' @param text_size Font size
#' @param x_breaks Number of x-axis breaks
#' @param y_breaks Number of y-axis breaks
#' @return A ggplot2 object
#' @export
plot_cor_twi_vegh <- function(input, extent = NULL, title_text = "r(H ~ TWI)", text_size = 6, x_breaks = 5, y_breaks = 5) {

  if (is.character(input)) {
    input <- terra::rast(input)
  }

  if (!inherits(input, "SpatRaster")) {
    stop("Input must be a SpatRaster object or a valid raster file path.")
  }

  if (is.null(extent)) {
    extent <- terra::ext(input)
  } else if (!inherits(extent, "SpatExtent")) {
    stop("`extent` must be a SpatExtent object created by terra::ext().")
  }

  xmin <- extent$xmin
  xmax <- extent$xmax
  ymin <- extent$ymin
  ymax <- extent$ymax

  # Estimate value range for diverging color scale
  vmin <- terra::global(input, "min", na.rm = TRUE)[1, 1] |> as.numeric()
  vmax <- terra::global(input, "max", na.rm = TRUE)[1, 1] |> as.numeric()

  p <- ggplot2::ggplot() +
    tidyterra::geom_spatraster(data = input, maxcell = Inf) +
    scico::scale_fill_scico(
      palette = "bam",
      limits = c(vmin, vmax),
      midpoint = 0,
      name = expression(r[H*","*TWI]),
      na.value = NA
    ) +
    ggplot2::labs(
      title = title_text,
      x = "Longitude",
      y = "Latitude"
    ) +
    ggplot2::scale_x_continuous(
      limits = c(xmin, xmax),
      breaks = seq(xmin, xmax, by = x_breaks),
      expand = c(0, 0)
    ) +
    ggplot2::scale_y_continuous(
      limits = c(ymin, ymax),
      breaks = seq(ymin, ymax, by = y_breaks),
      expand = c(0, 0)
    ) +
    ggplot2::theme_bw(base_size = text_size) +
    ggplot2::theme(
      legend.position = "right",
      legend.text = ggplot2::element_text(size = text_size),
      legend.title = ggplot2::element_text(size = text_size, face = "bold"),
      axis.title = ggplot2::element_text(size = text_size),
      axis.text = ggplot2::element_text(size = text_size * 0.9),
      plot.title = ggplot2::element_text(size = text_size * 1.2, face = "bold"),
      plot.title.position = "panel"
    )

  return(p)
}
