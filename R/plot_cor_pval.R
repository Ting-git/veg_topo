#' Plot Correlation between VEGH and TWI
#'
#' @param input A SpatRaster or raster file path
#' @param extent Optional terra::ext() extent
#' @param text_size Font size
#' @param x_breaks Number of x-axis breaks
#' @param y_breaks Number of y-axis breaks
#' @return A ggplot2 object
#' @export
plot_cor_pval <- function(input, extent = NULL, title_text = "VEGH–TWI Pearson Correlation: P-value Map", text_size = 14, x_breaks = 30, y_breaks = 30) {

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
    scale_fill_stepsn(
      colours = c("#2166AC", "#67A9CF", "#D1E5F0", "#FDDBC7", "#EF8A62", "#B2182B"),
      values = scales::rescale(c(0, 0.01, 0.05, 0.1, 0.5, 1)),  # 分段值（rescaled to 0–1）
      breaks = c(0.01, 0.05, 0.1, 0.5, 1),
      limits = c(0, 1),
      oob = scales::squish,
      name = "p",
      na.value = NA,
      guide = guide_colorbar(
        direction = "horizontal",
        title.position = "top",
        label.position = "bottom",
        barwidth = unit(12, "cm"),
        barheight = unit(0.5, "cm")
      )
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
      legend.position = "bottom",
      legend.text = element_text(size = text_size, angle = 45),
      legend.title = ggplot2::element_text(size = text_size),
      axis.title = ggplot2::element_text(size = text_size),
      axis.text = ggplot2::element_text(size = text_size * 0.9),
      plot.title = ggplot2::element_text(size = text_size * 1.2, face = "bold"),
      plot.title.position = "panel"
    )

  return(p)
}
