#' Plot the terrain effect, defined as the ratio of incident solar radiation on a rugged surface to that on a flat surface.
#'
#' @param input A SpatRaster object or raster file path
#' @param extent Optional terra::ext() extent.
#'   If provided and smaller than the input raster extent,
#'   the raster will be cropped to their intersecting area.
#' @param text_size Font size
#' @param x_step Number of x-axis breaks
#' @param y_step Number of y-axis breaks
#' @return A ggplot2 object
#' @export
plot_terrain_effect <- function(input, extent = NULL, title_text = "Radiation index",
                      text_size = 6, x_step = 5, y_step = 5) {


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
  } else {

    # crop the input raster if the plot area is smaller than the original raster
    area_in <- (xmax(input) - xmin(input)) * (ymax(input) - ymin(input))
    area_ex <- (xmax(extent) - xmin(extent)) * (ymax(extent) - ymin(extent))

    if (area_ex < area_in) {
      cropped <- terra::crop(input, extent)

      if (all(is.na(values(cropped)))) {
        stop("The specified extent does not intersect with the raster. No plot will be generated.")
      } else {
        input <- cropped
        message("Raster has been cropped to the intersecting area of the extent.")
      }
    }
  }

  xmin <- extent$xmin
  xmax <- extent$xmax
  ymin <- extent$ymin
  ymax <- extent$ymax

  p <- ggplot2::ggplot() +
    tidyterra::geom_spatraster(data = input, maxcell = Inf) +
    scico::scale_fill_scico(
      palette = "vik",
      direction = 1,
      # limits = c(-1, 1),
      # breaks = seq(-1, 1, by = 0.5),
      midpoint = 1,
      # name = "Ratio",
      na.value = NA
    ) +
    ggplot2::labs(
      title = title_text,
      x = "Longitude",
      y = "Latitude",
      fill = NULL
    ) +
    ggplot2::scale_x_continuous(
      limits = c(xmin, xmax),
      breaks = seq(xmin, xmax, by = x_step),
      expand = c(0, 0)
    ) +
    ggplot2::scale_y_continuous(
      limits = c(ymin, ymax),
      breaks = seq(ymin, ymax, by = y_step),
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
