plot_aspect <- function(input, extent = NULL, title_text =  "Aspect (°)",
                       text_size = 12, x_step = 10, y_step = 10) {

  if (is.character(input)) {
    input <- terra::rast(input)
  }

  if (!inherits(input, "SpatRaster")) {
    stop("Input must be a SpatRaster object or a valid raster file path.")
  }

  # Handle extent and cropping
  if (is.null(extent)) {
    extent <- terra::ext(input)
  } else if (!inherits(extent, "SpatExtent")) {
    stop("`extent` must be a SpatExtent object created by terra::ext().")
  } else {
    # crop if extent smaller than raster
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


  # Define a smooth circular color palette
  my_colors <- c("navy", "#88ccee", "white", "#ff8888", "red", "#ff8888", "white", "#88ccee", "navy")
  my_values <- seq(0, 360, length.out = length(my_colors))/360  # normalize 0-360 to 0-1

  p <- ggplot2::ggplot() +
    tidyterra::geom_spatraster(data = input, maxcell = Inf) +
    scale_fill_gradientn(
      colors = my_colors,
      values = my_values,
      limits = c(0, 360),
      oob = scales::squish
    ) +
    guides(fill = guide_colorbar(barwidth = 0.8, barheight = 6)) +
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
      legend.text = ggplot2::element_text(size = text_size * 0.9),
      legend.title = ggplot2::element_text(size = text_size),
      axis.title = ggplot2::element_text(size = text_size),
      axis.text = ggplot2::element_text(size = text_size  * 0.9),
      plot.title = ggplot2::element_text(size = text_size * 1.2, face = "bold"),
      plot.title.position = "panel"
    )

  return(p)
}
