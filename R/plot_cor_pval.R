#' Plot correlation p-values as categorized raster
#'
#' @param input SpatRaster object or path to raster file containing p-values
#' @param extent Optional SpatExtent to crop the raster
#' @param title_text Plot title
#' @param text_size Base font size
#' @param x_step Interval for x-axis ticks
#' @param y_step Interval for y-axis ticks
#' @return ggplot2 object
#' @export

plot_cor_pval <- function(input, extent = NULL,
                          title_text = "Pearson's p-value (H~TWI)",
                          text_size = 12, x_step = 10, y_step = 10) {

  # ---- Load raster ----
  if (is.character(input)) input <- terra::rast(input)
  if (!inherits(input, "SpatRaster")) stop("Input must be a SpatRaster or valid file path.")

  # ---- Handle extent and optional cropping ----
  if (is.null(extent)) {
    extent <- terra::ext(input)
  } else if (!inherits(extent, "SpatExtent")) {
    stop("`extent` must be a SpatExtent object from terra::ext().")
  } else {
    # Crop raster if extent is smaller
    area_in <- (terra::xmax(input) - terra::xmin(input)) * (terra::ymax(input) - terra::ymin(input))
    area_ex <- (terra::xmax(extent) - terra::xmin(extent)) * (terra::ymax(extent) - terra::ymin(extent))

    if (area_ex < area_in) {
      cropped <- terra::crop(input, extent)
      if (all(is.na(terra::values(cropped)))) stop("Extent does not intersect raster.")
      input <- cropped
      message("Raster cropped to specified extent.")
    }
  }

  # ---- Extract extent boundaries ----
  xmin <- terra::xmin(extent)
  xmax <- terra::xmax(extent)
  ymin <- terra::ymin(extent)
  ymax <- terra::ymax(extent)

  # ---- Classify p-values into discrete bins ----
  breaks <- c(-Inf, 0.001, 0.01, 0.05, 0.1, Inf)
  labels <- c("<0.001", "0.001–0.01", "0.01–0.05", "0.05–0.1", "≥0.1")
  rcl <- cbind(breaks[-length(breaks)], breaks[-1], 1:5)
  input_class <- terra::classify(input, rcl = rcl)
  names(input_class) <- "class"

  # ---- convert to factor ----
  input_class <- terra::as.factor(input_class)
  levels(input_class)[[1]] <- data.frame(ID = 1:5, label = labels)

  # ---- Plot using tidyterra ----
  p <- ggplot() +
    tidyterra::geom_spatraster(
      data = input_class,
      aes(fill = label),
      maxcell = Inf
    ) +
    scale_fill_manual(
      values = rev(RColorBrewer::brewer.pal(5, "RdYlBu")),
      labels = labels,
      na.value = NA,
      drop = FALSE,
      guide = guide_legend(keywidth = 0.8, keyheight = 1)
    ) +
    labs(
      title = title_text,
      x = "Longitude",
      y = "Latitude",
      fill = NULL
    ) +
    scale_x_continuous(
      limits = c(xmin, xmax),
      breaks = seq(xmin, xmax, by = x_step),
      expand = c(0, 0)
    ) +
    scale_y_continuous(
      limits = c(ymin, ymax),
      breaks = seq(ymin, ymax, by = y_step),
      expand = c(0, 0)
    ) +
    theme_bw(base_size = text_size) +
    theme(
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
