#' Plot a geospatial continuous variable (e.g., VEGH, TWI, Correlation, DEM) from a data frame or raster
#'
#' @description
#' Generates a map of a continuous geospatial variable using either a data frame with coordinates or a raster source.
#'
#' @param input Input data: a data frame (with `lon` and `lat`), a `SpatRaster` object, or a raster file path
#' @param fill_var Name of the variable to use for fill (e.g., "vegh", "twi")
#' @param title Plot title
#' @param fill_scale A ggplot2 fill scale (e.g., `scale_fill_scico()`)
#' @param extent Optional extent object created by `terra::ext()`. If NULL, it is inferred from input
#' @param x_breaks Number of breaks on the x-axis
#' @param y_breaks Number of breaks on the y-axis
#' @param text_size Base font size for all plot elements
#'
#' @return A ggplot2 object showing the spatial distribution of the selected variable
#' @export
plot_geovar <- function(
    input,
    fill_var = "vegh",
    title = "Vegetation Height 2020 (m)",
    fill_scale = scale_fill_scico(palette = "batlow", direction = -1),
    extent = NULL,
    x_breaks = 5,
    y_breaks = 5,
    text_size = 6
) {
  # Validate extent
  if (!is.null(extent) && !inherits(extent, "SpatExtent")) {
    stop("`extent` must be a `SpatExtent` object created by terra::ext().")
  }

  # Detect input type
  if (is.data.frame(input)) {
    input_type <- "data_frame"
  } else if (inherits(input, "SpatRaster")) {
    input_type <- "raster_data"
  } else if (is.character(input) && length(input) == 1 && file.exists(input)) {
    input <- terra::rast(input)
    input_type <- "file_path"
  } else {
    stop("Input must be a data frame, SpatRaster object, or valid raster file path.")
  }

  # Auto-detect extent
  if (is.null(extent)) {
    if (input_type == "data_frame") {
      if (!all(c("lon", "lat") %in% names(input))) {
        stop("Data frame input must contain 'lon' and 'lat' columns.")
      }
      extent <- terra::ext(range(input$lon), range(input$lat))
    } else {
      extent <- terra::ext(input)
    }
  }

  # Extract extent boundaries
  xmin <- extent$xmin
  xmax <- extent$xmax
  ymin <- extent$ymin
  ymax <- extent$ymax

  # Build plot
  if (input_type == "data_frame") {
    if (!(fill_var %in% names(input))) {
      stop(paste0("Column '", fill_var, "' not found in the input data frame."))
    }

    p <- ggplot(input, aes(x = lon, y = lat, fill = .data[[fill_var]])) +
      geom_tile()
  } else {
    # Try to match the layer by fill_var
    raster_layer_names <- names(input)
    matched_layer <- raster_layer_names[grepl(fill_var, raster_layer_names)]

    if (length(matched_layer) == 0) {
      stop(paste0("No raster layer found matching '", fill_var, "'. Available: ", paste(raster_layer_names, collapse = ", ")))
    } else if (length(matched_layer) > 1) {
      warning("Multiple layers matched. Using the first: ", matched_layer[1])
    }

    # Subset the layer
    input <- input[[matched_layer[1]]]
    layer_name <- names(input)[1]

    # Plot using tidyterra
    p <- ggplot() +
      tidyterra::geom_spatraster(data = input, aes(fill = .data[[layer_name]]), maxcell = Inf)
  }

  # Final styling
  p <- p +
    fill_scale +
    labs(
      title = title,
      x = "Longitude",
      y = "Latitude",
      fill = toupper(fill_var)
    ) +
    scale_x_continuous(
      limits = c(xmin, xmax),
      breaks = seq(xmin, xmax, by = x_breaks),
      expand = c(0, 0)
    ) +
    scale_y_continuous(
      limits = c(ymin, ymax),
      breaks = seq(ymin, ymax, by = y_breaks),
      expand = c(0, 0)
    ) +
    theme_bw(base_size = text_size) +
    theme(
      legend.position = "right",
      legend.text = element_text(size = text_size),
      legend.title = element_text(size = text_size),
      axis.title = element_text(size = text_size),
      axis.text = element_text(size = text_size * 0.9),
      plot.title = element_text(size = text_size * 1.2, face = "bold"),
      plot.title.position = "panel"
    ) +
    coord_fixed()

  return(p)
}

#'
#' #' Plot Topographic Wetness Index (TWI)
#' #'
#' #' @inheritParams plot_geovar
#' #' @export
#' plot_twi <- function(input, ...) {
#'   plot_geovar(
#'     input = input,
#'     fill_var = "twi",
#'     title = "Topographic Wetness Index (TWI)",
#'     fill_scale = scale_fill_scico(palette = "oslo", direction = -1),
#'     ...
#'   )
#' }
#'
#' #' Plot Vegetation Height (VEGH)
#' #'
#' #' @inheritParams plot_geovar
#' #' @export
#' plot_vegh <- function(input, ...) {
#'   plot_geovar(
#'     input = input,
#'     fill_var = "vegh",
#'     title = "Vegetation Height 2020 (m)",
#'     fill_scale = scale_fill_scico(palette = "batlow", direction = -1),
#'     ...
#'   )
#' }
#'
#' #' Plot Correlation Map
#' #'
#' #' @inheritParams plot_geovar
#' #' @export
#' plot_cor_vegh_twi <- function(input, ...) {
#'   plot_geovar(
#'     input = input,
#'     fill_var = "cor",
#'     title = "Correlation Coefficient Map (VEGH ~ TWI)",
#'     fill_scale = scale_fill_scico(
#'       palette = "bam",
#'       midpoint = 0,
#'       limits = c(min(df$correlation, na.rm = TRUE),
#'                  max(df$correlation, na.rm = TRUE)),
#'       name = expression(r[TWI,VEGH])
#'     ),
#'     ...
#'   )
#' }
