#' Extract a legend grob from a raster using a dummy raster
#'
#' This function generates a legend for a SpatRaster quickly without rendering
#' the full raster by creating a small dummy raster that spans the raster's value range.
#'
#' @param raster_input A SpatRaster object
#' @param palette Name of the scico palette (default: "bam")
#' @param name Legend title (default: "Legend")
#' @param n_breaks Number of breaks on the legend (default: 5)
#' @param dummy_size Number of rows/columns for the dummy raster (default: 10)
#' @return A grob object of the legend
#' @export
extract_legend_dummy <- function(input,
                                 extent = NULL,
                                 palette = "bam",
                                 name = "Legend",
                                 midpoint = NULL,
                                 n_breaks = 5,
                                 dummy_size = 10) {
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

  # Compute raster min/max
  vmin <- terra::global(input, "min", na.rm = TRUE)[1,1]
  vmax <- terra::global(input, "max", na.rm = TRUE)[1,1]

  # Create small dummy raster spanning the value range
  dummy <- terra::rast(nrows = dummy_size, ncols = dummy_size)
  values(dummy) <- seq(vmin, vmax, length.out = dummy_size^2)

  # Build ggplot legend using dummy raster
  if (is.null(midpoint)) {
    p_legend <- ggplot2::ggplot() +
      tidyterra::geom_spatraster(data = dummy, maxcell = Inf) +
      scico::scale_fill_scico(
        palette = palette,
        limits = c(vmin, vmax),
        name = name,
        breaks = pretty(c(vmin, vmax), n = n_breaks),
        guide = ggplot2::guide_colorbar(
          barheight = grid::unit(1, "npc"),
          draw.ulim = TRUE,
          draw.llim = TRUE
        )
      )
  } else {
    p_legend <- ggplot2::ggplot() +
      tidyterra::geom_spatraster(data = dummy, maxcell = Inf) +
      scico::scale_fill_scico(
        palette = palette,
        limits = c(vmin, vmax),
        midpoint = midpoint,
        name = name,
        breaks = pretty(c(vmin, vmax), n = n_breaks),
        guide = ggplot2::guide_colorbar(
          barheight = grid::unit(1, "npc"),
          draw.ulim = TRUE,
          draw.llim = TRUE
        )
      )
  }

  # Extract and return the legend grob
  cowplot::get_legend(p_legend)
}


#' Extract legend for TWI raster
#' @param input A SpatRaster object
#' @return A grob object of the TWI legend
#' @export
extract_legend_twi <- function(input, extent = NULL) {
  extract_legend_dummy(
    input = input,
    extent = extent,
    palette = "oslo",
    name = "TWI",
    n_breaks = 5,
    dummy_size = 10
  )
}

#' Extract legend for Vegetation Height (VEGH) raster
#' @param input A SpatRaster object
#' @return A grob object of the VEGH legend
#' @export
extract_legend_vegh <- function(input, extent = NULL) {
  extract_legend_dummy(
    input = input,
    extent = extent,
    palette = "batlow",
    name = "VEGH",
    n_breaks = 5,
    dummy_size = 10
  )
}

#' Extract legend for correlation raster (r(H, TWI))
#' @param input A SpatRaster object
#' @return A grob object of the correlation legend
#' @export
extract_legend_cor <- function(input, extent = NULL) {
  extract_legend_dummy(
    input = input,
    extent = extent,
    palette = "bam",
    midpoint = 0,
    name = expression(r[H*","*TWI]),
    n_breaks = 5,
    dummy_size = 10
  )
}
