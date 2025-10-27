# =============================================================================
# Function: plot_single_sample_location
# Description:
#   Plots a single geographic point on a map of its continent.
#   - Automatically determines the continent of the point.
#   - Sets map limits to the continent boundaries.
#   - Longitude and latitude axes show 30-degree intervals.
# Dependencies: ggplot2, sf, rnaturalearth
# =============================================================================

# library(sf)
# library(ggplot2)
# library(rnaturalearth)

# Helper: get bounding box of the continent containing the point

get_region_bounds <- function(lon, lat) {
  if (lon > -30 & lon < 40 & lat > 30 & lat < 70) {
    return(list(xlim = c(-15, 35), ylim = c(35, 60)))  # 欧洲
  } else if (lon < -30 & lon > -180 & lat > 10 & lat < 75) {
    return(list(xlim = c(-170, -50), ylim = c(10, 75)))  # 北美洲
  } else if (lon > -90 & lon < -30 & lat > -60 & lat < 15) {
    return(list(xlim = c(-90, -30), ylim = c(-60, 15)))  # 南美洲
  } else if (lon > -20 & lon < 60 & lat > -40 & lat < 40) {
    return(list(xlim = c(-20, 60), ylim = c(-40, 40)))  # 非洲
  } else if (lon > 60 & lon < 180 & lat > -50 & lat < 80) {
    return(list(xlim = c(60, 180), ylim = c(-50, 80)))  # 亚洲+大洋洲
  } else {
    return(list(xlim = c(-180, 180), ylim = c(-60, 85))) # 默认世界
  }
}


# Main function: plot a single point within its continent
plot_single_sample_location <- function(x, y, tile_id, text_size = 12) {
  # Load world map
  world <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")

  # Get display extent based on coordinates
  bounds <- get_region_bounds(x, y)

  # Create tile dataframe
  tile <- data.frame(xmid = x, ymid = y)

  # Compute tick breaks
  x_breaks <- seq(floor(bounds$xlim[1] / 30) * 30, ceiling(bounds$xlim[2] / 30) * 30, by = 30)
  y_breaks <- seq(floor(bounds$ylim[1] / 30) * 30, ceiling(bounds$ylim[2] / 30) * 30, by = 30)

  # Plot
  p <- ggplot2::ggplot(data = world) +
    ggplot2::geom_sf(fill = "gray95", color = "gray70") +
    ggplot2::geom_point(data = tile, aes(x = xmid, y = ymid), color = "red", size = 3) +
    ggplot2::coord_sf(xlim = bounds$xlim, ylim = bounds$ylim, expand = FALSE) +
    # ggplot2::scale_x_continuous(breaks = x_breaks, limits = bounds$xlim, expand = c(0,0)) +
    # ggplot2::scale_y_continuous(breaks = y_breaks, limits = bounds$ylim, expand = c(0,0)) +
    ggplot2::labs(title = paste("Location of", tile_id), x = "Longitude", y = "Latitude") +
    ggplot2::theme_bw(base_size = text_size) +
    ggplot2::theme(
      legend.position = "none",
      axis.title = ggplot2::element_text(size = text_size),
      axis.text = ggplot2::element_text(size = text_size*0.9),
      plot.title = ggplot2::element_text(size = text_size*1.2, face = "bold"),
      plot.title.position = "panel",
      panel.grid.major = ggplot2::element_line(color = "gray80", linewidth = 0.5),
      panel.grid.minor = ggplot2::element_line(color = "gray90", linewidth = 0.25)
    )

  return(p)
}


