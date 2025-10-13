# library(ggplot2)
# library(sf)
# library(rnaturalearth)

# helper function to define display extend
get_region_bounds <- function(lon, lat) {
  if (lon > -30 & lon < 40 & lat > 30 & lat < 70) {
    return(list(xlim = c(-15, 35), ylim = c(35, 60)))  # 欧洲
  } else if (lon < -30 & lon > -180 & lat > 10 & lat < 75) {
    return(list(xlim = c(-170, -50), ylim = c(10, 75)))  # 北美洲
  } else if (lon > -20 & lon < 60 & lat > -40 & lat < 40) {
    return(list(xlim = c(-20, 60), ylim = c(-40, 40)))  # 非洲
  } else if (lon > 60 & lon < 180 & lat > -50 & lat < 80) {
    return(list(xlim = c(60, 180), ylim = c(-50, 80)))  # 亚洲+大洋洲
  } else {
    return(list(xlim = c(-180, 180), ylim = c(-60, 85))) # 默认世界
  }
}

plot_single_sample_location <- function(x, y, tile_id, text_size = 6) {

  # load word map
  world <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")

  # get display extend
  bounds <- get_region_bounds(x, y)

  # create dataframe
  tile <- data.frame(xmid = x, ymid = y)

  # plot
  p <- ggplot2::ggplot(data = world) +
    ggplot2::geom_sf(fill = "gray95", color = "gray70") +
    ggplot2::geom_point(data = tile, aes(x = xmid, y = ymid), color = "red", size = 3) +
    ggplot2::coord_sf(
      xlim = bounds$xlim,
      ylim = bounds$ylim,
      expand = FALSE
    ) +
    ggplot2::labs(
      title = paste("Location of", tile_id),
      x = "Longitude",
      y = "Latitude",
      fill = NULL
    ) +
    ggplot2::theme_bw(base_size = text_size) +
    ggplot2::theme(
      # legend.position = "right",
      # legend.text = ggplot2::element_text(size = text_size),
      # legend.title = ggplot2::element_text(size = text_size, face = "bold"),
      axis.title = ggplot2::element_text(size = text_size),
      axis.text = ggplot2::element_text(size = text_size * 0.9),
      plot.title = ggplot2::element_text(size = text_size * 1.2, face = "bold"),
      plot.title.position = "panel"
    )

  return(p)
}
