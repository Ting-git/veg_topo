plot_google_img <- function(extent = NULL, title = "Google Satellite Map",
                            text_size = 12) {

  # ---- Register Google API key ----
  api_key <- Sys.getenv("GOOGLE_API_KEY")
  register_google(key = api_key)

  # ---- Extract extent boundaries ----
  xmin <- terra::xmin(extent)
  xmax <- terra::xmax(extent)
  ymin <- terra::ymin(extent)
  ymax <- terra::ymax(extent)

  # ---- Compute zoom ----
  earth_circumference <- 40075017
  region_width <- xmax - xmin
  region_width_meters <- region_width * 111000
  zoom_level <- ceiling(log2(earth_circumference / region_width_meters))
  zoom_level <- max(1, min(zoom_level, 21))

  # ---- Fetch map ----
  bbox <- c(left = xmin, bottom = ymin, right = xmax, top = ymax)
  satellite_map <- get_map(location = bbox, source = "google", maptype = "satellite", zoom = zoom_level)

  # ---- Crop to exact extent ----
  p <- ggmap(satellite_map) +

    coord_sf(xlim = c(xmin, xmax), ylim = c(ymin, ymax)) +
    labs(title = title, x = NULL, y = NULL) +
    theme_bw() +
    theme(
      axis.title = ggplot2::element_text(size = text_size),
      axis.text = ggplot2::element_text(size = text_size * 0.8),
      plot.title = ggplot2::element_text(size = text_size * 1.2, face = "plain",
                                         margin = margin(b = 3)),
      axis.text.x = ggplot2::element_text(
        size = text_size * 0.8,
        hjust = 0,
        vjust = 1,
        margin = margin(t = 2, b = 2),
      ),
      axis.text.y = ggplot2::element_text(
        size = text_size * 0.8,
        hjust = 0.5,
        vjust = 0.5,
        angle = 90,
        margin = margin(r = 0, l = 0)
      )
    )

  rm(satellite_map); gc()

  return(p)
}
