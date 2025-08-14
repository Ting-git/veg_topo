# plot Google satellite imagine by ext
plot_google_img <- function(ext_test) {
  api_key <- Sys.getenv("GOOGLE_API_KEY")
  register_google(key = api_key)

  earth_circumference <- 40075017  # 赤道周长（米）
  map_width_pixels <- 640 * 2  # Google Maps 默认高清尺寸（size=640, scale=2）
  region_width <- xmax(ext_test) - xmin(ext_test)  # 单位：度
  region_width_meters <- region_width * 111000

  required_zoom <- log2(earth_circumference / region_width_meters)
  zm <- ceiling(required_zoom)
  zm <- max(1, min(zm, 21))  # 限制范围

  bbox <- c(left = xmin(ext_test), bottom = ymin(ext_test), right = xmax(ext_test), top = ymax(ext_test))

  satellite_map <- get_map(location = bbox, source = "google", maptype = "satellite", zoom = zm)
  p <- ggmap(satellite_map)

  rm(satellite_map)
  gc()
  return(p)
}
