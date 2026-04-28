get_lonlat_extent <- function(r, n = 50) {
  e <- ext(r)

  # 修改：创建更多采样点（原来是4个角点）
  x_pts <- seq(e$xmin, e$xmax, length.out = n)
  y_pts <- seq(e$ymin, e$ymax, length.out = n)

  # 创建网格点而不是只有角点
  pts <- expand.grid(x = x_pts, y = y_pts)

  # 转换为 sf
  sf_pts <- sf::st_as_sf(pts, coords = c("x", "y"), crs = crs(r))
  sf_pts_wgs84 <- sf::st_transform(sf_pts, crs = 4326)

  coords <- sf::st_coordinates(sf_pts_wgs84)

  return(ext(min(coords[,1]), max(coords[,1]),
             min(coords[,2]), max(coords[,2])))
}
