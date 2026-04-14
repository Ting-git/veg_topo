# Fast function to get lon/lat extent
get_lonlat_extent <- function(r) {

  # Get UTM extent
  e <- ext(r)

  # Create corner points
  corners <- matrix(c(e[1], e[3],  # bottom-left
                      e[2], e[3],  # bottom-right
                      e[2], e[4],  # top-right
                      e[1], e[4],  # top-left
                      e[1], e[3]), # close polygon
                    ncol = 2, byrow = TRUE)

  # Create polygon and transform
  poly_utm <- st_polygon(list(corners))
  sf_utm <- st_sfc(poly_utm, crs = crs(r))
  sf_wgs84 <- st_transform(sf_utm, crs = 4326)

  # Extract bounding box
  bbox <- st_bbox(sf_wgs84)

  # Return as extent object
  return(ext(bbox["xmin"], bbox["xmax"], bbox["ymin"], bbox["ymax"]))
}
