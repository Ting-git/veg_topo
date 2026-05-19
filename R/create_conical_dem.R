create_conical_dem <- function(center_lon, center_lat, slope_angle_deg,
                               output_file = NULL, window_size = 0.1) {

  # Convert slope angle to elevation (meters)
  slope_rate <- tan(slope_angle_deg * pi / 180)
  res <- 15 / 3600  # degrees

  n <- floor(window_size / res)
  if(n %% 2 == 0) n <- n + 1
  half <- (n - 1) * res / 2  # degrees

  lon <- seq(center_lon - half, center_lon + half, length.out = n)
  lat <- seq(center_lat - half, center_lat + half, length.out = n)

  # Convert horizontal distance from degrees to meters
  # 1 degree ≈ 111320 meters (near equator)
  meters_per_deg <- 111320 * cos(center_lat * pi / 180)
  half_meters <- half * meters_per_deg

  lon_mat <- matrix(lon, n, n, byrow = TRUE)
  lat_mat <- matrix(lat, n, n, byrow = FALSE)

  # Distance (meters)
  dist_meters <- sqrt((lon_mat - center_lon)^2 * meters_per_deg^2 +
                        (lat_mat - center_lat)^2 * meters_per_deg^2)

  # Elevation (meters)
  max_height_meters <- slope_rate * half_meters
  elev <- max_height_meters - slope_rate * dist_meters
  elev[elev < 0] <- 0

  # Create raster
  r <- rast(nrows = n, ncols = n,
            xmin = min(lon), xmax = max(lon),
            ymin = min(lat), ymax = max(lat),
            crs = "EPSG:4326")
  values(r) <- as.vector(elev)

  if(!is.null(output_file)) writeRaster(r, output_file, overwrite = TRUE)
  return(r)
}

# # Test
# dem <- create_conical_dem(0, 0, 15)
# summary(dem)  # Elevation range should be 0 ~ about 670 meters
#
# slope <- terrain(dem, "slope", unit = "degrees")
# summary(slope)  # Should be close to 15 degrees
