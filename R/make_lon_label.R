# Helper functions for naming (fixed-width format)
make_lon_label <- function(lon) {
  lon_abs <- abs(lon)
  # Longitude uses 3 digits, zero-padded
  lon_formatted <- sprintf("%03d", round(lon_abs))
  ifelse(lon < 0, paste0(lon_formatted, "W"), paste0(lon_formatted, "E"))
}

make_lat_label <- function(lat) {
  lat_abs <- abs(lat)
  # Latitude uses 2 digits, zero-padded
  lat_formatted <- sprintf("%02d", round(lat_abs))
  ifelse(lat < 0, paste0(lat_formatted, "S"), paste0(lat_formatted, "N"))
}
