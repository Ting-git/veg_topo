# ------create_spatial_windows------------------------------------


#' Create spatial windows from raster data
#'
#' Divides spatial data into regular windows and calculates window centroids.
#'
#' @param raster Input raster object
#' @param coord_vars names of coordinate variables (default: c("lon","lat"))
#' @param value_vars names of value variables (default: "lccs_class")
#' @return A data frame with nested data by spatial window
create_spatial_windows <- function(raster,
                                   coord_vars = c("lon", "lat"),
                                   value_vars = c("twi", "vegh"),
                                   dwin = 0.05) {

  # Convert raster to dataframe
  suppressWarnings({df <- as.data.frame(raster, xy = TRUE, na.rm = TRUE)})
  colnames(df) <- c(coord_vars, value_vars)

  # Create window boundaries
  lon_breaks <- seq(
    from = floor(min(df$lon)), to = ceiling(max(df$lon)), by = dwin)

  lat_breaks <- seq(
    from = floor(min(df$lat)), to = ceiling(max(df$lat)), by = dwin)

  # Create window variables (lon_mid, lat_mid)
  df_win <- df |>
    ungroup() |>
    mutate(ilon = cut(lon, breaks = lon_breaks),
           ilat = cut(lat, breaks = lat_breaks)
    ) |>
    mutate(lon_lower = as.numeric(sub("\\((.+),.*", "\\1", ilon)),
           lon_upper = as.numeric(sub("[^,]*,([^]]*)\\]", "\\1", ilon)),
           lat_lower = as.numeric(sub("\\((.+),.*", "\\1", ilat)),
           lat_upper = as.numeric(sub("[^,]*,([^]]*)\\]", "\\1", ilat))
    ) |>
    mutate(lon_mid = (lon_lower + lon_upper)/2,
           lat_mid = (lat_lower + lat_upper)/2) |>

    ## create cell name to associate with climate input
    dplyr::select(-ilon, -ilat, -lon_lower, -lon_upper, -lat_lower, -lat_upper)

  return(df_win)
}
