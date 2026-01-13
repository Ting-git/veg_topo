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


# ------calculate_window_correlations------------------------------------

#' Calculate windowed correlation statistics
#'
#' Computes correlation statistics for nested spatial windows.
#'
#' @param window_data Data frame from create_spatial_windows()
#' @param x Character, name of first variable (default "twi")
#' @param y Character, name of second variable (default "vegh")
#' @param if_nobs Logical, include observation counts? (default TRUE)
#' @param if_pval Logical, include p-values? (default TRUE)
#' @param if_data Logical, keep raw data? (default FALSE)
#' @param if_peak Logical, calculate peak relationships? (default FALSE)
#' @return A data frame with correlation statistics by window
calculate_window_correlations <- function(df_win,
                                          x = "twi",
                                          y = "vegh",
                                          if_nobs = TRUE,
                                          if_pval = TRUE,
                                          if_data = FALSE,
                                          if_peak = FALSE) {

  df_cor <- df_win |>

    group_by(lon_mid, lat_mid) |>
    tidyr::nest() |>

    mutate(
      # Perform statistical computations
      stats = purrr::map(data, ~{
        df <- .x
        n_obs <- nrow(df)  # Count the number of valid observations

        # Initialize result list
        result <- list(
          correlation = NA_real_,
          n_obs = if(if_nobs) n_obs else NULL,
          cor_pval = if(if_pval) NA_real_ else NULL,
          peak = if(if_peak) NA_real_ else NULL
        )

        # Only calculate correlation if there are enough valid observations with variation
        if (n_obs >= 3 && sd(df[[x]], na.rm = TRUE) > 0 && sd(df[[y]], na.rm = TRUE) > 0) {
          test <- cor.test(df[[x]], df[[y]])  # Pearson correlation test
          result$correlation <- test$estimate  # Extract correlation coefficient
          if(if_pval) result$cor_pval <- test$p.value  # Extract p-value
          if(if_peak) result$peak <- identify_peak(df) # Check for peak
        }

        return(result[!sapply(result, is.null)])  # Remove NULL elements
      }),

      # Extract individual fields from the stats list-column
      correlation = purrr::map_dbl(stats, "correlation"),
      n_obs = if(if_nobs) purrr::map_int(stats, "n_obs") else NULL,
      cor_pval = if(if_pval) purrr::map_dbl(stats, "cor_pval") else NULL,
      peak = if(if_peak) purrr::map_dbl(stats, "peak") else NULL
    ) |>
    # Remove NULL columns (those not requested)
    select(-stats) |>
    # Remove columns based on function arguments
    # {function(.) {
    #   if (!if_data) . <- select(., -data)
    #   if (!if_nobs) . <- select(., -n_obs)
    #   if (!if_pval) . <- select(., -cor_pval)
    #   if (!if_peak) . <- select(., -peak)
    #   .
    # }}() |>
    ungroup()

  return(df_cor)
}

#' Combined windowed correlation analysis
#'
#' Original combined function that calls both sub-functions
windows_cor_analysis <- function(raster, ...) {
  d_win <- create_spatial_windows(raster, ...)
  calculate_window_correlations(d_win, ...)
}


# ------calculate_land_use_fraction---------------
calculate_fraction_land_use <- function(df_win, output_file = NULL){

  # set reference land cover classes
  ref_classes <- c(10, 11, 12, 20, 30, 40, 190, 200, 201, 202, 210)

  # calculation
  df_flc <- df_win  |>

    mutate(lccs_class = factor(lccs_class, levels = ref_classes)) |>
    count(lon_mid, lat_mid, lccs_class, .drop = FALSE) |>

    group_by(lon_mid, lat_mid) |>
    mutate(prop = n / sum(n)) |>

    summarise(
      fused = sum(prop[lccs_class %in% c(10, 11, 12, 20, 190)], na.rm = TRUE) +
        0.75 * sum(prop[lccs_class == 30], na.rm = TRUE) +
        0.25 * sum(prop[lccs_class == 40], na.rm = TRUE),
      fbare   = sum(prop[lccs_class %in% c(200, 201, 202)], na.rm = TRUE),
      fwater   = sum(prop[lccs_class == 210], na.rm = TRUE),
      .groups = "drop"
    )

   # ------ save 5km flc output -------
  if (!is.null(output_file)) {
    flc_r <- terra::rast(
      df_flc[, c("lon_mid", "lat_mid", "fused", "fbare", "fwater")],
      type = "xyz",
      crs = "EPSG:4326"
    )

    names(flc_r) <- c("fused", "fbare", "fwater")

    terra::writeCDF(flc_r, output_file, overwrite = TRUE)
  }

  return(df_flc)
}


