crop_raster_to_extent <- function(nc_file, xmin, xmax, ymin, ymax) {
  # Check if file exists
  if (!file.exists(nc_file)) {
    stop("The NetCDF file does not exist: ", nc_file)
  }

  r <- rast(nc_file)

  # Check if the extent is valid
  extent_raster <- ext(xmin, xmax, ymin, ymax)
  if (xmin >= xmax | ymin >= ymax) {
    stop("Invalid extent: xmin should be less than xmax, and ymin should be less than ymax.")
  }

  cropped_raster <- crop(r, extent_raster)
  return(cropped_raster)
}


resample_and_merge_rasters <- function(reference_raster, raster_to_resample) {
  # Check if both rasters are valid SpatRaster objects
  if (!inherits(reference_raster, "SpatRaster") | !inherits(raster_to_resample, "SpatRaster")) {
    stop("Both inputs must be SpatRaster objects.")
  }

  # Resample to match reference resolution
  resampled_raster <- resample(raster_to_resample, reference_raster, method = "bilinear")
  # Mask based on the reference raster's extent and resolution
  resampled_raster <- mask(resampled_raster, reference_raster)

  # Merge rasters
  merged_raster <- c(reference_raster, resampled_raster)

  # Set standardized attributes
  names(merged_raster) <- c("twi", "vegh")
  varnames(merged_raster) <- c("twi", "vegh")
  longnames(merged_raster) <- c("Topographic Wetness Index", "Vegetation Height")
  units(merged_raster) <- c("unitless", "m")

  return(merged_raster)
}

create_spatial_windows <- function(raster_data, window_size = 15) {
  # Convert raster to dataframe
  df <- as.data.frame(raster_data, xy = TRUE, na.rm = TRUE)
  colnames(df) <- c("lon", "lat", "twi", "vegh")

  # Ensure window size is positive
  if (window_size <= 0) {
    stop("Window size must be a positive integer.")
  }

  # Calculate window dimensions based on raster resolution
  resolution <- res(raster_data)
  lon_window_size <- round(resolution[1] * window_size, 4)
  lat_window_size <- round(resolution[2] * window_size, 4)

  # Create window boundaries
  lon_breaks <- seq(
    from = floor(min(df$lon)),
    to = ceiling(max(df$lon)),
    by = lon_window_size
  )

  lat_breaks <- seq(
    from = floor(min(df$lat)),
    to = ceiling(max(df$lat)),
    by = lat_window_size
  )

  # Assign each point to a window
  df <- df |>
    mutate(
      lon_window = cut(lon, breaks = lon_breaks, include.lowest = TRUE),
      lat_window = cut(lat, breaks = lat_breaks, include.lowest = TRUE),
      window_id = as.integer(interaction(lon_window, lat_window))
    ) |>
    select(lon, lat, twi, vegh, window_id)

  return(df)
}

calculate_window_correlations <- function(windowed_data) {
  windowed_data |>
    group_by(window_id) |>
    group_nest() |>
    mutate(
      correlation = purrr::map_dbl(data, ~{
        if (nrow(.x) < 2) return(NA)

        twi_sd <- sd(.x$twi, na.rm = TRUE)
        vegh_sd <- sd(.x$vegh, na.rm = TRUE)

        if (is.na(twi_sd) || is.na(vegh_sd) || twi_sd == 0 || vegh_sd == 0) {
          return(NA)
        } else {
          return(cor(.x$twi, .x$vegh, use = "complete.obs"))
        }
      })
    ) |>
    unnest(cols = c(data)) |> # unnest will remove the correlation column!!!
    ungroup()

}

plot_window_distribution <- function(windowed_data) {
  n_windows <- length(unique(windowed_data$window_id))

  set.seed(33)  # For reproducibility of the random colors
  # Generate a random color palette
  window_colors <- sample(colors(), n_windows)

  # 确保数据框传递给 ggplot2 是正确的
  p <- ggplot(windowed_data, aes(x = lon, y = lat, fill = factor(window_id))) +
    geom_tile() +
    scale_fill_manual(values = window_colors) +
    theme_minimal() +
    labs(
      title = "Spatial Window Distribution",
      x = "Longitude",
      y = "Latitude"
    ) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "none"  # Too many windows for legend
    )

  return(p)
}


plot_window_pixel_counts <- function(windowed_data) {
  window_counts <- windowed_data |>
    group_by(window_id) |>
    summarise(pixel_count = n(), .groups = "drop")

  # 计算 225 的比例
  pct_225 <- mean(window_counts$pixel_count == 225) * 100

  # 绘图
  p <- ggplot(window_counts, aes(x = pixel_count)) +
    geom_histogram(binwidth = 5, fill = "skyblue", color = "white", alpha = 0.8) +
    theme_minimal() +
    labs(
      title = "Distribution of Pixel Counts Across Windows",
      x = "Number of Pixels per Window",
      y = "Frequency (Number of Windows)"
    ) +
    theme(panel.grid.minor = element_blank())

  # 标注 225 处的占比
  if (pct_225 > 80) {
    p <- p + annotate("text", x = 225, y = max(table(window_counts$pixel_count)),
                      label = paste0(round(pct_225, 1), "% at 225"),
                      color = "red", vjust = -1.5)
  }

  return(p)
}
