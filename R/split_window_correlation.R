crop_byextent <- function(nc_file, extent_raster) {
  # Check if file exists
  if (!file.exists(nc_file)) {
    stop("The NetCDF file does not exist: ", nc_file)
  }

  r <- rast(nc_file)

  cropped_raster <- crop(r, extent_raster)

  rm(r, nc_file)
  gc()

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

  return(merged_raster)
}

ext_to_merge <- function(region_ext, file_ga2, file_vegh, output_path) {
  # 加载 raster 数据
  twi_r <- crop(rast(file_ga2), region_ext)
  vegh_r <- crop(rast(file_vegh), region_ext)

  # 合并 raster 数据
  merged_r <- c(twi_r, vegh_r)

  # 将合并后的结果保存为临时文件
  writeRaster(merged_r, output_path, overwrite = TRUE)
}


ext_to_merge2 <- function(ext, region_name, output_dir, twi_file, vegh_file){

  twi_raster <- crop_byextent(twi_file, ext)
  vegh_raster <- crop_byextent(vegh_file, ext)
  merged_raster <- resample_and_merge_rasters(twi_raster, vegh_raster)

  output_file <- file.path(output_dir, paste0("twi_vegh_", region_name, ".nc"))

  suppressWarnings({
    writeCDF(merged_raster, output_file, overwrite = TRUE)
  })

  rm(twi_raster, vegh_raster)
  gc()


  return(output_file)
}

create_spatial_windows <- function(raster_data, window_size = 12) {
  # Convert raster to dataframe
  suppressWarnings({df <- as.data.frame(raster_data, xy = TRUE, na.rm = TRUE)})

  colnames(df) <- c("lon", "lat", "twi", "vegh")

  # Ensure window size is positive
  if (window_size <= 0) {
    stop("Window size must be a positive integer.")
  }

  # Calculate window dimensions based on raster resolution
  resolution <- res(raster_data)
  lon_window_size <- round(resolution[1] * window_size, 3)
  lat_window_size <- round(resolution[2] * window_size, 3)

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
  correlation_results <- windowed_data |>
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
      }),
    ) |>
    unnest(cols = c(data)) |> # unnest to expand the grouped data
    ungroup() |>
    select(lon, lat, window_id, correlation)
}

calculate_window_correlations1 <- function(windowed_data) {
  correlation_results <- windowed_data |>
    group_by(window_id) |>
    group_nest() |>
    mutate(
      # 回归和统计计算
      stats = purrr::map(data, ~{
        df <- .x
        df <- df[complete.cases(df$twi, df$vegh), ]
        n_obs <- nrow(df)

        # 默认返回 NA 值
        result <- list(
          correlation = NA_real_,
          cor_pval = NA_real_,
          n_obs = n_obs
        )

        # 如果观测数足够
        if (n_obs >= 3 && sd(df$twi) > 0 && sd(df$vegh) > 0) {
          # Pearson correlation and p-value
          test <- cor.test(df$twi, df$vegh)
          result$correlation <- test$estimate
          result$cor_pval <- test$p.value
        }

        return(result)
      }),

      # 提取窗口中心坐标
      lon_win = purrr::map_dbl(data, ~mean(.x$lon, na.rm = TRUE)),
      lat_win = purrr::map_dbl(data, ~mean(.x$lat, na.rm = TRUE)),

      # 拆分 stats 列为单独列
      correlation = purrr::map_dbl(stats, "correlation"),
      cor_pval = purrr::map_dbl(stats, "cor_pval"),
      n_obs = purrr::map_int(stats, "n_obs")
    ) |>
    select(window_id, data, lon_win, lat_win, n_obs, correlation, cor_pval) |>
    ungroup()

  return(correlation_results)
}


calculate_window_correlations2 <- function(windowed_data) {
  correlation_results <- windowed_data |>
    group_by(window_id) |>
    group_nest() |>
    mutate(
      # 回归和统计计算
      stats = purrr::map(data, ~{
        df <- .x
        df <- df[complete.cases(df$twi, df$vegh), ]
        n_obs <- nrow(df)

        # 默认返回 NA 值
        result <- list(
          correlation = NA_real_,
          cor_pval = NA_real_,
          peak = NA_real_,
          n_obs = n_obs
        )

        # 如果观测数足够
        if (n_obs >= 3 && sd(df$twi) > 0 && sd(df$vegh) > 0) {
          # Pearson correlation and p-value
          test <- cor.test(df$twi, df$vegh)
          result$correlation <- test$estimate
          result$cor_pval <- test$p.value
          result$peak <- identify_peak(df) # Check for peak using identify_peak function
        }
        return(result)
      }),

      # 提取窗口中心坐标
      lon_win = purrr::map_dbl(data, ~mean(.x$lon, na.rm = TRUE)),
      lat_win = purrr::map_dbl(data, ~mean(.x$lat, na.rm = TRUE)),

      # 拆分 stats 列为单独列
      correlation = purrr::map_dbl(stats, "correlation"),
      cor_pval = purrr::map_dbl(stats, "cor_pval"),
      n_obs = purrr::map_int(stats, "n_obs"),
      peak = purrr::map_dbl(stats, "peak")  # Adding the peak column
    ) |>
    select(window_id, data, lon_win, lat_win, n_obs, correlation, cor_pval, peak) |>
    ungroup()

  return(correlation_results)
}

identify_peak <- function(df) {
  linmod <- lm(twi ~ vegh, data = df)

  # 安全地尝试拟合 segmented 模型
  segmod <- tryCatch(
    segmented::segmented(linmod, seg.Z = ~ twi, npsi = 1, silent = TRUE),
    error = function(e) return(NULL)
  )

  # 如果拟合失败，则返回 FALSE（或 NA）
  if (is.null(segmod)) return(NA)

  # 确保系数存在
  coefs <- coef(segmod)
  if (!all(c("twi", "U1.twi") %in% names(coefs))) return(NA)

  slope1 <- coefs[["twi"]]
  slope2 <- coefs[["twi"]] + coefs[["U1.twi"]]

  return(slope1 > 0 && slope2 < 0)
}



plot_random_windows <- function(correlation_results, seed = 123) {
  set.seed(seed)

  valid_windows <- correlation_results |>
    filter(!is.na(correlation), n_obs >= 100)

  if (nrow(valid_windows) < 3) {
    stop("The number of available windows is less than 3, please check the data.")
  }

  selected_windows <- sample(valid_windows$window_id, 3)

  plots <- purrr::map(selected_windows, function(wid) {
    row <- valid_windows |> filter(window_id == wid)

    df <- row$data[[1]] |> filter(complete.cases(twi, vegh))

    corr <- round(row$correlation, 3)
    pval <- signif(row$cor_pval, 3)
    lon <- round(row$lon_win, 4)
    lat <- round(row$lat_win, 4)

    ggplot(df, aes(x = twi, y = vegh)) +
      geom_point(alpha = 0.6) +
      geom_smooth(method = "lm", color = "blue", linewidth = 1) +
      ggtitle(
        paste0("Window ", wid,
               "\nLon: ", lon, ", Lat: ", lat,
               "\nR = ", corr, ", p = ", pval)
      ) +
      theme_classic()


  })

  return(plots)
}


spit_window_analysis_parallel_byext <- function(output_dir, region_names, merged_raster_files){
  # Clear memory
  gc()

  # 设置并行
  plan(multisession, workers = 2)
  handlers("cli")

  log_file <- file.path(output_dir, "processing_log.txt")
  writeLines("=== Region Processing Log ===\n", log_file)

  with_progress({
    pb <- progressor(along = merged_raster_files)

    results <- future_map2(
      merged_raster_files,
      region_names,
      function(raster_file, region_name) {
        pb(sprintf("Processing %s", region_name))

        # 定义安全运行函数
        safe_run <- safely(function(raster_file, region_name) {
          message(sprintf("Processing region: %s", region_name))

          # get raster using file path
          suppressWarnings({ merged_raster <- rast(raster_file) })

          region_ext <- ext(merged_raster)

          windowed_data <- create_spatial_windows(merged_raster, 12)
          correlation_df <- calculate_window_correlations(windowed_data)

          # 打印 NA 信息（调试用）
          cat(region_name, " NA count:\n")
          print(colSums(is.na(correlation_df)))

          # 绘图
          plot1 <- plot_twi(windowed_data)
          plot2 <- plot_vegh(windowed_data)
          plot4 <- plot_correlation_vs_pixel_count(correlation_df)
          plot5 <- plot_corr(correlation_df)
          plot6 <- plot_img(region_ext)
          plot7 <- plot_landcover(file_modis_landcover, region_ext)

          combined_plot <- plot_grid(plot1, plot2, plot5, plot4, plot6, plot7,
                                     ncol = 3, align = "v") +
            theme(plot.background = element_rect(
              fill = "white", color = "white"))

          output_file <- file.path(output_dir, paste0("figures/combined_plot_",
                                                      region_name,
                                                      ".png"))
          ggsave(output_file, combined_plot, width = 20, height = 10, dpi = 300, bg = "white")
          return(output_file)
        })


        result <- safe_run(raster_file, region_name)

        # 写日志
        log_msg <- if (is.null(result$error)) {
          sprintf("[SUCCESS] %s -> %s", region_name, basename(result$result))
        } else {
          sprintf("[ERROR] %s -> %s", region_name, result$error$message)
        }
        write(log_msg, file = log_file, append = TRUE)

        return(result)
      }
    )
  })

  # 恢复单线程
  plan(sequential)
  gc()
}


# -----------------------------------
# Visualization
# -----------------------------------
plot_twi <- function(windowed_data) {
  p <- ggplot(windowed_data, aes(x = lon, y = lat, fill = twi)) +
    geom_raster() +
    scale_fill_scico(palette = "oslo", direction = -1) +
    labs(title = "Topographic Wetness Index (TWI)",
         fill = "TWI",
         x = "Lontitude",
         y = "Latitude") +
    theme_classic() +
    theme(legend.position = "right")

  return(p)
}

plot_vegh <- function(windowed_data) {
  p <- ggplot(windowed_data, aes(x = lon, y = lat, fill = vegh)) +
    geom_raster() +
    scale_fill_scico(palette = "batlow", direction = -1) +
    labs(title = "Vegetation Height (m)",
         fill = "VEGH",
         x = "Lontitude",
         y = "Latitude") +
    theme_classic() +
    theme(legend.position = "right")

  return(p)
}

plot_landcover <- function(file_modis_landcover, ext){

  modis <- terra::rast(file_modis_landcover)
  landcover <- modis[["landcover"]]
  landcover_crop <- crop(landcover, ext)

  modis_df <- as.data.frame(landcover_crop, xy = TRUE, na.rm = TRUE)
  colnames(modis_df) <- c("lon", "lat", "landcover")

  modis_colors <- c(
    "#0000FF", # 水域 (Water)
    "#006400", # 常绿针叶林 (Evergreen Needleleaf Forest)
    "#228B22", # 常绿阔叶林 (Evergreen Broadleaf Forest)
    "#ADFF2F", # 落叶针叶林 (Deciduous Needleleaf Forest)
    "#7CFC00", # 落叶阔叶林 (Deciduous Broadleaf Forest)
    "#32CD32", # 混合林 (Mixed Forest)
    "#8B4513", # 封闭灌木丛 (Closed Shrublands)
    "#DEB887", # 开放灌木丛 (Open Shrublands)
    "#BDB76B", # 林地稀树草原 (Woody Savannas)
    "#EEE8AA", # 草原 (Savannas)
    "#FFFF00", # 湿地 (Grasslands)
    "#00CED1", # 农田 (Permanent Wetlands)
    "#FFA500", # 农田 (Croplands)
    "#FF0000", # 城市和建成区 (Urban and Built-Up)
    "#DAA520", # 农业与自然植被混合 (Cropland/Natural Vegetation Mosaic)
    "#FFFFFF", # 冰雪覆盖区 (Snow and Ice)
    "#D3D3D3"  # 贫瘠或稀疏植被区 (Barren or Sparsely Vegetated)
  )

  modis_labels <- c(
    "Water",
    "Evergreen Needleleaf Forest",
    "Evergreen Broadleaf Forest",
    "Deciduous Needleleaf Forest",
    "Deciduous Broadleaf Forest",
    "Mixed Forest",
    "Closed Shrublands",
    "Open Shrublands",
    "Woody Savannas",
    "Savannas",
    "Grasslands",
    "Permanent Wetlands",
    "Croplands",
    "Urban and Built-Up",
    "Cropland/Natural Mosaic",
    "Snow and Ice",
    "Barren or Sparsely Vegetated"
  )

  p <- ggplot(modis_df) +
    geom_raster(aes(x = lon, y = lat, fill = factor(landcover))) +
    scale_fill_manual(values = modis_colors, labels = modis_labels, name = "Land Cover") +
    coord_equal() +
    labs(title = "MODIS Land Cover (2010)", x = "Longitude", y = "Latitude") +
    theme_classic()

  rm(modis, landcover, landcover_crop, modis_df)
  gc()
  return(p)
}


plot_img <- function(ext_test) {
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

plot_corr <- function(correlation_df) {

  df <- correlation_df |>
    unnest(data)|>
    select(lon, lat, correlation) |>
    drop_na()  # Remove rows with missing values

  p <- ggplot(df, aes(x = lon, y = lat, fill = correlation)) +
    geom_tile() +
    scale_fill_scico(
      palette = "bam",
      midpoint = 0,
      limits = c(min(df$correlation, na.rm = TRUE),
                 max(df$correlation, na.rm = TRUE)),
      name = expression(r[TWI,VEGH])
    ) +
    labs(
      title = "TWI-VEGH Correlation Analysis",
      fill = "Correlation",
      x = "Longitude",
      y = "Latitude"
    ) +
    theme_classic() +
    theme(legend.position = "right")

  return(p)
}



plot_correlation_vs_pixel_count <- function(correlation_df) {
  # 原数据分成两类
  df <- correlation_df |>
    select(n_obs, correlation)

  # 拆出 correlation 为 NA 的部分
  na_data <- df |> filter(is.na(correlation))
  data_valid <- df |> filter(!is.na(correlation))

  ggplot() +
    # 非 NA 值的点密度图
    geom_pointdensity(data = data_valid, aes(x = n_obs, y = correlation), adjust = 1.5) +
    scale_color_viridis_c() +

    # NA 的点，用红色叉号显示
    geom_point(data = na_data, aes(x = n_obs, y = 0),
               shape = 4, color = "red", size = 3, stroke = 1.2) +

    labs(
      title = "Density-colored Scatter Plot \n(with NA correlations shown)",
      x = "Pixel Count per Window",
      y = "Correlation Coefficient",
      color = "Local Density of Window"
    ) +
    theme_classic() +
    theme(legend.position = "right",
          axis.title = element_text(size = 12),
          plot.title = element_text(size = 14, face = "bold"))
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
    theme_classic() +
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
  pct_144 <- mean(window_counts$pixel_count == 144) * 100

  # 绘图
  p <- ggplot(window_counts, aes(x = pixel_count)) +
    geom_histogram(binwidth = 5, fill = "skyblue", color = "white", alpha = 0.8) +
    labs(
      title = "Distribution of Pixel Counts Across Windows",
      x = "Number of Pixels per Window",
      y = "Frequency (Number of Windows)"
    ) +
    theme_classic()

  # 标注 225 处的占比
  if (pct_144 > 80) {
    p <- p + annotate("text", x = 144, y = max(table(window_counts$pixel_count)),
                      label = paste0(round(pct_144, 1), "% at 144"),
                      color = "red", vjust = -1.5)
  }

  return(p)
}
