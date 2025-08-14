# ------------------------------------------------------------------------------
# Main Functions
# ------------------------------------------------------------------------------

create_spatial_windows <- function(raster_data) {

  # Convert raster to dataframe
  suppressWarnings({df <- as.data.frame(raster_data, xy = TRUE, na.rm = TRUE)})
  colnames(df) <- c("lon", "lat", "twi", "vegh")

  dlon <- 0.05 # corrected here
  dlat <- 0.05 # corrected here

  # Create window boundaries
  lon_breaks <- seq(
    from = floor(min(df$lon)), to = ceiling(max(df$lon)), by = dlon)

  lat_breaks <- seq(
    from = floor(min(df$lat)), to = ceiling(max(df$lat)),  by = dlat)

  df <- df |>
    ungroup() |>
    mutate(ilon = cut(lon, breaks = lon_breaks),
           ilat = cut(lat, breaks = lat_breaks)
    ) |>
    mutate(lon_lower = as.numeric( sub("\\((.+),.*", "\\1", ilon)),
           lon_upper = as.numeric( sub("[^,]*,([^]]*)\\]", "\\1", ilon) ),
           lat_lower = as.numeric( sub("\\((.+),.*", "\\1", ilat) ),
           lat_upper = as.numeric( sub("[^,]*,([^]]*)\\]", "\\1", ilat) ),
           window_id = as.integer(interaction(ilon, ilat))
    ) |>
    mutate(lon_mid = (lon_lower + lon_upper)/2,
           lat_mid = (lat_lower + lat_upper)/2) |>

    ## create cell name to associate with climate input
    dplyr::select(-ilon, -ilat, -lon_lower, -lon_upper, -lat_lower, -lat_upper)

  return(df)
}


# Function to calculate Pearson correlation and p-value for each window of data
# Return only windowed data with coarser resolution
calculate_window_correlations <- function(windowed_data) {
  correlation_results <- windowed_data |>
    group_by(window_id, lon_mid, lat_mid) |>  # Group data by window ID and location
    tidyr::nest() |>  # Nest the grouped data into list-columns
    mutate(
      # Perform statistical computations
      stats = purrr::map(data, ~{
        df <- .x
        # df <- df[complete.cases(df$twi, df$vegh), ]  # Remove rows with missing twi or vegh
        n_obs <- nrow(df)  # Count the number of valid observations

        # Default result with NA values
        result <- list(
          correlation = NA_real_,
          cor_pval = NA_real_,
          n_obs = n_obs
        )

        # Only calculate correlation if there are enough valid observations with variation
        if (n_obs >= 3 && sd(df$twi) > 0 && sd(df$vegh) > 0) {
          test <- cor.test(df$twi, df$vegh)  # Pearson correlation test
          result$correlation <- test$estimate  # Extract correlation coefficient
          result$cor_pval <- test$p.value  # Extract p-value
        }

        return(result)
      }),

      # Extract individual fields from the stats list-column
      correlation = purrr::map_dbl(stats, "correlation"),
      cor_pval = purrr::map_dbl(stats, "cor_pval"),
      n_obs = purrr::map_int(stats, "n_obs")
    ) |>
    dplyr::select(window_id, lon_mid, lat_mid, n_obs, correlation, cor_pval) |>  # Keep relevant columns
    ungroup()

  return(correlation_results)  # Return final result
}

# Function to calculate Pearson correlation and p-value for each window of data
# Return unnested original data with original resolution as well
calculate_window_correlations1 <- function(windowed_data) {
  correlation_results <- windowed_data |>
    group_by(window_id, lon_mid, lat_mid) |>  # Group data by window ID and location
    tidyr::nest() |>  # Nest the grouped data into list-columns
    mutate(
      # Perform statistical computations
      stats = purrr::map(data, ~{
        df <- .x
        # df <- df[complete.cases(df$twi, df$vegh), ]  # Remove rows with missing twi or vegh
        n_obs <- nrow(df)  # Count the number of valid observations

        # Default result with NA values
        result <- list(
          correlation = NA_real_,
          cor_pval = NA_real_,
          n_obs = n_obs
        )

        # Only calculate correlation if there are enough valid observations with variation
        if (n_obs >= 3 && sd(df$twi) > 0 && sd(df$vegh) > 0) {
          test <- cor.test(df$twi, df$vegh)  # Pearson correlation test
          result$correlation <- test$estimate  # Extract correlation coefficient
          result$cor_pval <- test$p.value  # Extract p-value
        }

        return(result)
      }),

      # Extract individual fields from the stats list-column
      correlation = purrr::map_dbl(stats, "correlation"),
      cor_pval = purrr::map_dbl(stats, "cor_pval"),
      n_obs = purrr::map_int(stats, "n_obs")
    ) |>
    # unnest(cols = c(data)) |> # unnest to expand the grouped data
    ungroup() |>
    dplyr::select(data, window_id, lon_mid, lat_mid, n_obs, correlation, cor_pval) |>  # Keep relevant columns
    ungroup()

  return(correlation_results)  # Return final result
}

# Function to calculate Pearson correlation, p-value and ifpeak for each window of data
# Return only windowed data with coarser resolution
calculate_window_correlations2 <- function(windowed_data) {
  correlation_results <- windowed_data |>
    group_by(window_id, lon_mid, lat_mid) |>
    tidyr::nest() |>
    mutate(
      # Perform statistical computations
      stats = purrr::map(data, ~{
        df <- .x
        # df <- df[complete.cases(df$twi, df$vegh), ]
        n_obs <- nrow(df)

        # Default result with NA values
        result <- list(
          correlation = NA_real_,
          cor_pval = NA_real_,
          peak = NA_real_,
          n_obs = n_obs
        )

        # Only calculate correlation if there are enough valid observations with variation
        if (n_obs >= 3 && sd(df$twi) > 0 && sd(df$vegh) > 0) {
          # Pearson correlation and p-value
          test <- cor.test(df$twi, df$vegh)
          result$correlation <- test$estimate
          result$cor_pval <- test$p.value
          result$peak <- identify_peak(df) # Check for peak using identify_peak function
        }
        return(result)
      }),

      # Extract individual fields from the stats list-column
      correlation = purrr::map_dbl(stats, "correlation"),
      cor_pval = purrr::map_dbl(stats, "cor_pval"),
      n_obs = purrr::map_int(stats, "n_obs"),
      peak = purrr::map_dbl(stats, "peak")  # Adding the peak column
    ) |>
    dplyr::select(data, window_id, lon_mid, lat_mid, n_obs, correlation, cor_pval, peak) |>
    ungroup()

  return(correlation_results)
}


# Internal processing function
pre_merge_tile_rasters <- function(tile_extents, twi_file, vegh_file, processed_dir) {
  # Load rasters
  twi_raster_full <- terra::rast(twi_file)
  vegh_raster_full <- terra::rast(vegh_file)
  mosaic_extent <- terra::ext(twi_raster_full)

  # Progress bar
  p <- progressr::progressor(along = tile_extents)

  # Process each tile
  processed_tile_files <- purrr::imap(tile_extents, function(tile_extent, tile_name) {
    p(message = paste("Processing", tile_name))
    output_file <- file.path(processed_dir, paste0(tile_name, "_twi_vegh_rm.nc"))

    # Skip if there is no overlap
    if (is.null(terra::intersect(mosaic_extent, tile_extent))) {
      message(glue::glue("⚠ Skipping {tile_name}: no intersection"))
      return(NA)
    }

    # Try processing the tile
    tryCatch({
      # Crop the TWI and vegetation rasters
      twi_crop <- terra::crop(twi_raster_full, tile_extent)
      vegh_crop <- terra::crop(vegh_raster_full, tile_extent)

      # Check if either of the cropped rasters is entirely NA
      if (terra::ncell(twi_crop) == 0 || all(is.na(twi_crop[])) || terra::ncell(vegh_crop) == 0 || all(is.na(vegh_crop[]))) {
        message(glue::glue("⚠ Skipping {tile_name}: no valid data in TWI or vegetation"))
        return(NA)
      }

      # Resample vegetation raster to match TWI raster's resolution
      vegh_resampled <- terra::resample(vegh_crop, twi_crop, method = "bilinear")

      # Mask the resampled vegetation raster with the TWI raster
      vegh_masked <- terra::mask(vegh_resampled, twi_crop)

      # Merge the TWI and masked vegetation rasters
      merged <- c(twi_crop, vegh_masked)
      names(merged) <- c("twi", "vegh")

      # Write the merged raster to the output file
      terra::writeCDF(merged, output_file, overwrite = TRUE)
      message(glue::glue("✓ Saved: {output_file}"))
      output_file
    }, error = function(e) {
      message(glue::glue("❌ Failed {tile_name}: {e$message}"))
      NA
    })
  })

  # Clean up
  rm(twi_raster_full, vegh_raster_full); gc()
  return(processed_tile_files)
}

# Main function to process all tiles and merge results
generate_prep_tiles_info <- function(ext_list, twi_file, vegh_file, temp_dir,
                                     output_file = here::here("data/preprocessed_tiles_info.csv")) {
  # Create output subfolder
  processed_dir <- file.path(temp_dir, "pre_merged_rasters")
  dir.create(processed_dir, showWarnings = FALSE, recursive = TRUE)

  # Generate extents for 72 tiles
  tile_extents <- ext_list

  # Run processing
  prep_tile_files <- with_progress({
    pre_merge_tile_rasters(
      tile_extents = tile_extents,
      twi_file = twi_file,
      vegh_file = vegh_file,
      processed_dir = processed_dir
    )
  })

  # Merge name, extent, and file path
  all_tile_names <- names(tile_extents)

  prep_tiles_info <- map2_dfr(
    all_tile_names, seq_along(all_tile_names),
    function(name, idx) {
      ext <- tile_extents[[idx]]
      file <- prep_tile_files[[idx]]

      if (!is.na(file)) {
        tibble(
          name = name,
          xmin = terra::xmin(ext),
          xmax = terra::xmax(ext),
          ymin = terra::ymin(ext),
          ymax = terra::ymax(ext),
          file = file
        )
      } else {
        NULL
      }
    }
  )

  # Save as CSV
  readr::write_csv(prep_tiles_info, info_file)

  return(prep_tiles_info)
}


mosaic_and_save_rasters <- function(input_dir, output_file, pattern = "*.nc", verbose = FALSE) {
  if (verbose) message("Reading raster files...")
  files <- dir_ls(path = input_dir, glob = pattern)
  if (length(files) == 0) stop("No matching raster files found.")

  rasters <- lapply(files, rast)
  rasters <- unname(rasters)

  mosaic <- do.call(merge, rasters)

  rm(rasters)
  gc()

  writeCDF(mosaic, filename = output_file, overwrite = TRUE)

  rm(mosaic)
  gc()

  if (verbose) message("Done: Mosaic saved to ", output_file)
}
# ------------------------------------------------------------------------------
# Additional Functions
# ------------------------------------------------------------------------------

# Divide the globe into multipal tiles and
# name the west_south_cor(i.e. lat_min, lon_min)

# Divide the globe into multipal tiles and
# name the west_south_cor(i.e. lat_min, lon_min)
generate_global_extents <- function(
    lon_step = 30,
    lat_step = 30
) {
  ext_valids <- list()

  # Define longitude and latitude edges
  lon_edges <- seq(-180, 180, by = lon_step)
  lat_edges <- seq(-90, 90, by = lat_step)

  # Label formatting functions for longitude and latitude
  make_lon_label <- function(lon) {
    if (lon < 0) paste0("W", abs(lon)) else paste0("E", lon)
  }

  make_lat_label <- function(lat) {
    if (lat < 0) paste0("S", abs(lat)) else paste0("N", lat)
  }

  # Iterate from south to north, and west to east
  for (lat_i in 1:(length(lat_edges) - 1)) {
    for (lon_j in 1:(length(lon_edges) - 1)) {
      lat_min <- lat_edges[lat_i]
      lat_max <- lat_edges[lat_i + 1]
      lon_min <- lon_edges[lon_j]
      lon_max <- lon_edges[lon_j + 1]

      # Use northwest (upper-left) corner for naming
      name <- paste0(
        make_lat_label(lat_min), "_",
        make_lon_label(lon_min)
      )

      ext_valids[[name]] <- ext(lon_min, lon_max, lat_min, lat_max)
    }
  }

  return(ext_valids)
}

# Function to identify a peak (breakpoint) in non-monotonic relationship
identify_peak <- function(df) {
  # Fit a linear model: vegh as a function of twi
  linmod <- lm(vegh ~ twi, data = df)

  # Safely try to fit a segmented (piecewise) regression model
  segmod <- tryCatch(
    segmented::segmented(linmod, seg.Z = ~ twi, npsi = 1, silent = TRUE),
    error = function(e) return(NULL)  # Return NULL if model fitting fails
  )

  # If the segmented model fitting fails, return NA
  if (is.null(segmod)) return(NA)

  # Extract coefficients from the segmented model
  coefs <- coef(segmod)

  # Ensure the necessary coefficients exist
  if (!all(c("twi", "U1.twi") %in% names(coefs))) return(NA)

  # Calculate slope before and after the breakpoint
  slope1 <- coefs[["twi"]]                     # Slope before breakpoint
  slope2 <- coefs[["twi"]] + coefs[["U1.twi"]] # Slope after breakpoint

  # Return TRUE if peak exists (slope changes from positive to negative)
  return(slope1 > 0 && slope2 < 0)
}

normalize_string <- function(x) {
  x <- tolower(x)
  x <- gsub(" ", "_", x)
  return(x)
}

# ------------------------------------------------------------------------------
# Visualization
# ------------------------------------------------------------------------------

# plot topographic wetness index (TWI or CTI) map by dataset
plot_twi <- function(dataframe, xmin, xmax, ymin, ymax, x_breaks = 5, y_breaks = 5) {
  p <- ggplot(dataframe, aes(x = lon, y = lat, fill = twi)) +
    geom_tile() +
    scale_fill_scico(palette = "oslo", direction = -1) +
    labs(title = "Topographic Wetness Index (TWI)",
         fill = "TWI") +
    scale_x_continuous(
      name = "Longitude",  # 横坐标标题
      expand = c(0, 0),
      limits = c(xmin, xmax),
      breaks = seq(xmin, xmax, by = x_breaks)
    ) +
    scale_y_continuous(
      name = "Latitude",   # 纵坐标标题
      expand = c(0, 0),
      limits = c(ymin, ymax),
      breaks = seq(ymin, ymax, by = y_breaks)
    ) +
    theme(
      legend.position = "right",
      legend.text = element_text(size = 6),
      legend.title = element_blank(),
      plot.title = element_text(face = "bold")
    )

  return(p)
}

# plot vegetation height map by dataset
plot_vegh <- function(dataframe, xmin, xmax, ymin, ymax, x_breaks = 5, y_breaks = 5) {
  ggplot(dataframe, aes(x = lon, y = lat, fill = vegh)) +
    geom_tile() +
    scale_fill_scico(palette = "batlow", direction = -1) +
    labs(
      title = "Vegetation Height 2020 (m)",
      fill = "VEGH"
    ) +
    scale_x_continuous(
      name = "Longitude",  # 横坐标标题
      expand = c(0, 0),
      limits = c(xmin, xmax),
      breaks = seq(xmin, xmax, by = x_breaks)
    ) +
    scale_y_continuous(
      name = "Latitude",   # 纵坐标标题
      expand = c(0, 0),
      limits = c(ymin, ymax),
      breaks = seq(ymin, ymax, by = y_breaks)
    ) +
    theme(
      legend.position = "right",
      legend.text = element_text(size = 6),
      legend.title = element_blank(),
      plot.title = element_text(face = "bold")
    )
}


# plot MODIS land cover map by ext
plot_landcover <- function(file_modis_landcover, ext){

  modis <- terra::rast(file_modis_landcover)
  landcover <- modis[["landcover"]]
  landcover_crop <- crop(landcover, ext)

  modis_df <- as.data.frame(landcover_crop, xy = TRUE, na.rm = TRUE)
  colnames(modis_df) <- c("lon", "lat", "landcover")

  p <- ggplot(modis_df) +
    geom_raster(aes(x = lon, y = lat, fill = factor(landcover))) +
    scale_fill_manual(values = c(
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
    ),
    labels = c(
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
      "Barren or Sparsely Vegetated",
      name = "Land Cover"
    ))+
    labs(title = "MODIS Land Cover (2010)") +
    scale_x_continuous(
      name = "Longitude",  # 横坐标标题
      expand = c(0, 0),
      limits = c(xmin, xmax),
      breaks = seq(xmin, xmax, by = 5)
    ) +
    scale_y_continuous(
      name = "Latitude",   # 纵坐标标题
      expand = c(0, 0),
      limits = c(ymin, ymax),
      breaks = seq(ymin, ymax, by = 5)
    ) +
    theme(legend.position = "right",
          legend.text = element_text(size = 6),
          legend.title = element_blank())

  rm(modis, landcover, landcover_crop, modis_df)
  gc()
  return(p)
}

plot_landcover2 <- function(cci_landcover_path, xmin, xmax, ymin, ymax, x_breaks = 5, y_breaks = 5) {

  # Load and crop the raster
  lc <- rast(cci_landcover_path)
  print(names(lc))
  lccs_class <- lc[["lccs_class"]]
  extent_crop <- ext(xmin, xmax, ymin, ymax)
  landcover_crop <- crop(lccs_class, extent_crop)

  # Convert to categorical raster and assign labels/colors
  landcover_crop <- as.factor(landcover_crop)
  levels(landcover_crop)[[1]] <- data.frame(
    value = c(0, 10, 11, 12, 20, 30, 40, 50, 60, 61, 62,
              70, 71, 72, 80, 81, 82, 90, 100, 110, 120,
              121, 122, 130, 140, 150, 151, 152, 153, 160,
              170, 180, 190, 200, 201, 202, 210, 220),
    label = c("No Data", "Cropland, rainfed", "Herbaceous cover", "Tree or shrub cover",
              "Cropland, irrigated or post-flooding",
              "Mosaic cropland >50% / natural vegetation <50%",
              "Mosaic natural vegetation >50% / cropland <50%",
              "Tree broadleaf evergreen", "Tree broadleaf deciduous",
              "Tree broadleaf deciduous closed", "Tree broadleaf deciduous open",
              "Tree needleleaf evergreen", "Tree needleleaf evergreen closed",
              "Tree needleleaf evergreen open", "Tree needleleaf deciduous",
              "Tree needleleaf deciduous closed", "Tree needleleaf deciduous open",
              "Tree mixed leaf type", "Mosaic tree/shrub >50%",
              "Mosaic herbaceous >50%", "Shrubland",
              "Evergreen shrubland", "Deciduous shrubland", "Grassland",
              "Lichens and mosses", "Sparse vegetation <15%", "Sparse tree <15%",
              "Sparse shrub <15%", "Sparse herbaceous <15%",
              "Tree flooded fresh/brackish", "Tree flooded saline",
              "Shrub/herb flooded", "Urban areas", "Bare areas",
              "Consolidated bare", "Unconsolidated bare",
              "Water bodies", "Snow and ice"),
    color = c("#000000", "#FFFF64", "#FFFF64", "#FFFF00", "#AAF0F0", "#DCF064", "#C8C864",
              "#006400", "#00A000", "#00A000", "#AAC800", "#003C00", "#003C00", "#005000",
              "#285000", "#285000", "#326400", "#788000", "#8CA000", "#BE9600", "#966400",
              "#966400", "#966400", "#FFB432", "#FFDCD6", "#FFEBAF", "#FFC864", "#FFD278",
              "#FFEBAF", "#00785A", "#009678", "#00DC82", "#C31400", "#FFF5D7", "#DCDCDC",
              "#FFF5D7", "#0046C8", "#FFFFFF")
  )

  print(levels(landcover_crop))

  # Plot with tidyterra and ggplot2
  p <- ggplot() +
    geom_spatraster(data = landcover_crop) +
    scale_fill_manual(
      values = setNames(levels(landcover_crop)[[1]]$color,
                        levels(landcover_crop)[[1]]$value),
      labels = levels(landcover_crop)[[1]]$label,
      name = "Land Cover Class"
    ) +
    scale_x_continuous(
      name = "Longitude",
      expand = c(0, 0),
      limits = c(xmin, xmax),
      breaks = seq(xmin, xmax, length.out = x_breaks)
    ) +
    scale_y_continuous(
      name = "Latitude",
      expand = c(0, 0),
      limits = c(ymin, ymax),
      breaks = seq(ymin, ymax, length.out = y_breaks)
    ) +
    labs(title = "CCI Land Cover Classification (2020)") +
    theme_classic() +
    theme(
      plot.title = element_text(face = "bold"),
      legend.position = "none"  # Set to "right" to enable legend
    )

  # Clean memory
  rm(lc, lccs_class, extent_crop)
  gc(verbose = FALSE)

  return(p)
}



plot_biomes_by_extent <- function(ecoregions_path, xmin, xmax, ymin, ymax, x_breaks = 5, y_breaks = 5) {
  # Load the ecoregions shapefile
  suppressMessages(ecoregions <- sf::st_read(ecoregions_path, quiet = TRUE))

  # Fix invalid geometries
  ecoregions <- sf::st_make_valid(ecoregions)

  # Build the plot (no cropping, just setting visible extent)
  p <- ggplot(data = ecoregions) +
    geom_sf(aes(fill = BIOME_NAME), color = NA) +
    scale_fill_manual(
      values = setNames(ecoregions$COLOR_BIO, ecoregions$BIOME_NAME)
    ) +
    scale_x_continuous(
      name = "Longitude",
      expand = c(0, 0),
      limits = c(xmin, xmax),
      breaks = seq(xmin, xmax, by = x_breaks)
    ) +
    scale_y_continuous(
      name = "Latitude",
      expand = c(0, 0),
      limits = c(ymin, ymax),
      breaks = seq(ymin, ymax, by = y_breaks)
    ) +
    labs(title = "Biomes", fill = "Biome") +
    theme_classic() +
    theme(
      legend.position = "none",
      plot.title = element_text(face = "bold"),
      aspect.ratio = (ymax - ymin) / (xmax - xmin)
    )

  return(p)
}

# plot Google satellite imagine by ext
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

# plot Pearson correlation (between VEGH and TWI) for by dataset
plot_corr <- function(correlation_df, xmin, xmax, ymin, ymax, x_breaks = 5, y_breaks = 5) {
  # Select relevant columns and drop rows with NA values
  df <- correlation_df |>
    dplyr::select(lon_mid, lat_mid, correlation) |>
    drop_na()  # Remove rows with missing correlation values

  # Compute summary statistics
  corr_mean <- round(mean(df$correlation, na.rm = TRUE), 3)
  corr_q25 <- round(quantile(df$correlation, 0.25, na.rm = TRUE), 3)
  corr_q75 <- round(quantile(df$correlation, 0.75, na.rm = TRUE), 3)

  # Build subtitle text
  subtitle_text <- paste0(
    "Mean = ", corr_mean,
    ";  Q25 = ", corr_q25,
    ";  Q75 = ", corr_q75
  )

  # Create plot
  p <- ggplot(df, aes(x = lon_mid, y = lat_mid, fill = correlation)) +
    geom_raster() +
    scale_fill_scico(
      palette = "bam",
      midpoint = 0,
      limits = c(min(df$correlation, na.rm = TRUE),
                 max(df$correlation, na.rm = TRUE)),
      name = expression(r[TWI,VEGH])
    ) +
    labs(title = "TWI–VEGH Correlation Analysis",
         subtitle = subtitle_text,
         fill = "Correlation") +
    scale_x_continuous(
      name = "Longitude",
      expand = c(0, 0),
      limits = c(xmin, xmax),
      breaks = seq(xmin, xmax, by = x_breaks)
    ) +
    scale_y_continuous(
      name = "Latitude",
      expand = c(0, 0),
      limits = c(ymin, ymax),
      breaks = seq(ymin, ymax, by = y_breaks)
    ) +
    theme(
      legend.position = "right",
      legend.text = element_text(size = 6),
      legend.title = element_blank(),
      plot.title = element_text(face = "bold")
    )
  return(p)
}


# plot correlation (mark NA) with window size (how many pixels in it)
plot_correlation_vs_pixel_count <- function(correlation_df) {

  # data clean
  df <- correlation_df |>
    dplyr::select(n_obs, correlation)

  # Separate the rows where the correlation is NA
  na_data <- df |> filter(is.na(correlation))
  data_valid <- df |> filter(!is.na(correlation))

  ggplot() +
    # none NA point density
    geom_pointdensity(data = data_valid, aes(x = n_obs, y = correlation), adjust = 1.5) +
    scale_color_viridis_c() +

    # Red Cross NA Point
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

# 02_2 plot3 --> can delete
plot_window_distribution <- function(windowed_data) {
  n_windows <- length(unique(windowed_data$window_id))

  # Generate a random color palette
  set.seed(33)  # For reproducibility of the random colors
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

  # 计算 144 的比例
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

  # 标注 144 处的占比
  if (pct_144 > 80) {
    p <- p + annotate("text", x = 144, y = max(table(window_counts$pixel_count)),
                      label = paste0(round(pct_144, 1), "% at 144"),
                      color = "red", vjust = -1.5)
  }

  return(p)
}


plot_random_windows <- function(correlation_results, seed = 123) {
  set.seed(seed)

  valid_windows <- correlation_results |>
    filter(!is.na(correlation), n_obs >= 100)

  if (nrow(valid_windows) < 3) {
    stop("The number of available windows is less than 3, please check the data.")
  }

  selected_windows <- sample(valid_windows$window_id, 2)

  plots <- purrr::map(selected_windows, function(wid) {
    row <- valid_windows |> filter(window_id == wid)

    df <- row$data[[1]] |> filter(complete.cases(twi, vegh))

    corr <- round(row$correlation, 3)
    pval <- signif(row$cor_pval, 3)
    lon <- round(row$lon_mid, 4)
    lat <- round(row$lat_mid, 4)

    ggplot(df, aes(x = twi, y = vegh)) +
      geom_point(alpha = 0.6) +
      geom_smooth(method = "lm", color = "blue", linewidth = 1) +
      ggtitle(
        paste0("Window ", wid,
               "\nLon: ", lon, ", Lat: ", lat,
               "\nR = ", corr, ", p = ", pval)
      ) +
      labs(
        x = "Topographic Wetness Index (TWI)",
        y = "Vegetation Height (VEGH)"
      ) +
      theme_classic()


  })

  return(plots)
}

plot_overview <- function(windowed_data) {

  p <- ggplot(windowed_data, aes(x = twi, y = vegh)) +
    geom_hex(bins = 50) +  # 调整 bins 以控制六边形大小
    scale_fill_scico(palette = "batlow", name = "Pixel Count") +
    geom_smooth(method = "lm", color = "blue", linewidth = 1) +
    labs(
      title = "Regional Map of the Relationship\nBetween TWI and VEGH",
      x = "Topographic Wetness Index (TWI)",
      y = "Vegetation Height (VEGH)"
    ) +
    theme_classic() +
    theme(
      legend.position = "right",
      axis.title = element_text(size = 12),
      plot.title = element_text(size = 14, face = "bold")
    )

  return(p)
}


plot_peak <- function(correlation_df_peak) {
  ggplot(correlation_df_peak, aes(x = lon_mid, y = lat_mid, fill = factor(peak))) +
    geom_tile() +
    scale_fill_manual(
      values = c("0" = "lightblue", "1" = "darkred", "NA" = "grey"),
      na.translate = TRUE,
      name = "Peak"
    ) +
    coord_equal() +
    theme_classic() +
    labs(title = "Peak Distribution", x = "Lontitude", y = "Latitut")
}



save_combined_plot <- function(
    plots,
    region_name,
    title_text,
    ncol = 3,
    output_dir = here::here("data", "figures"),
    width = 20,
    height = 13,
    dpi = 300,
    file_index = ""
) {

  valid_plots <- keep(plots, ~ inherits(.x, "ggplot"))

  # Create the full title by combining region name and title text
  title_text_full <- paste0(region_name, " ", title_text)

  # Combine the plots with the title on top
  combined_plot <- cowplot::plot_grid(
    cowplot::ggdraw() + cowplot::draw_label(title_text_full, fontface = "bold", size = 20, x = 0, hjust = 0),
    cowplot::plot_grid(plotlist = valid_plots, ncol = ncol, align = "hv"),
    ncol = 1,
    rel_heights = c(0.05, 1)
  ) +
    theme(plot.background = element_rect(fill = "white", color = "white"))

  # Construct the output file path
  output_file <- file.path(
    output_dir,
    paste0(
      file_index,
      "_",
      normalize_string(region_name),
      "_",
      normalize_string(title_text),
      ".png"
    )
  )

  # Save the combined plot to a file
  ggplot2::ggsave(
    filename = output_file,
    plot = combined_plot,
    width = width,
    height = height,
    dpi = dpi,
    bg = "white"
  )

  message("✅ Plot saved to: ", output_file)
  return(output_file)
}
