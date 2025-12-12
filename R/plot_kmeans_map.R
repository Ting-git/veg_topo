plot_kmeans_map <- function(input, extent = NULL, title_text = "K-means cluster map (k=8)",
                            highlight_cluster = NULL,
                            text_size = 12, x_step = 30, y_step = 30) {

  # Reorder cluster_value and cluster_labels with an defined order
  load(here::here("data/cluster_data.RData")) # cluster_values, cluster_labels

  # fill_color for dry to wet cluster
  fill_colors <- c(
    "#E78AC3",  # Pink - Arid
    "#FC8D62",  # Orange - Semi-arid
    "#FFD92F",  # Yellow - Semi-arid
    "#E5C494",  # Light brown - Dry-sub-humid
    "#B3B3B3",  # Gray - Humid
    "#66C2A5", # Blue-green - Humid
    "#8DA0CB",  # Blue - Humid
    "#A6D854"   # Green - Humid
  )

  # Remove names from colors
  fill_colors <- unname(fill_colors)

  # ---- Load raster ----
  if (is.character(input)) input <- terra::rast(input)
  if (!inherits(input, "SpatRaster")) stop("Input must be a SpatRaster or valid file path.")

  # ---- Handle extent and optional cropping ----
  if (is.null(extent)) {
    extent <- terra::ext(input)
  } else if (!inherits(extent, "SpatExtent")) {
    stop("`extent` must be a SpatExtent object from terra::ext().")
  } else {
    # Crop raster if extent is smaller
    area_in <- (terra::xmax(input) - terra::xmin(input)) * (terra::ymax(input) - terra::ymin(input))
    area_ex <- (terra::xmax(extent) - terra::xmin(extent)) * (terra::ymax(extent) - terra::ymin(extent))

    if (area_ex < area_in) {
      cropped <- terra::crop(input, extent)
      if (all(is.na(terra::values(cropped)))) stop("Extent does not intersect raster.")
      input <- cropped
      message("Raster cropped to specified extent.")
    }
  }

  # ---- Extract extent boundaries ----
  xmin <- terra::xmin(extent)
  xmax <- terra::xmax(extent)
  ymin <- terra::ymin(extent)
  ymax <- terra::ymax(extent)

  # Get actual values present in the (possibly cropped) raster
  raster_vals <- terra::values(input) |> na.omit() |> unique() |> sort()

  # Convert to factor
  raster <- as.factor(input)

  # ---- Handle cluster labels ----
  # Get the actual categories present after cropping
  if (!is.null(cluster_labels)) {
    # Filter cluster_labels to only include those present in the cropped raster
    present_clusters <- as.character(raster_vals)
    present_labels <- cluster_labels[names(cluster_labels) %in% present_clusters]

    if (length(present_labels) > 0) {
      # Create levels dataframe with only present clusters
      levels_df <- data.frame(
        value = as.numeric(names(present_labels)),
        category = unname(present_labels)
      )
      levels(raster) <- levels_df
      final_levels <- as.character(levels(raster)[[1]]$category)
    } else {
      # If no cluster labels match, use numeric values
      final_levels <- as.character(raster_vals)
    }
  } else {
    # Use raster unique values directly if no labels provided
    final_levels <- as.character(raster_vals)
  }

  # ---- Create color and alpha mappings ----
  # Match colors to present levels
  if (!is.null(cluster_labels) && exists("present_labels")) {
    # Get the index order from cluster_labels
    color_index <- match(names(present_labels), names(cluster_labels))
    present_colors <- fill_colors[color_index]
  } else {
    # For numeric values, use colors based on sorted values
    color_index <- match(raster_vals, 1:length(fill_colors))
    present_colors <- fill_colors[color_index]
  }

  # Create named vectors for scale_* functions
  names(present_colors) <- final_levels

  # Handle highlighting
  if (!is.null(highlight_cluster)) {
    highlight_label <- if (!is.null(cluster_labels)) {
      as.character(cluster_labels[as.character(highlight_cluster)])
    } else {
      as.character(highlight_cluster)
    }

    alpha_values <- ifelse(final_levels == highlight_label, 1, 0.2)
  } else {
    alpha_values <- rep(1, length(final_levels))
  }
  names(alpha_values) <- final_levels

  # ---- Create ggplot ----
  p <- ggplot() +
    tidyterra::geom_spatraster(
      data = raster,
      aes(fill = after_stat(as.factor(value)), alpha = after_stat(as.factor(value))),
      maxcell = Inf
    ) +
    scale_fill_manual(
      values = present_colors,
      name = "Cluster",
      na.value = NA,
      na.translate = FALSE,
      guide = guide_legend(
        title.position = "left",
        label.position = "bottom",
        nrow = 1
      )
    ) +
    scale_alpha_manual(values = alpha_values, guide = "none") +
    labs(title = title_text) +
    ggplot2::scale_x_continuous(
      breaks = seq(from = xmin, to = xmax, by = x_step),
      expand = c(0, 0)
    ) +
    ggplot2::scale_y_continuous(
      breaks = seq(from = ymin, to = ymax, by = y_step),
      expand = c(0, 0)
    ) +
    ggplot2::coord_sf(
      xlim = c(xmin, xmax),
      ylim = c(ymin, ymax),
      expand = FALSE,
      clip = "off"
    ) +
    ggplot2::theme_bw(base_size = text_size) +
    ggplot2::theme(
      legend.position = "bottom",
      legend.box = "horizontal",
      legend.text = ggplot2::element_text(size = text_size * 0.9),
      legend.title = ggplot2::element_text(size = text_size),
      axis.title = ggplot2::element_text(size = text_size),
      axis.text = ggplot2::element_text(size = text_size * 0.9),
      plot.title = ggplot2::element_text(size = text_size * 1.2, face = "bold"),
      plot.title.position = "panel"
    )

  return(p)
}
