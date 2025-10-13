plot_cci_land_cover <- function(cci_landcover_path, extent = NULL,
                                title_text = "Land Cover Type",
                                text_size = 6, x_step = 5, y_step = 5) {

  # Load raster
  lc <- terra::rast(cci_landcover_path)
  lccs_class <- lc[["lccs_class"]]

  # Handle extent
  if (!inherits(extent, "SpatExtent")) {
    stop("`extent` must be a SpatExtent object.")
  }

  # Crop raster to extent
  landcover_crop <- terra::crop(lccs_class, extent)

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

  # Plot
  p <- ggplot() +
    tidyterra::geom_spatraster(data = landcover_crop) +
    scale_fill_manual(
      values = setNames(levels(landcover_crop)[[1]]$color,
                        levels(landcover_crop)[[1]]$value),
      labels = levels(landcover_crop)[[1]]$label,
      name = "Land Cover Class"
    ) +
    ggplot2::labs(
      title = title_text,
      x = "Longitude",
      y = "Latitude",
      fill = NULL
    ) +
    ggplot2::scale_x_continuous(
      limits = c(terra::xmin(extent), terra::xmax(extent)),
      breaks = seq(terra::xmin(extent), terra::xmax(extent), by = x_step),
      expand = c(0, 0)
    ) +
    ggplot2::scale_y_continuous(
      limits = c(terra::ymin(extent), terra::ymax(extent)),
      breaks = seq(terra::ymin(extent), terra::ymax(extent), by = y_step),
      expand = c(0, 0)
    ) +
    ggplot2::theme_bw(base_size = text_size) +
    ggplot2::theme(
      legend.position = "none",
      legend.text = ggplot2::element_text(size = text_size),
      legend.title = ggplot2::element_text(size = text_size, face = "bold"),
      axis.title = ggplot2::element_text(size = text_size),
      axis.text = ggplot2::element_text(size = text_size * 0.9),
      plot.title = ggplot2::element_text(size = text_size * 1.2, face = "bold"),
      plot.title.position = "panel"
    )

  return(p)
}
