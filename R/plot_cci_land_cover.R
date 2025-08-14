plot_cci_land_cover <- function(cci_landcover_path, xmin, xmax, ymin, ymax, x_breaks = 5, y_breaks = 5) {

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
