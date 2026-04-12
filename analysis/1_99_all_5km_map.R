# ==============================================================================
# Script: plot_fraction_land_use_maps.R
#
# Purpose:
#   Visualize global fraction land cover layers and annual mean temperature (MAT)
#   using rasters produced from the processing pipeline.
#
# Runtime:
#   ~ 10 mins
# ==============================================================================

# ------------------------- 1. Setup ---------------------------------------------
library(terra)
library(sf)
library(ggplot2)
library(rnaturalearth)
library(RColorBrewer)
library(tidyterra)
library(here)

# Load configuration and plotting helper
source(here::here("R/config.R"))
source(here::here("R/plot_var.R"))

message("🌍 Starting visualization maps...")

# ------------------------- 2. Load Data ----------------------------------------
# Load coastlines and land polygons
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")
land  <- rnaturalearth::ne_countries(scale = 110, returnclass = "sf")
land_vect <- vect(land)

# ------------------------- 3. Plot & Save --------------------------------------
plot_map <- function(input, title_text, output_file) {
  r_layer <- rast(input)
  #  r_layer <- mask(crop(r_layer, land_vect), land_vect)
  p <- plot_var(
    input     = r_layer,
    title_text = title_text,
    extent    = ext_global,
    text_size = 12,
    x_step    = 30,
    y_step    = 30
  ) +
    guides(fill = guide_colorbar(
      title.position = "left",
      barwidth = grid::unit(0.2, "in"),
      barheight = grid::unit(5.3, "in")
    )) +
    geom_sf(data = coast,
            colour = 'black',
            linewidth = 0.1) +
    coord_sf(
      xlim = c(terra::xmin(ext_global), terra::xmax(ext_global)),
      ylim = c(terra::ymin(ext_global), terra::ymax(ext_global)),
      expand = FALSE,
      clip = "on"
    ) +
    ggplot2::theme(
      legend.margin = margin(0, 0, 0, 0),
      legend.box.margin = margin(0, 0, 0, -10),
      axis.title.x = ggplot2::element_blank(),
      axis.title.y = ggplot2::element_blank(),
      panel.spacing = unit(0, "pt"),
      plot.margin = margin(t = 0, r = 0, b = 0, l = 0, unit = "pt"),
      panel.background = element_rect(fill = "white", color = NA),  # panel 内部白色
      plot.background  = element_blank(),                            # plot 外部透明
      legend.background = element_blank(),
      legend.box.background = element_blank()
    )

  ggsave(
    filename = output_file,
    plot     = p,
    width    = 14,
    height   = 6,
    dpi      = 600,
    units    = "in"
  )

  if (file.exists(output_file)) message("✅ Saved map: ", output_file)
}

# Plot maps
plot_map(fused_5km_file, "Fraction of used land", here::here("data/figures/1_99_fused_5km_map.png"))
plot_map(fbare_5km_file, "Bare land fraction", here::here("data/figures/1_99_fbare_5km_map.png"))
plot_map(fwater_5km_file, "Water body fraction", here::here("data/figures/1_99_fwater_5km_map.png"))
plot_map(fsnow_5km_file, "Permanent snow and ice fraction", here::here("data/figures/1_99_fsnow_5km_map.png"))
plot_map(mat_5km_file, "Annual mean surface temperature (℃)", here::here("data/figures/1_99_mat_5km_map.png"))
plot_map(map_5km_file, "Annual mean precipitation (mm)", here::here("data/figures/1_99_map_5km_map.png"))
plot_map(srad_5km_file, "Incident solar radiation (kJ m⁻² day⁻¹)", here::here("data/figures/1_99_srad_5km_map.png"))
plot_map(ecoregion_5km_path, "Biomes distribution", here::here("data/figures/1_99_biome_5km_map.png"))
