# ==============================================================================
# Script: plot_fraction_land_use_maps.R
# Author: Ting Tan
# Date: 2025-08-31
#
# Description:
# This script visualizes global fraction land use layers derived from the
# processing pipeline. It performs the following tasks:
#   1. Loads configuration, shapefiles, and raster outputs
#   2. Crops and masks rasters to land areas
#   3. Produces publication-ready maps of:
#        - Fraction of used land (fused)
#        - Fraction of water bodies, permanent snow, and ice (fwi)
#   4. Saves figures as high-resolution PNG files
#
# Dependencies:
#   - ggplot2, rnaturalearth, sf, RColorBrewer, khroma, terra, tidyterra
#   - config.R (for file paths and global settings)
# ==============================================================================

# ------------------------- Set Up ---------------------------------------------
library(ggplot2)
library(rnaturalearth)
library(sf)
library(RColorBrewer)
library(khroma)
library(terra)
library(tidyterra)

# Load configuration (file paths etc.)
source(here::here("config.R"))

# ------------------------- Load Data ------------------------------------------
# Load coastlines (vector data)
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# Load land polygons
land <- rnaturalearth::ne_countries(scale = 110, returnclass = "sf")
land_vect <- vect(land)

# Load raster outputs and mask to land extent
fused_r <- rast(fused_5km_file)
fused_r_masked <- mask(crop(fused_r, land_vect), land_vect)

fwi_r <- rast(fwi_5km_file)
fwi_r_masked <- mask(crop(fwi_r, land_vect), land_vect)

# ------------------------- Plot and Save --------------------------------------
# --- Plot fused (fraction of used land) ---
p_fused <- ggplot() +
  tidyterra::geom_spatraster(data = fused_r_masked, maxcell = Inf) +
  geom_sf(data = coast, color = 'black', linewidth = 0.1) +
  scale_fill_gradientn(
    colours = rev(brewer.pal(7, "Spectral")),
    name = "Fused",
    na.value = NA,
    guide = guide_colorbar(
      title.position = "left",
      label.position = "bottom",
      direction = "horizontal",
      barwidth = unit(10, "cm"),
      barheight = unit(0.5, "cm")
    )
  ) +
  labs(title = "Fraction of Used Land") +
  scale_x_continuous(
    expand = c(0, 0),
    breaks = seq(-180, 180, by = 30)
  ) +
  scale_y_continuous(
    expand = c(0, 0),
    limits = c(-60, 85),
    breaks = seq(-60, 90, by = 30)
  ) +
  theme_bw() +
  theme(
    plot.title = element_text(size = 24, face = "bold", hjust = 0, margin = margin(b = 5)),
    plot.title.position = "panel",
    axis.title = element_text(size = 18),
    axis.text = element_text(size = 14),
    legend.position = "bottom",
    legend.box = "horizontal",
    legend.text = element_text(size = 14),
    legend.title = element_text(size = 16, face = "bold", margin = margin(r = 10))
  )

# Save fused map
ggsave(
  filename = here::here("data/figures/03_fused_5km_map.png"),
  plot = p_fused, width = 24, height = 11.5, dpi = 300, units = "in"
)

# --- Plot fwi (fraction of water, snow, and ice) ---
p_fwi <- ggplot() +
  tidyterra::geom_spatraster(data = fwi_r_masked, maxcell = Inf) +
  geom_sf(data = coast, color = 'black', linewidth = 0.1) +
  scale_fill_viridis_c(
    option = "G",
    name = "Water & Ice Proportion",
    na.value = NA,
    guide = guide_colorbar(
      title.position = "left",
      label.position = "bottom",
      direction = "horizontal",
      barwidth = unit(10, "cm"),
      barheight = unit(0.5, "cm")
    )
  ) +
  labs(title = "Fraction of Water Bodies & Permanent Snow and Ice") +
  scale_x_continuous(
    expand = c(0, 0),
    breaks = seq(-180, 180, by = 30)
  ) +
  scale_y_continuous(
    expand = c(0, 0),
    limits = c(-60, 85),
    breaks = seq(-60, 90, by = 30)
  ) +
  theme_bw() +
  theme(
    plot.title = element_text(size = 24, face = "bold", hjust = 0, margin = margin(b = 5)),
    plot.title.position = "panel",
    axis.title = element_text(size = 18),
    axis.text = element_text(size = 14),
    legend.position = "bottom",
    legend.box = "horizontal",
    legend.text = element_text(size = 14),
    legend.title = element_text(size = 16, face = "bold", margin = margin(r = 10))
  )

# Save fwi map
ggsave(
  filename = here::here("data/figures/03_fwi_5km_map.png"),
  plot = p_fwi, width = 24, height = 11.5, dpi = 300, units = "in"
)
