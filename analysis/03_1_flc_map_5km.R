# ------------ Set Up ----------------------------------------------------------

library(ggplot2)
library(rnaturalearth)
library(sf)
library(RColorBrewer)
library(terra)
library(tidyterra)

# Load configuration and functions
source(here::here("config.R"))

# ------------ Load Data ----------------------------------------------------------

# Load land polygons
land <- rnaturalearth::ne_countries(scale = 110, returnclass = "sf")
land_vect <- vect(land)

# Load raster data
# Crop and mask raster to land
fused_r <- rast(fused_5km_file)
fused_r_masked <- mask(crop(fused_r, land_vect), land_vect)

# ------------ Plot and Save ----------------------------------------------------------

# Plot using masked raster
p <- ggplot() +
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

ggsave(
  filename = here::here("data/figures/03_flc_5km_map.png"),
  plot = p, width = 24, height = 11.5, dpi = 300, units = "in"
)

