# ~ 6.5 min on UBELIX
# ---------- SetUp -------------------------------------------------------------

library(terra)
library(ggplot2)
library(tidyterra)
library(scico)
library(rnaturalearth)
library(sf)

# source(here::here("config.R"))
source(here::here("R/plot_sw_in.R"))
source(here::here("R/plot_terrain_effect.R"))

# ---------- File Configuration on UBELIX----------------------------------------------------------
sw_in_uneven_450m_path <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_450m.nc")
sw_in_flat_450m_path <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_flat_450m.nc")
sw_in_terrain_effect_450m_path <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_terrain_effect_450m.nc")

ext_global <- ext(-180, 180, -60, 85)

# ---------- Load Data ----------------------------------------------------------
# Load coastline vector data (for plotting reference only)
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# Load raster data
sw_in_flat_450m <- terra::rast(sw_in_flat_450m_path)
sw_in_uneven_450m <- terra::rast(sw_in_uneven_450m_path)
sw_in_terrain_effect_450m <- terra::rast(sw_in_terrain_effect_450m_path)

sw_in_flat_450m_agg <- terra::aggregate(sw_in_flat_450m, fact = 12)
sw_in_uneven_450m_agg <- terra::aggregate(sw_in_uneven_450m, fact = 12)
sw_in_terrain_effect_450m_agg <-  terra::aggregate(sw_in_terrain_effect_450m, fact = 12)

# ------- Plot incident solar radiation on flat surefce  -----------------------

p_flat <- plot_sw_in(
  input = sw_in_flat_450m_agg,
  extent = ext_global,
  title = "Incident Solar Radiation: Flat Surface",
  text_size = 16,
  x_breaks = 30,
  y_breaks = 30
) +
  guides(fill = guide_colorbar(
  title.position = "left",
  barwidth = grid::unit(15, "in"),
  barheight = grid::unit(0.3, "in")
)) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1)

# save
ggsave(
  filename = file.path(here::here("data/figures/05_flat_surface_sw_in_map.png")),
  plot = p_flat,
  width = 30,
  height = 15,
  dpi = 300,
  units = "in"
)

# ------- Plot incident solar radiation on uneven surefce  ---------------------

p_uneven <- plot_sw_in(
  input = sw_in_uneven_450m_agg,
  extent = ext_global,
  title = "Incident Solar Radiation: Uneven Surface",
  text_size = 16,
  x_breaks = 30,
  y_breaks = 30
) +
  guides(fill = guide_colorbar(
  title.position = "left",
  barwidth = grid::unit(15, "in"),
  barheight = grid::unit(0.3, "in")
)) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1)

# save
ggsave(
  filename = file.path(here::here("data/figures/05_uneven_surface_sw_in_map.png")),
  plot = p_uneven,
  width = 30,
  height = 15,
  dpi = 300,
  units = "in"
)

# ------- Plot Terrain Effect  ---------------------

p_te <- plot_terrain_effect(
  input = sw_in_terrain_effect_450m_agg,
  extent = ext_global,
  text_size = 16,
  x_breaks = 30,
  y_breaks = 30
) +
  guides(fill = guide_colorbar(
  title.position = "left",
  barwidth = grid::unit(15, "in"),
  barheight = grid::unit(0.3, "in")
)) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1)

# save
ggsave(
  filename = file.path(here::here("data/figures/05_terrain_effect_sw_in_map.png")),
  plot = p_te,
  width = 30,
  height = 15,
  dpi = 300,
  units = "in"
)



