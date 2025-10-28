# ~ 6.5 min on UBELIX
# ---------- SetUp -------------------------------------------------------------

library(terra)
library(ggplot2)
library(tidyterra)
library(scico)
library(rnaturalearth)
library(sf)

# Detect host and load configuration
hostname <- trimws(tolower(system("hostname", intern = TRUE)))

if (hostname == "dash") {
  message("💻 Workstation detected ('dash') → loading config.R")
  source(here::here("config.R"))
} else {
  message("🖥️ HPC environment detected (", hostname, ") → loading config_ubelix.R")
  source(here::here("config_ubelix.R"))
}
source(here::here("R/plot_sw_in.R"))
source(here::here("R/plot_rin.R"))

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
  title = "Flat Surface Solar Radiation",
  text_size = 12,
  x_step = 30,
  y_step = 30
) +
  guides(fill = guide_colorbar(
    title.position = "left",
    barwidth = grid::unit(0.1, "in"),
    barheight = grid::unit(5, "in")
  )) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1) +
  coord_sf(
    xlim = c(terra::xmin(ext_global), terra::xmax(ext_global)),
    ylim = c(terra::ymin(ext_global), terra::ymax(ext_global)),
    expand = FALSE,
    clip = "on"
  )

# save
ggsave(
  filename = file.path(here::here("data/figures/05_flat_surface_sw_in_map.png")),
  plot = p_flat,
  width = 14,
  height = 7,
  dpi = 600,
  units = "in"
)

# ------- Plot incident solar radiation on uneven surefce  ---------------------

p_uneven <- plot_sw_in(
  input = sw_in_uneven_450m_agg,
  extent = ext_global,
  title = "Surface Solar Radiation",
  text_size = 12,
  x_step = 30,
  y_step = 30
) +
  guides(fill = guide_colorbar(
    title.position = "left",
    barwidth = grid::unit(0.1, "in"),
    barheight = grid::unit(5, "in")
  )) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1) +
  coord_sf(
    xlim = c(terra::xmin(ext_global), terra::xmax(ext_global)),
    ylim = c(terra::ymin(ext_global), terra::ymax(ext_global)),
    expand = FALSE,
    clip = "on"
  )

# save
ggsave(
  filename = file.path(here::here("data/figures/05_uneven_surface_sw_in_map.png")),
  plot = p_uneven,
  width = 14,
  height = 7,
  dpi = 600,
  units = "in"
)

# ------- Plot Terrain Effect  ---------------------

p_te <- plot_rin(
  input = sw_in_terrain_effect_450m_agg,
  extent = ext_global,
  text_size = 12,
  x_step = 30,
  y_step = 30
) +
  guides(fill = guide_colorbar(
  title.position = "left",
  barwidth = grid::unit(0.1, "in"),
  barheight = grid::unit(5, "in")
)) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1)+
  coord_sf(
    xlim = c(terra::xmin(ext_global), terra::xmax(ext_global)),
    ylim = c(terra::ymin(ext_global), terra::ymax(ext_global)),
    expand = FALSE,
    clip = "on"
  )

# save
ggsave(
  filename = file.path(here::here("data/figures/05_terrain_effect_sw_in_map.png")),
  plot = p_te,
  width = 14,
  height = 7,
  dpi = 600,
  units = "in"
)



