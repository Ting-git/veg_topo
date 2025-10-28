# ---------- SetUp -------------------------------------------------------------
library(terra)
library(ggplot2)
library(tidyterra)
library(scico)
library(rnaturalearth)
library(sf)

# Automatically select configuration file
hostname <- trimws(tolower(system("hostname", intern = TRUE)))
if (hostname == "dash") {
  message("💻 Detected Worksation: dash → using config.R")
  source(here::here("config.R"))
} else {
  message("🖥️ Detected HPC environment (", hostname, ") → using config_ubelix.R")
  source(here::here("config_ubelix.R"))
}

source(here::here("R/plot_r_H_R.R"))
source(here::here("R/plot_cor_pval.R"))

# ---------- data pre ----------------------------------------------------------
# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# ------- Plot global correlation analysis of TWI and VEGH ---------------------

p_r <- plot_r_H_R(
  input = r_H_R_5km_path,
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
  filename = file.path(project_root, "data/figures/05_r_H_R_5km_map.png"),
  plot = p_r,
  width = 14,
  height = 7,
  dpi = 600,
  units = "in"
)

# --------- plot P value ----------------------------

p_pval <-  plot_cor_pval(
  input = pval_r_H_R_5km_path,
  extent = ext_global,
  title_text = "Pearson's p-value (H～Rᵢₙ)",
  text_size = 12,
  x_step = 30,
  y_step = 30
) +
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
  filename = file.path(project_root, "data/figures/05_r_H_R_5km_pval.png"),
  plot = p_pval,
  width = 14,
  height = 7,
  dpi = 600,
  units = "in"
)
