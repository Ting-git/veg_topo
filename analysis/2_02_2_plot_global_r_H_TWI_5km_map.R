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

source(here::here("R/plot_cor_twi_vegh.R"))
source(here::here("R/plot_cor_pval.R"))

# ---------- data pre ----------------------------------------------------------
# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")


# ------- Plot global correlation analysis of TWI and VEGH ---------------------

p_cor <- plot_cor_twi_vegh(
  input = cor_twi_vegh_mosaic_file,
  extent = ext_global,
  title_text = bquote("5-km Pearson's " * r[.("H")*","*.("TWI")]),
  text_size = 14,
  x_step = 30,
  y_step = 30,
  land_color = "#EFECE4"
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
    panel.background = element_rect(fill = "white", color = NA),
    plot.background  = element_blank(),
    legend.background = element_blank(),
    legend.box.background = element_blank()
  )

# save
ggsave(
  filename = file.path(project_root, "data/figures/2_02_r_H_TWI_5km_map.png"),
  plot = p_cor,
  width = 14,
  height = 6,
  dpi = 600,
  units = "in"
)


# --------- plot P value ----------------------------

p_pval <- plot_cor_pval(
  input = pval_cor_twi_vegh_mosaic_file,
  extent = ext_global,
  title_text = "5-km Pearson's p-value (H~TWI)",
  text_size = 14,
  x_step = 30,
  y_step = 30
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
    panel.background = element_rect(fill = "white", color = NA),
    plot.background  = element_blank(),
    legend.background = element_blank(),
    legend.box.background = element_blank()
  )

# save
ggsave(
  filename = file.path(project_root, "data/figures/2_02_r_H_TWI_5km_pval.png"),
  plot = p_pval,
  width = 14,
  height = 5.8,
  dpi = 600,
  units = "in"
)
