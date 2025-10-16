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
  title = "VEGH-TWI Pearson Correlation Map",
  text_size = 12,
  x_step = 30,
  y_step = 30,
  fix_aspect = FALSE
) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1)

# save
ggsave(
  filename = file.path(project_root, "data/figures/02_r_H_TWI_5km_map.png"),
  plot = p_cor,
  width = 14,
  height = 7,
  dpi = 600,
  units = "in"
)

# --------- plot P value ----------------------------

p_pval <-  plot_cor_pval(
  input = pval_cor_twi_vegh_mosaic_file,
  extent = ext_global,
  text_size = 14,
  x_step = 30,
  y_step = 30,
  fix_aspect = FALSE
) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1)

# save
ggsave(
  filename = file.path(project_root, "data/figures/02_r_H_TWI_5km_pval.png"),
  plot = p_pval,
  width = 14,
  height = 7,
  dpi = 600,
  units = "in"
)
