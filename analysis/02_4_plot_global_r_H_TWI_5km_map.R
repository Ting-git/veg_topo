# ---------- SetUp -------------------------------------------------------------
library(terra)
library(ggplot2)
library(tidyterra)
library(scico)
library(rnaturalearth)
library(sf)

source(here::here("config.R"))
source(here::here("R/plot_cor_twi_vegh.R"))
source(here::here("R/plot_cor_pval.R"))

# ---------- data pre ----------------------------------------------------------
# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# load correlation data, raster data
cor_r <- terra::rast(cor_twi_vegh_mosaic_file)

# ------- Plot global correlation analysis of TWI and VEGH ---------------------

p_cor <- plot_cor_twi_vegh(
  input = cor_r[[1]],
  extent = ext_global,
  title = "VEGH-TWI Pearson Correlation Map",
  text_size = 16,
  x_breaks = 30,
  y_breaks = 30
) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1)

# save
ggsave(
  filename = file.path(project_root, "data/figures/02_cor_twi_vegh_map.png"),
  plot = p_cor,
  width = 30,
  height = 15,
  dpi = 300,
  units = "in"
)

# --------- plot P value ----------------------------

p_pval <-  plot_cor_pval(
  input = pval_cor_twi_vegh_mosaic_file,
  extent = ext_global
) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1)

# save
ggsave(
  filename = file.path(project_root, "data/figures/02_cor_p_val_twi_vegh_map.png"),
  plot = p_pval,
  width = 30,
  height = 15,
  dpi = 300,
  units = "in"
)
