# ---------- SetUp -------------------------------------------------------------
library(terra)
library(ggplot2)
library(tidyterra)
library(scico)
library(rnaturalearth)
library(sf)

source(here::here("config.R"))
source(here::here("R/plot_r_H_R.R"))
source(here::here("R/plot_cor_pval.R"))

# ---- File Configuration ------------------------------------------------------

# UBELIX File Configuration ------------------------------------------------------
#
# sw_in_450m_tile_dir <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/1_1_deg_tiles")
# sw_in_450m_path <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_450m.nc")
# sw_in_flat_450m_path <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_flat_450m.nc")
# sw_in_terrain_effect_450m_path <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_terrain_effect_450m.nc")
# r_H_R_5km_path <- file.path("/storage/scratch/giub_geco/tting/global_r_H_R_5km/r_H_R_5km.nc")
# pval_r_H_R_5km_path <- file.path("/storage/scratch/giub_geco/tting/global_r_H_R_5km/pval_r_H_R_5km.nc")
# twi_450m_mosaic_clean_path <- file.path("/storage/scratch/giub_geco/tting/global_twi_450m_clean/ga2_clean.nc")
#
# Load data on ubelix
# sw_in_450m <- terra::rast(sw_in_450m_path)
# sw_in_flat_450m <- terra::rast(sw_in_flat_450m_path)
# sw_in_terrain_effect_450m <- terra::rast(sw_in_terrain_effect_450m_path)
# twi_450m <- terra::rast(twi_450m_mosaic_clean_path)
# r_H_R_5km <- terra::rast(r_H_R_5km_path)
# pval_r_H_R_5km <- terra::rast(pval_r_H_R_5km_path)


# ---------- data pre ----------------------------------------------------------
# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# load correlation data, raster data
r_H_R_5km <- terra::rast(r_H_R_5km_path)
pval_r_H_R_5km <- terra::rast(pval_r_H_R_5km_path)


r_H_R_5km
# ------- Plot global correlation analysis of TWI and VEGH ---------------------

p_r <- plot_r_H_R(
  input = r_H_R_5km,
  extent = ext_global,
  title = "Pearson Correlation: Vegetation Height vs. Surface Solar Radiation",
  text_size = 16,
  x_breaks = 30,
  y_breaks = 30
) + guides(fill = guide_colorbar(
  title.position = "left",
  barwidth = grid::unit(15, "in"),
  barheight = grid::unit(0.3, "in")
)) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1)

# save
ggsave(
  filename = file.path(project_root, "data/figures/05_r_H_R_5km_map.png"),
  plot = p_r,
  width = 30,
  height = 15,
  dpi = 300,
  units = "in"
)

# --------- plot P value ----------------------------

p_pval <-  plot_cor_pval(
  input = pval_r_H_R_5km,
  extent = ext_global,
  title_text = "Vegetation Height - Insolation Pearson Correlation: P-value Map"
) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1)

# save
ggsave(
  filename = file.path(project_root, "data/figures/05_pval_r_H_R_5km_map.png"),
  plot = p_pval,
  width = 30,
  height = 15,
  dpi = 300,
  units = "in"
)
