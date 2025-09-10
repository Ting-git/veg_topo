# ---------- SetUp -------------------------------------------------------------
library(terra)
# library(ggplot2)
# library(tidyterra)
library(scico)
# library(rnaturalearth)
# library(sf)

# source(here::here("config.R"))
# source(here::here("R/plot_cor_twi_vegh.R"))
# source(here::here("R/plot_cor_pval.R"))

# ---- File Configuration ------------------------------------------------------

# File Configuration ------------------------------------------------------

sw_in_450m_tile_dir <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/1_1_deg_tiles")
twi_450m_mosaic_clean_path <- file.path("/storage/scratch/giub_geco/tting/global_twi_450m_clean/ga2_clean.nc")
r_H_R_5km_path <- file.path("/storage/scratch/giub_geco/tting/global_r_H_R_5km/r_H_R_5km.nc")
pval_r_H_R_5km_path <- file.path("/storage/scratch/giub_geco/tting/global_r_H_R_5km/pval_r_H_R_5km.nc")

# Output file paths
sw_in_450m_path <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_450m.nc")
sw_in_flat_450m_path <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_flat_450m.nc")
sw_in_terrain_effect_450m_path <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_terrain_effect_450m.nc")

sw_in_450m <- terra::rast(sw_in_450m_path)
sw_in_flat_450m <- terra::rast(sw_in_flat_450m_path)
sw_in_terrain_effect_450m <- terra::rast(sw_in_terrain_effect_450m_path)
# twi_450m <- terra::rast(twi_450m_mosaic_clean_path)
r_H_R_5km <- terra::rast(r_H_R_5km_path)
# pval_r_H_R_5km <- terra::rast(pval_r_H_R_5km_path)

# sw_in_450m
# twi_450m

# plot(sw_in_450m)
# plot(sw_in_flat_450m)
# plot(sw_in_terrain_effect_450m)
# plot(twi_450m)
# plot(pval_r_H_R_5km)

# terra::plot(sw_in_flat_450m,
#             main = "Solar Radiation: Flat Surface",
#             col = scico(100, palette = "roma", direction = -1),
#             plg = list(title = "MJ/m²"))
#
# terra::plot(sw_in_450m,
            # main = "Solar Radiation: Terrain Modified",
            # col = scico(100, palette = "roma", direction = -1),
            # plg = list(title = "MJ/m²"))


terra::plot(sw_in_terrain_effect_450m,
            main = "Terrain Effect on Radiation",
            col = scico(100, palette = "cork",),
            plg = list(title = "Δ Radiation"))

terra::plot(r_H_R_5km,
            main = "Vegetation-Solar Radiation Correlation",
            col = scico(100, palette = "vik"),
            plg = list(title = "r-value"))

# ---------- data pre ----------------------------------------------------------
# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# load correlation data, raster data
r_H_R_5km <- terra::rast(r_H_R_5km_path)
pval_r_H_R_5km <- terra::path(pval_r_H_R_5km_path)

ext <- terra::ext(-180, 180, -60, 85)

# ------- Plot global correlation analysis of TWI and VEGH ---------------------

p_cor <- plot_cor_twi_vegh(
  input = cor_r[[1]],
  extent = ext,
  title = "VEGH-RAD Pearson Correlation Map",
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
  width = 24,
  height = 11.5,
  dpi = 300,
  units = "in"
)
# --------- plot P value ----------------------------

p_pval <-  plot_cor_pval(
  input = pval_cor_twi_vegh_mosaic_file,
  extent = ext
) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1)

# save
ggsave(
  filename = file.path(project_root, "data/figures/02_cor_p_val_twi_vegh_map.png"),
  plot = p_pval,
  width = 24,
  height = 11.5,
  dpi = 300,
  units = "in"
)
