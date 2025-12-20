# -----------------------Global configuration for project------------------------

# Paths
project_root <- "~/veg_topo"
veg_topo_extr_dir <- file.path("/storage/scratch/giub_geco/tting")

# Use it to create global map
ext_global <- terra::ext(-180, 180, -60, 85)

# Vegetation height
vegh_10m_tiles_dir <- file.path(veg_topo_extr_dir, "vegheight_lang_2023/data/3deg_cogs")  # Path for higher resolution data
vegh_450m_tiles_dir <- file.path(veg_topo_extr_dir, "global_vegh_450m/3_3_deg")
vegh_450m_mosaic_path <- file.path(veg_topo_extr_dir, "global_vegh_450m/vegh_450m_2020_mosaic.nc")

# TWI
twi_450m_mosaic_clean_path <- file.path(veg_topo_extr_dir, "global_twi_450m_clean/ga2_clean.nc")
twi_30m_path <- file.path(veg_topo_extr_dir, "twi_ho_2025/data/twi_edtm_m_30m_v20241230.tif")

dem_30m_copernicus_dir <- file.path(veg_topo_extr_dir, "copernicus_dem_30m/copernicus_dem_30m")

dem_450m_tiles_dir <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/dem_1_1_deg")
slope_450m_tiles_dir <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/slope_1_1_deg")
aspect_450m_tiles_dir <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/aspect_1_1_deg")

dem_450m_mosaic_path <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/dem_450m1.nc")
slope_450m_mosaic_path <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/slope_450m1.nc")
aspect_450m_mosaic_path <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/aspect_450m1.nc")

# Radiation
sw_in_450m_tile_dir <- file.path(veg_topo_extr_dir, "global_sw_in_450m/1_1_deg_tiles")
sw_in_uneven_450m_path <- file.path(veg_topo_extr_dir, "global_sw_in_450m/sw_in_uneven_450m.nc")
sw_in_flat_450m_path <- file.path(veg_topo_extr_dir, "global_sw_in_450m/sw_in_flat_450m.nc")
sw_in_terrain_effect_450m_path <- file.path(veg_topo_extr_dir, "global_sw_in_450m/sw_in_terrain_effect_450m.nc")

# Fraction of land cover type
fused_5km_file <- file.path(veg_topo_extr_dir, "global_flc_5km/fused_5km.nc")
fbare_5km_file <- file.path(veg_topo_extr_dir, "global_flc_5km/fbare_5km.nc")
fwater_5km_file <- file.path(veg_topo_extr_dir, "global_flc_5km/fwater_5km.nc") # fraction of water body
fsnow_5km_file <- file.path(veg_topo_extr_dir, "global_flc_5km/fsnow_5km.nc") # fraction of permanent snow and ice

mi_5km_file <- file.path(veg_topo_extr_dir, "global_mi_5km/mi_5km.nc")

valid_tiles_info_path <- file.path(project_root,"data/valid_tiles_info.rds")
valid_tiles_vect_path <- file.path(project_root, "data/valid_tiles.gpkg")

# Global correlation (H~TWI)
cor_twi_vegh_tiles_dir <- file.path(veg_topo_extr_dir, "global_cor_twi_vegh/30_30_deg")
cor_twi_vegh_mosaic_file <- file.path(dirname(cor_twi_vegh_tiles_dir),"cor_twi_vegh_5km_mosaic.nc")
pval_cor_twi_vegh_mosaic_file <- file.path(dirname(cor_twi_vegh_tiles_dir),"pval_cor_twi_vegh_5km_mosaic.nc")

cor_twi_vegh_mask_fused0.05_file <- file.path(dirname(cor_twi_vegh_tiles_dir),"cor_twi_vegh_5km_mask_fused0.05.nc")
kmeans_map_8c_path <- file.path(veg_topo_extr_dir, "global_kmeans_5km/kmeans_corth_fused_mi_5km_8c.nc")

# Global correlation (H~R)
r_H_R_tiles_dir <-  file.path(veg_topo_extr_dir, "global_r_H_R_5km/30_30_deg")
r_H_R_5km_path <- file.path(veg_topo_extr_dir, "global_r_H_R_5km/r_H_R_5km.nc")
pval_r_H_R_5km_path <- file.path(veg_topo_extr_dir, "global_r_H_R_5km/pval_r_H_R_5km.nc")

# Mean Annual temperature
mat_55km_file <- file.path(veg_topo_extr_dir, "global_mat_55km/global_mat_55km_1970_2000.nc")
mat_5km_file <- file.path(veg_topo_extr_dir, "global_mat_5km/global_mat_5km_1970_2000.nc")

# random sample region extent
reg_sample_dir <- file.path(veg_topo_extr_dir, "reg_sample")

reg_sample_info_path <- file.path(reg_sample_dir, "reg_sample_info.rds")
reg_sample_vect_path <- file.path(reg_sample_dir, "reg_sample_vect.gpkg")

regA_sample_info_path <- file.path(reg_sample_dir, "regA_sample_info.rds")
regA_sample_vect_path <- file.path(reg_sample_dir, "regA_sample_vect.gpkg")

regB_sample_info_path <- file.path(reg_sample_dir, "regB_sample_info.rds")
regB_sample_vect_path <- file.path(reg_sample_dir, "regB_sample_vect.gpkg")

# random sample region result
reg_correlation_dir <- file.path(veg_topo_extr_dir, "reg_correlation_500m")
regA_cor_twi_vegh_dir <- file.path(veg_topo_extr_dir, "regA_cor_twi_vegh_500m")
regB_r_R_H_dir <- file.path(veg_topo_extr_dir, "regB_r_R_H_500m")

# validation regions result
reg_validate_dir <- file.path(veg_topo_extr_dir, "reg_validate_500m")

# -----------------------Additional Data------------------------------------------
ecoregion_path <- file.path(veg_topo_extr_dir, "ecoregion2017/Ecoregions2017/Ecoregions2017.shp")

# Köppen–Geiger Climate Classification
kg_present_0p0083_file <- file.path(veg_topo_extr_dir, "koeppengeiger_beck_2018/data/Beck_KG_V1_present_0p0083.tif")
kg_present_0p083_file <- file.path(veg_topo_extr_dir, "koeppengeiger_beck_2018/data/Beck_KG_V1_present_0p083.tif")
kg_legend_file <- file.path(veg_topo_extr_dir, "koeppengeiger_beck_2018/data/legend.txt")

