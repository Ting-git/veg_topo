
# -----------------------Global configuration for project------------------------

# Paths
project_root <- "~/veg_topo"
veg_topo_extr_dir <- file.path("/storage/scratch/giub_geco/tting")

# Use it to create global map
ext_global <- terra::ext(-180, 180, -60, 85)

# ----------------------- NOTES ------------------------
# - project_root:
#   Used only for storage of source code and output figures.
#
# - veg_topo_extr_dir:
#   Stores large datasets, including all raw, intermediate, and output data.
#
# - Raw data locations:
#   - veg_topo_extr_dir/ (excluding global_* and reg_*)
#
# - Intermediate and output data:
#   - veg_topo_extr_dir/global_*   # global-scale intermediate/output files
#   - veg_topo_extr_dir/reg_*      # regional-scale intermediate/output files
#   - veg_topo_extr_dir/data/      # structured intermediate data and output figures

# -----------------------Data Raw and Data (Pre-)Process--------------------------------

# 1_01 Topographic Wetness Index
twi_30m_path <- file.path(veg_topo_extr_dir, "twi_ho_2025/data/twi_edtm_m_30m_v20241230.tif")
twi_450m_mosaic_clean_path <- file.path(veg_topo_extr_dir, "global_twi_450m_clean/ga2_clean.nc")

# 1_02 Vegetation height
vegh_10m_tiles_dir <- file.path(veg_topo_extr_dir, "vegheight_lang_2023/data/3deg_cogs")  # Path for higher resolution data
vegh_450m_tiles_dir <- file.path(veg_topo_extr_dir, "global_vegh_450m/3_3_deg")
vegh_450m_mosaic_path <- file.path(veg_topo_extr_dir, "global_vegh_450m/vegh_450m_2020_mosaic.nc")
fveg_55km_path <- file.path(veg_topo_extr_dir, "global_fveg_55km/fveg_55km.nc")

# 1_03 Elevation
dem_30m_copernicus_dir <- file.path(veg_topo_extr_dir, "copernicus_dem_30m/copernicus_dem_30m")

  # elevation, slope, and aspect at 450 m
dem_450m_tiles_dir <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/dem_1_1_deg")
slope_450m_tiles_dir <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/slope_1_1_deg")
aspect_450m_tiles_dir <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/aspect_1_1_deg")

dem_450m_mosaic_path <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/dem_450m.tif")
slope_450m_mosaic_path <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/slope_450m.tif")
aspect_450m_mosaic_path <- file.path(veg_topo_extr_dir, "global_dem_slope_aspect_450m/aspect_450m.tif")

  # Incident shortwave solar radiation and rin
sw_in_450m_tile_dir <- file.path(veg_topo_extr_dir, "global_sw_in_450m/1_1_deg_tiles")
sw_in_uneven_450m_path <- file.path(veg_topo_extr_dir, "global_sw_in_450m/sw_in_uneven_450m.tif")
sw_in_flat_450m_path <- file.path(veg_topo_extr_dir, "global_sw_in_450m/sw_in_flat_450m.tif")
sw_in_terrain_effect_450m_path <- file.path(veg_topo_extr_dir, "global_sw_in_450m/sw_in_terrain_effect_450m.tif")

  # elevation (SD and range)
dem_sd_55km_path <- file.path(veg_topo_extr_dir, "global_dem_55km/dem_sd_55km.nc")
dem_rg_98p_02p_55km_path <- file.path(veg_topo_extr_dir, "global_dem_55km/dem_rg_98p_02p_55km.nc") # range of elevation: 98p - 02p

# 1_04 fraction of land cover
fused_5km_file <- file.path(veg_topo_extr_dir, "global_flc_5km/fused_5km.nc")
fbare_5km_file <- file.path(veg_topo_extr_dir, "global_flc_5km/fbare_5km.nc")
fwater_5km_file <- file.path(veg_topo_extr_dir, "global_flc_5km/fwater_5km.nc") # fraction of water body
fsnow_5km_file <- file.path(veg_topo_extr_dir, "global_flc_5km/fsnow_5km.nc") # fraction of permanent snow and ice

fused_55km_file <- file.path(veg_topo_extr_dir, "global_flc_55km/fused_55km.nc")
fbare_55km_file <- file.path(veg_topo_extr_dir, "global_flc_55km/fbare_55km.nc") # fraction of bare area
fwater_55km_file <- file.path(veg_topo_extr_dir, "global_flc_55km/fwater_55km.nc") # fraction of water body
fsnow_55km_file <- file.path(veg_topo_extr_dir, "global_flc_55km/fsnow_55km.nc") # fraction of permanent snow and ice

# 1_05 Moisture index
mi_5km_file <- file.path(veg_topo_extr_dir, "global_mi_5km/mi_5km.nc")
mi_55km_file <- file.path(veg_topo_extr_dir, "global_mi_55km/mi_55km.nc")

# 1_06 Mean Annual temperature
mat_5km_file <- file.path(veg_topo_extr_dir, "global_mat_5km/global_mat_5km_1970_2000.nc")
mat_55km_file <- file.path(veg_topo_extr_dir, "global_mat_55km/global_mat_55km_1970_2000.nc")

# 1_07 BIOME
ecoregion_path <- file.path(veg_topo_extr_dir, "data_raw/ecoregion2017/Ecoregions2017/Ecoregions2017.shp")
ecoregion_5km_path <- file.path(veg_topo_extr_dir, "data/global_ecoregion_5km/Ecoregions2017_5km.nc")

# 1_08 Protected area
pa_shp0 <- file.path(veg_topo_extr_dir, "wdpa_2025/WDPA_WDOECM_Jul2025_Public_all_shp/WDPA_WDOECM_Jul2025_Public_all_shp_0/WDPA_WDOECM_Jul2025_Public_all_shp-polygons.shp")
pa_shp1 <- file.path(veg_topo_extr_dir, "wdpa_2025/WDPA_WDOECM_Jul2025_Public_all_shp/WDPA_WDOECM_Jul2025_Public_all_shp_1/WDPA_WDOECM_Jul2025_Public_all_shp-polygons.shp")
pa_shp2 <- file.path(veg_topo_extr_dir, "wdpa_2025/WDPA_WDOECM_Jul2025_Public_all_shp/WDPA_WDOECM_Jul2025_Public_all_shp_2/WDPA_WDOECM_Jul2025_Public_all_shp-polygons.shp")
fpa_55km_path <- file.path(veg_topo_extr_dir, "global_fpa_55km_2025/WDPA_WDOECM_Jul2025_55km.nc")

# 2_01 Split  tiles for global correlation
valid_tiles_info_path <- file.path(project_root,"data/valid_tiles_info.rds")
valid_tiles_vect_path <- file.path(project_root, "data/valid_tiles.gpkg")

# 2_02 Global correlation (H~TWI)
cor_twi_vegh_tiles_dir <- file.path(veg_topo_extr_dir, "global_cor_twi_vegh/30_30_deg")
cor_twi_vegh_mosaic_file <- file.path(dirname(cor_twi_vegh_tiles_dir),"cor_twi_vegh_5km_mosaic.nc")
pval_cor_twi_vegh_mosaic_file <- file.path(dirname(cor_twi_vegh_tiles_dir),"pval_cor_twi_vegh_5km_mosaic.nc")

# 3_01 Global correlation (H~Rin)
r_H_R_tiles_dir <-  file.path(veg_topo_extr_dir, "global_r_H_R_5km/30_30_deg")
r_H_R_5km_path <- file.path(veg_topo_extr_dir, "global_r_H_R_5km/r_H_R_5km.nc")
pval_r_H_R_5km_path <- file.path(veg_topo_extr_dir, "global_r_H_R_5km/pval_r_H_R_5km.nc")

# 4_03 Kmeans clustering
kmeans_map_8c_path <- file.path(veg_topo_extr_dir, "global_kmeans_5km/kmeans_corth_fused_mi_5km_8c.nc")


# 5_01 Random sample region extent
reg_sample_dir <- file.path(veg_topo_extr_dir, "reg_sample")

reg_sample_info_path <- file.path(reg_sample_dir, "reg_sample_info.rds")
reg_sample_vect_path <- file.path(reg_sample_dir, "reg_sample_vect.gpkg")

# 5_02 Validation regions result
reg_validate_dir <- file.path(veg_topo_extr_dir, "reg_validate_500m")

# -----------------------Additional Data------------------------------------------
# CCI land cover
cci_landcover_path <- file.path(veg_topo_extr_dir, "landcover_defourny_2023/data/C3S-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1.nc")

# Köppen–Geiger Climate Classification
kg_present_0p0083_file <- file.path(veg_topo_extr_dir, "koeppengeiger_beck_2018/data/Beck_KG_V1_present_0p0083.tif")
kg_present_0p083_file <- file.path(veg_topo_extr_dir, "koeppengeiger_beck_2018/data/Beck_KG_V1_present_0p083.tif")
kg_legend_file <- file.path(veg_topo_extr_dir, "koeppengeiger_beck_2018/data/legend.txt")

