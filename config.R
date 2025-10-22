# -----------------------Global configuration for project------------------------

# Paths
project_root <- "~/veg_topo"
veg_topo_extr_dir <- file.path("/data_2/scratch/ting/veg_topo_data")

# Use it to create global map
ext_global <- ext(-180, 180, -60, 85)

# -----------------------Data Raw and Data Clean--------------------------------

# Vegetation height
vegh_10m_tiles_dir <- file.path("/data_2/archive/vegheight_lang_2023/data/3deg_cogs")  # Path for higher resolution data

vegh_450m_tiles_dir <- file.path(veg_topo_extr_dir,"data/global_vegh_450m/3_3_deg")
vegh_450m_mosaic_path <- file.path(veg_topo_extr_dir, "data/global_vegh_450m/vegh_450m_2020_mosaic.nc")

fveg_55km_path <- file.path(veg_topo_extr_dir, "data/global_fveg_55km/fveg_55km.nc")

# Topographic Wetness Index
twi_450m_path <- file.path("/data/archive/gti_marthews_2015/data/ga2.nc")  # Target raster file path
twi_450m_mosaic_clean_path <- file.path(veg_topo_extr_dir,"data/global_twi_450m_clean/ga2_clean.nc")  # Target raster file path
twi_30m_path <- file.path("/data_2/archive/twi_ho_2025/data/twi_edtm_m_30m_v20241230.tif")

# Digital Terrain Model
dem_30m_copernicus_dir <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/copernicus_dem_30m/copernicus_dem_30m")

dem_450m_tiles_dir <- file.path(veg_topo_extr_dir, "data/global_dem_slope_aspect_450m/dem_1_1_deg")
slope_450m_tiles_dir <- file.path(veg_topo_extr_dir, "data/global_dem_slope_aspect_450m/slope_1_1_deg")
aspect_450m_tiles_dir <- file.path(veg_topo_extr_dir, "data/global_dem_slope_aspect_450m/aspect_1_1_deg")

dem_450m_mosaic_path <- file.path(veg_topo_extr_dir, "data/global_dem_slope_aspect_450m/dem_450m1.nc")
slope_450m_mosaic_path <- file.path(veg_topo_extr_dir, "data/global_dem_slope_aspect_450m/slope_450m1.nc")
aspect_450m_mosaic_path <- file.path(veg_topo_extr_dir, "data/global_dem_slope_aspect_450m/aspect_450m1.nc")

dem_sd_10km_path <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/dem_10km_50km_amatulli_2018/elevation_10KMsd_GMTEDsd.tif")
dem_max_10km_path <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/dem_10km_50km_amatulli_2018/elevation_10KMma_GMTEDma.tif")
dem_min_10km_path <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/dem_10km_50km_amatulli_2018/elevation_10KMmi_GMTEDmi.tif")
dem_sd_50km_path <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/dem_10km_50km_amatulli_2018/elevation_50KMsd_GMTEDsd.tif")

dem_sd_55km_path <- file.path("/data_2/scratch/ting/veg_topo_data/data/global_dem_55km/dem_sd_55km.nc")
dem_rg_max_min_55km_path <- file.path("/data_2/scratch/ting/veg_topo_data/data/global_dem_55km/dem_rg_max_min_55km.nc") # range of elevation: max - min
dem_rg_95p_05p_55km_path <- file.path("/data_2/scratch/ting/veg_topo_data/data/global_dem_55km/dem_rg_95p_05p_55km.nc") # range of elevation: 95p - 05p

# Protected area
pa_shp0 <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/wdpa_2025/WDPA_WDOECM_Jul2025_Public_all_shp/WDPA_WDOECM_Jul2025_Public_all_shp_0/WDPA_WDOECM_Jul2025_Public_all_shp-polygons.shp")
pa_shp1 <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/wdpa_2025/WDPA_WDOECM_Jul2025_Public_all_shp/WDPA_WDOECM_Jul2025_Public_all_shp_1/WDPA_WDOECM_Jul2025_Public_all_shp-polygons.shp")
pa_shp2 <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/wdpa_2025/WDPA_WDOECM_Jul2025_Public_all_shp/WDPA_WDOECM_Jul2025_Public_all_shp_2/WDPA_WDOECM_Jul2025_Public_all_shp-polygons.shp")

fpa_55km_path <- file.path("/data_2/scratch/ting/veg_topo_data/data/global_fpa_55km_2025/WDPA_WDOECM_Jul2025_55km.nc")

# Mean Annual temperature

mat_55km_file <- file.path("/data_2/scratch/ting/veg_topo_data/data/global_mat_55km/global_mat_55km_1970_2000.nc")
mat_5km_file <- file.path("/data_2/scratch/ting/veg_topo_data/data/global_mat_5km/global_mat_5km_1970_2000.nc")

twi_vegh_merg_450m_tiles_dir <- file.path(veg_topo_extr_dir, "data_temp/global_merg_twi_vegh_450m_30_30_deg")
valid_tiles_info_path <- file.path(project_root,"data/valid_tiles_info.rds")
valid_tiles_vect_path <- file.path(project_root, "data/valid_tiles.gpkg")

# aridity index
mi_950m_file <- file.path(veg_topo_extr_dir,"data_raw/aridityindex_zomer_2022/Global-AI_ET0_v3_annual/ai_v3_yr.tif")
mi_5km_file <- file.path(veg_topo_extr_dir, "data/global_mi_5km/mi_5km.nc")
mi_55km_file <- file.path(veg_topo_extr_dir, "data/global_mi_55km/mi_55km.nc")

# correalation between twi and vegh
cor_twi_vegh_tiles_dir <- file.path(veg_topo_extr_dir, "data/global_cor_twi_vegh/30_30_deg")
cor_twi_vegh_mosaic_file <- file.path(dirname(cor_twi_vegh_tiles_dir),"cor_twi_vegh_5km_mosaic.nc")
pval_cor_twi_vegh_mosaic_file <- file.path(dirname(cor_twi_vegh_tiles_dir),"pval_cor_twi_vegh_5km_mosaic.nc")

cor_twi_vegh_mask_fused0.05_file <- file.path(dirname(cor_twi_vegh_tiles_dir),"cor_twi_vegh_5km_mask_fused0.05.nc")


# fraction of land cover
# flc_5km_mosacic_file <- file.path(veg_topo_extr_dir, "data/global_flc/flc_5km_mosaic.nc")
fused_5km_file <- file.path(veg_topo_extr_dir, "data/global_flc_5km/fused_5km.nc")
fbare_5km_file <- file.path(veg_topo_extr_dir, "data/global_flc_5km/fbare_5km.nc") # fraction of bare area
fwater_5km_file <- file.path(veg_topo_extr_dir, "data/global_flc_5km/fwater_5km.nc") # fraction of water body
fsnow_5km_file <- file.path(veg_topo_extr_dir, "data/global_flc_5km/fsnow_5km.nc") # fraction of permanent snow and ice

fused_55km_file <- file.path(veg_topo_extr_dir, "data/global_flc_55km/fused_55km.nc")
fbare_55km_file <- file.path(veg_topo_extr_dir, "data/global_flc_55km/fbare_55km.nc") # fraction of bare area
fwater_55km_file <- file.path(veg_topo_extr_dir, "data/global_flc_55km/fwater_55km.nc") # fraction of water body
fsnow_55km_file <- file.path(veg_topo_extr_dir, "data/global_flc_55km/fsnow_55km.nc") # fraction of permanent snow and ice

kmeans_map_8c_path <- file.path(veg_topo_extr_dir, "data/global_kmeans_5km/kmeans_corth_fused_mi_5km_8c.nc")
# kmeans_map_7c_path <- file.path(veg_topo_extr_dir, "data/global_kmeans_5km/kmeans_corth_fused_mi_5km_7c.nc")

sw_in_450m_tile_dir <- file.path(veg_topo_extr_dir, "data/global_sw_in_450m/1_1_deg_tiles")
sw_in_uneven_450m_path <- file.path(veg_topo_extr_dir, "data/global_sw_in_450m/sw_in_uneven_450m.nc")
sw_in_flat_450m_path <- file.path(veg_topo_extr_dir, "data/global_sw_in_450m/sw_in_flat_450m.nc")
sw_in_terrain_effect_450m_path <- file.path(veg_topo_extr_dir, "data/global_sw_in_450m/sw_in_terrain_effect_450m.nc")

r_H_R_tiles_dir <-  file.path(veg_topo_extr_dir, "data/global_r_H_R_5km/30_30_deg")
r_H_R_5km_path <- file.path(veg_topo_extr_dir, "data/global_r_H_R_5km/r_H_R_5km.nc")
pval_r_H_R_5km_path <- file.path(veg_topo_extr_dir, "data/global_r_H_R_5km/pval_r_H_R_5km.nc")



reg_sample_dir <- file.path(veg_topo_extr_dir, "data/reg_sample")

regA_sample_info_path <- file.path(reg_sample_dir, "regA_sample_info.rds")
regB_sample_info_path <- file.path(reg_sample_dir, "regB_sample_info.rds")

regA_sample_vect_path <- file.path(reg_sample_dir, "regA_sample_vect.gpkg")
regB_sample_vect_path <- file.path(reg_sample_dir, "regB_sample_vect.gpkg")

regA_cor_twi_vegh_dir <- file.path(veg_topo_extr_dir, "data/regA_cor_twi_vegh_500m")
regB_r_R_H_dir <- file.path(veg_topo_extr_dir, "data/regB_r_R_H_500m")

# -----------------------Additional Data------------------------------------------
ecoregion_path <- file.path(veg_topo_extr_dir, "data_raw/ecoregion2017/Ecoregions2017/Ecoregions2017.shp")
ecoregion_5km_path <- file.path(veg_topo_extr_dir, "data/global_ecoregion_5km/Ecoregions2017_5km.nc")

# cci_landcover_path <- file.path("/data_2/scratch/ting/data_raw/CCI_landcover_2020/C3S-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1.nc")
cci_landcover_path <- file.path("/data/archive/landcover_defourny_2023/data/C3S-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1.nc")
