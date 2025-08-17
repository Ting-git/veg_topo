# -----------------------Global configuration for project------------------------

# Paths
project_root <- "~/veg_topo"
veg_topo_extr_dir <- file.path("/data_2/scratch/ting/veg_topo_data")

# -----------------------Data Raw and Data Clean--------------------------------

# Vegtation height
vegh_10m_tiles_dir <- file.path("/data_2/archive/vegheight_lang_2023/data/3deg_cogs")  # Path for higher resolution data
vegh_450m_mosaic_path <- file.path(veg_topo_extr_dir, "data/global_vegh_450m/vegh_450m_2020_mosaic.nc")
fvegh_55km_path <- file.path(veg_topo_extr_dir, "data/global_fvegh_55km/fvegh_55km.nc")

# Topographic Wetness Index
twi_450m_path <- file.path("/data/archive/gti_marthews_2015/data/ga2.nc")  # Target raster file path
twi_450m_mosaic_clean_path <- file.path(veg_topo_extr_dir,"data/global_twi_450m_clean/ga2_clean.nc")  # Target raster file path
twi_30m_path <- file.path("/data_2/archive/twi_ho_2025/data/twi_edtm_m_30m_v20241230.tif")

# Digital Terrain Model
dtm_30m_path <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/dtm_30m_ho_2025/edtm/gedtm_rf_m_30m_s_20060101_20151231_go_epsg.4326.3855_v20250611.tif")

dem_sd_10km_path <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/dem_10km_amatulli_2018/elevation_10KMsd_GMTEDsd.tif")
dem_max_10km_path <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/dem_10km_amatulli_2018/elevation_10KMma_GMTEDma.tif")
dem_min_10km_path <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/dem_10km_amatulli_2018/elevation_10KMmi_GMTEDmi.tif")
dem_sd_55km_path <- file.path("/data_2/scratch/ting/veg_topo_data/data/global_dem_55km/dem_sd_55km_resampled.nc")
dem_rg_55km_path <- file.path("/data_2/scratch/ting/veg_topo_data/data/global_dem_55km/dem_rg_55km_resampled.nc") # range of elevation: max - min

# Protected area
pa_shp0 <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/wdpa_2025/WDPA_WDOECM_Jul2025_Public_all_shp/WDPA_WDOECM_Jul2025_Public_all_shp_0/WDPA_WDOECM_Jul2025_Public_all_shp-polygons.shp")
pa_shp1 <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/wdpa_2025/WDPA_WDOECM_Jul2025_Public_all_shp/WDPA_WDOECM_Jul2025_Public_all_shp_1/WDPA_WDOECM_Jul2025_Public_all_shp-polygons.shp")
pa_shp2 <- file.path("/data_2/scratch/ting/veg_topo_data/data_raw/wdpa_2025/WDPA_WDOECM_Jul2025_Public_all_shp/WDPA_WDOECM_Jul2025_Public_all_shp_2/WDPA_WDOECM_Jul2025_Public_all_shp-polygons.shp")

fpa_55km_path <- file.path("/data_2/scratch/ting/veg_topo_data/data/global_fpa_55km_2025/WDPA_WDOECM_Jul2025_55km.nc")

# Mean Annual temperature

mat_55km_file <- file.path("/data_2/scratch/ting/veg_topo_data/data/global_mat_55km/global_mat_55km_1970_2000.nc")
mat_5km_file <- file.path("/data_2/scratch/ting/veg_topo_data/data/global_mat_5km/global_mat_5km_1970_2000.nc")

# -----------------------Data Pre--------------------------------------------------

twi_vegh_merg_450m_tiles_dir <- file.path(veg_topo_extr_dir, "data_temp/global_merg_twi_vegh_450m_30_30_deg")
valid_tiles_info_path <- file.path(project_root,"data/valid_tiles_info.rds")
valid_geotiles_path <- file.path(project_root, "data/valid_tiles.gpkg")

# aridity index
ai_950m_file <- file.path(veg_topo_extr_dir,"data_raw/aridityindex_zomer_2022/Global-AI_ET0_v3_annual/ai_v3_yr.tif")
ai_5km_file <- file.path(veg_topo_extr_dir, "data/global_aridityindex_zomer_2022_to5km/ai_v3_yr_to5km.nc")
ai_55km_file <- file.path(veg_topo_extr_dir, "data/global_ai_55km/ai_55km.nc")

# -----------------------Results--------------------------------------------------

# correalation between twi and vegh
cor_twi_vegh_tiles_dir <- file.path(veg_topo_extr_dir, "data/global_cor_twi_vegh/30_30_deg")
cor_twi_vegh_mosaic_file <- file.path(dirname(cor_twi_vegh_tiles_dir),"cor_twi_vegh_5km_mosaic.nc")

regA_cor_twi_vegh_dir <- file.path(veg_topo_extr_dir, "data/regA_cor_twi_vegh")

# fraction of used land
# flc_5km_mosacic_file <- file.path(veg_topo_extr_dir, "data/global_flc/flc_5km_mosaic.nc")
fused_5km_file <- file.path(veg_topo_extr_dir, "data/global_flc_5km/fused_5km.nc")
fbare_5km_file <- file.path(veg_topo_extr_dir, "data/global_flc_5km/fbare_5km.nc")
fwi_5km_file <- file.path(veg_topo_extr_dir, "data/global_flc_5km/fwi_5km.nc") # fraction of water and ice

fused_55km_file <- file.path(veg_topo_extr_dir, "data/global_flc_55km/fused_55km.nc")
fwi_55km_file <- file.path(veg_topo_extr_dir, "data/global_flc_55km/fwi_55km.nc") # fraction of water and ice
fbare_11km_file <- file.path(veg_topo_extr_dir, "data/global_flc_11km/fbare_11km.nc")
fwater_11km_file <- file.path(veg_topo_extr_dir, "data/global_flc_11km/fwater_11km.nc")

kmeans_map_8c_path <- file.path(veg_topo_extr_dir, "data/global_kmeans_5km/kmeans_corth_fused_ai_5km_8c.nc")
kmeans_map_7c_path <- file.path(veg_topo_extr_dir, "data/global_kmeans_5km/kmeans_corth_fused_ai_5km_7c.nc")

# -----------------------Additional Data------------------------------------------
ecoregion_path <- file.path(veg_topo_extr_dir, "data_raw/ecoregion2017/Ecoregions2017/Ecoregions2017.shp")
ecoregion_5km_path <- file.path(veg_topo_extr_dir, "data/global_ecoregion_5km/Ecoregions2017_5km.nc")


# cci_landcover_path <- file.path("/data_2/scratch/ting/data_raw/CCI_landcover_2020/C3S-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1.nc")
cci_landcover_path <- file.path("/data/archive/landcover_defourny_2023/data/C3S-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1.nc")
