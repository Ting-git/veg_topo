# -----------------------Global configuration for project------------------------

# Paths
project_root <- "~/veg_topo"
veg_topo_extr_dir <- file.path("/data_2/scratch/ting/veg_topo_data")

# -----------------------Data Raw and clean--------------------------------------------------
vegh_10m_tiles_dir <- file.path("/data_2/archive/vegheight_lang_2023/data/3deg_cogs")  # Path for higher resolution data


vegh_450m_tiles_dir <- file.path(veg_topo_extr_dir, "data/global_vegh_450m/3_3_deg")
vegh_450m_mosaic_path <- file.path(dirname(vegh_450m_tiles_dir), "vegh_450m_2020_mosaic.nc")

twi_450m_path <- file.path("/data/archive/gti_marthews_2015/data/ga2.nc")  # Target raster file path
twi_450m_mosaic_clean_path <- file.path(veg_topo_extr_dir,"data/global_twi_450m_clean/ga2_clean.nc")  # Target raster file path

twi_30m_path <- file.path("/data_2/archive/twi_ho_2025/data/twi_edtm_m_30m_v20241230.tif")

# -----------------------Data Pre--------------------------------------------------

twi_vegh_merg_450m_tiles_dir <- file.path(veg_topo_extr_dir, "data_temp/global_merg_twi_vegh_450m_30_30_deg")
valid_tiles_info_path <- file.path(project_root,"data/valid_tiles_info.rds")
valid_geotiles_path <- file.path(project_root, "data/valid_tiles.gpkg")

# aridity index
ai_950m_file <- file.path(veg_topo_extr_dir,"data_raw/aridityindex_zomer_2022/Global-AI_ET0_v3_annual/ai_v3_yr.tif")
ai_5km_file <- file.path(veg_topo_extr_dir, "data/global_aridityindex_zomer_2022_to5km/ai_v3_yr_to5km.nc")
# -----------------------Results--------------------------------------------------

# correalation between twi and vegh
cor_twi_vegh_tiles_dir <- file.path(veg_topo_extr_dir, "data/global_cor_twi_vegh/30_30_deg")
cor_twi_vegh_mosaic_file <- file.path(dirname(cor_twi_vegh_tiles_dir),"cor_twi_vegh_5km_mosaic.nc")

# fraction of used land
flc_5km_tiles_dir <- file.path(veg_topo_extr_dir,"data_temp/flc_5km/30_30_deg")
flc_5km_mosacic_file <- file.path(veg_topo_extr_dir, "data/global_flc/flc_5km_mosaic.nc")

kmeans_corth_fused_ai_path <- file.path(veg_topo_extr_dir, "data/global_kmeans_1/kmeans_corth_fused_ai_5km.nc")

# -----------------------Additional Data------------------------------------------
ecoregions_path <- file.path("/data_2/scratch/ting/data_raw/ecoregion2017/Ecoregions2017/Ecoregions2017.shp")

# cci_landcover_path <- file.path("/data_2/scratch/ting/data_raw/CCI_landcover_2020/C3S-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1.nc")
cci_landcover_path <- file.path("/data/archive/landcover_defourny_2023/data/C3S-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1.nc")
