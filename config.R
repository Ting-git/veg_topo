# -----------------------Global configuration for project------------------------

# Paths
project_root <- "~/veg_topo"
temp_dir <- file.path("/data_2/scratch/ting/data_temp")

# -----------------------Data Raw and clean--------------------------------------------------
vegh_10m_tiles_dir <- file.path("/data_2/archive/vegheight_lang_2023/data/3deg_cogs")  # Path for higher resolution data


vegh_450m_tiles_dir <- file.path("/data_2/scratch/ting/data/vegh_450m/3_3_deg")
vegh_450m_mosaic_path <- file.path(dirname(vegh_450m_tiles_dir), "vegh_450m_2020_mosaic.nc")

twi_450m_path <- file.path("/data/archive/gti_marthews_2015/data/ga2.nc")  # Target raster file path
twi_450m_mosaic_clean_path <- file.path("/data/archive/gti_marthews_2015/data/ga2.nc")  # Target raster file path

# -----------------------Data Pre--------------------------------------------------

twi_vegh_merg_450m_tiles_dir <- file.path(temp_dir, "twi_vegh_450m_merg_30_30_deg")
tiles_info_path <- here::here("data/predata_info.rds")

# -----------------------Results--------------------------------------------------
cor_twi_vegh_tiles_dir <- file.path("/data_2/scratch/ting/data/cor_twi_vegh/30_30_deg")
cor_twi_vegh_mosaic_file <- file.path("/data_2/scratch/ting/data/cor_twi_vegh/cor_twi_vegh_5km_mosaic.nc")


# -----------------------Additional Data------------------------------------------
ecoregions_path <- file.path("/data_2/scratch/ting/data_raw/ecoregion2017/Ecoregions2017/Ecoregions2017.shp")
cci_landcover_path <- file.path("/data_2/scratch/ting/data_raw/CCI_landcover_2020/C3S-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1.nc")
