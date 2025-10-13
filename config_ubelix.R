# -----------------------Global configuration for project------------------------

# Paths
project_root <- "~/veg_topo"
veg_topo_extr_dir <- file.path("/storage/scratch/giub_geco/tting")

# Use it to create global map
ext_global <- ext(-180, 180, -60, 85)

# -----------------------  Vegetation height ------------------------

# Vegetation height
vegh_10m_tiles_dir <- file.path("/storage/scratch/giub_geco/tting/vegheight_lang_2023/data/3deg_cogs")  # Path for higher resolution data
vegh_450m_tiles_dir <- file.path(veg_topo_extr_dir, "global_vegh_450m/3_3_deg")
vegh_450m_mosaic_path <- file.path(veg_topo_extr_dir, "global_vegh_450m/vegh_450m_2020_mosaic.nc")

# TWI
twi_450m_mosaic_clean_path <- file.path("/storage/scratch/giub_geco/tting/global_twi_450m_clean/ga2_clean.nc")
twi_30m_path <- file.path(veg_topo_extr_dir, "twi_ho_2025/data/twi_edtm_m_30m_v20241230.tif")
dem_30m_copernicus_dir <- file.path(veg_topo_extr_dir, "copernicus_dem_30m/copernicus_dem_30m")

# Radiation
sw_in_450m_tile_dir <- file.path(veg_topo_extr_dir, "global_sw_in_450m/1_1_deg_tiles")
sw_in_uneven_450m_path <- file.path(veg_topo_extr_dir, "global_sw_in_450m/sw_in_uneven_450m.nc")
sw_in_flat_450m_path <- file.path(veg_topo_extr_dir, "global_sw_in_450m/sw_in_flat_450m.nc")
sw_in_terrain_effect_450m_path <- file.path(veg_topo_extr_dir, "global_sw_in_450m/sw_in_terrain_effect_450m.nc")

# Global correlation
r_H_R_tiles_dir <-  file.path(veg_topo_extr_dir, "global_r_H_R_5km/30_30_deg")
r_H_R_5km_path <- file.path(veg_topo_extr_dir, "global_r_H_R_5km/r_H_R_5km.nc")
pval_r_H_R_5km_path <- file.path(veg_topo_extr_dir, "global_r_H_R_5km/pval_r_H_R_5km.nc")

# Regional correlation
regA_cor_twi_vegh_dir <- file.path(veg_topo_extr_dir, "regA_cor_twi_vegh_500m")
regB_r_R_H_dir <- file.path(veg_topo_extr_dir, "regB_r_R_H_500m")
