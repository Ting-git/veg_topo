# -----------------------Global configuration for project------------------------

# Paths
project_root <- "~/veg_topo"
veg_topo_extr_dir <- file.path("/storage/scratch/giub_geco/tting")

# Use it to create global map
ext_global <- ext(-180, 180, -60, 85)

# -----------------------  Vegetation height ------------------------

# Vegetation height

vegh_10m_tiles_dir <- file.path("/storage/scratch/giub_geco/tting/vegheight_lang_2023/data/3deg_cogs")  # Path for higher resolution data
vegh_450m_tiles_dir <- file.path(veg_topo_extr_dir, "data/global_vegh_450m/3_3_deg")
vegh_450m_mosaic_path <- file.path(veg_topo_extr_dir, "data/global_vegh_450m/vegh_450m_2020_mosaic.nc")

# TWI
twi_450m_mosaic_clean_path <- file.path("/storage/scratch/giub_geco/tting/global_twi_450m_clean/ga2_clean.nc")
