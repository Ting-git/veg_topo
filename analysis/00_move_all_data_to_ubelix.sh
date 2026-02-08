#!/bin/bash
set -e  # if error, stop

echo "=== START DATA TRANSFER ==="

# TWI 450m
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_twi_450m_clean \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# flc 5km
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_flc_5km \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# r_H_TWI 5km
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_cor_twi_vegh \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# r_H_Rin 5km
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_r_H_R_5km \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# k-means 5km
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_kmeans_5km \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# mi 5km
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_mi_5km \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# mat 5km
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_mat_5km \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# VEGH 450m
rsync --human-readable -i --info=progress2 -avhP --no-perms --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_vegh_450m/vegh_450m_2020_mosaic.nc \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/global_vegh_450m/

# reg_sample
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  --chmod=Du+rwx,Dgo+rx,Fu+rw,Fgo+r \
  /data_2/scratch/ting/veg_topo_data/data/reg_sample \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# global_dem_slope_aspect_450m
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  --chmod=Du+rwx,Dgo+rx,Fu+rw,Fgo+r \
  /data_2/scratch/ting/veg_topo_data/data/global_dem_slope_aspect_450m \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# ecoregion_5km
rsync --human-readable -i --info=progress2 -avhP --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_ecoregion_5km \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# TWI 30m
rsync -avhP --info=progress2 --no-owner --no-group \
  --chmod=Du+rwx,Dgo+rx,Fu+rw,Fgo+r \
  /data_2/archive/twi_ho_2025 \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# Koeppen-Geiger
rsync --human-readable -i --info=progress2 -avhP \
  --chmod=Du+rwx,Dgo+rx,Fu+rw,Fgo+r \
  --no-owner --no-group \
  /data/archive/koeppengeiger_beck_2018/ \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/koeppengeiger_beck_2018/

# cci 300m
rsync --human-readable -i --info=progress2 -avhP \
  --chmod=Du+rwx,Dgo+rx,Fu+rw,Fgo+r \
  --no-owner --no-group \
  /data/archive/landcover_defourny_2023 \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# VEGH 10m
rsync --human-readable -i --info=progress2 -avhP \
  --chmod=Du+rwx,Dgo+rx,Fu+rw,Fgo+r \
  --no-owner --no-group \
  /data_2/archive/vegheight_lang_2023 \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# DEM 30m
rsync --human-readable -i --info=progress2 -avhP --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data_raw/copernicus_dem_30m/ \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/copernicus_dem_30m/

# WDPA: world database on Protected area
rsync --human-readable -i --info=progress2 -avhP --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data_raw/wdpa_2025 \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/



# dem 55km: dem_sd_55km, dem_rg_55km
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_dem_55km \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# flc 55km
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_flc_55km \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# mi 55km
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_mi_55km \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# fpa 55km
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_fpa_55km_2025 \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# fveg 55km
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_fveg_55km \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

# mat 55km
rsync -avhP --info=progress2 --no-perms --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data/global_mat_55km \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/

echo "=== ALL TRANSFERS COMPLETED ==="
