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

# VEGH 10m
rsync --human-readable -i --info=progress2 -avhP \
  --chmod=Du+rwx,Dgo+rx,Fu+rw,Fgo+r \
  --no-owner --no-group \
  --include="*/" --include="*Map.tif" --exclude="*" \
  /data_2/archive/vegheight_lang_2023/ \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/vegheight_lang_2023/


# DEM 30m
rsync --human-readable -i --info=progress2 -avhP --no-owner --no-group \
  /data_2/scratch/ting/veg_topo_data/data_raw/copernicus_dem_30m/ \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/copernicus_dem_30m/

echo "=== ALL TRANSFERS COMPLETED ==="
