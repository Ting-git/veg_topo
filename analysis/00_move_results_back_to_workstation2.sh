#!/bin/bash
set -e  # if error, stop

echo "=== START DATA TRANSFER ==="

# Move all processed data
rsync -avhP --no-perms --no-owner --no-group \
  --human-readable -i --info=progress2 \
  --exclude='*_deg/' \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/data/ \
  /data_2/scratch/ting/veg_topo_data/data/

# TWI 450m clean
rsync -avhP --no-perms --no-owner --no-group \
  --human-readable -i --info=progress2 \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/data/global_twi_450m_clean \
  /data_2/scratch/ting/veg_topo_data/data/

# Veg 450m, exclude the sub-folder of splited tiles
rsync -avhP --no-perms --no-owner --no-group \
  --human-readable -i --info=progress2 \
  --exclude='*/' \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/data/global_vegh_fveg_450m/ \
  /data_2/scratch/ting/veg_topo_data/data/global_vegh_fveg_450m/

# sw_in_450m, exclude the sub-folder of splited tiles
rsync -avhP --no-perms --no-owner --no-group \
  --human-readable -i --info=progress2 \
  --exclude='*/' \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/data/global_sw_in_450m/ \
  /data_2/scratch/ting/veg_topo_data/data/global_sw_in_450m/

# global_dem_slope_aspect_450m, exclude the sub-folder of splited tiles
rsync -avhP --no-perms --no-owner --no-group \
  --exclude='*/' --human-readable -i --info=progress2 \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/data/global_dem_slope_aspect_450m/ \
  /data_2/scratch/ting/veg_topo_data/data/global_dem_slope_aspect_450m/

# global_r_H_R_5km
rsync -avhP --no-perms --no-owner --no-group \
  --exclude='*/' --human-readable -i --info=progress2 \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/data/global_r_H_R_5km/ \
  /data_2/scratch/ting/veg_topo_data/data/global_r_H_R_5km/

# global_cor_twi_vegh
rsync -avhP --no-perms --no-owner --no-group \
  --exclude='*/' --human-readable -i --info=progress2 \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/data/global_cor_twi_vegh/ \
  /data_2/scratch/ting/veg_topo_data/data/global_cor_twi_vegh/

# reg_sample
rsync -avhP --no-perms --no-owner --no-group \
  --human-readable -i --info=progress2 \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/data/reg_sample/ \
  /data_2/scratch/ting/veg_topo_data/data/reg_sample/

# reg_validate_500m
rsync -avhP --no-perms --no-owner --no-group \
  --human-readable -i --info=progress2 \
  tt22k003@submit04.unibe.ch:/storage/scratch/giub_geco/tting/data/reg_validate_500m/ \
  /data_2/scratch/ting/veg_topo_data/data/reg_validate_500m/

# data/figures
rsync -avhP --no-perms --no-owner --no-group \
  --human-readable -i --info=progress2 \
  tt22k003@submit04.unibe.ch:/storage/homefs/tt22k003/veg_topo/data/figures/ \
   /data_2/scratch/ting/veg_topo_data/data/figures/

echo "=== ALL TRANSFERS COMPLETED ==="
