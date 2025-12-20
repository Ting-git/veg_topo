#! /usr/bin/bash -l
#SBATCH --job-name="global_sw_in_terrain_effect_450m"
#SBATCH --time=2:00:00 # 10 min
#SBATCH --account=invest
#SBATCH --qos=job_icpu-stocker
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=icpu-stocker
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G  # 256G
#SBATCH --mail-user=ting.tan@students.unibe.ch
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --chdir=/storage/homefs/tt22k003/veg_topo/analysis

# ---------------------------
# Load required modules
# ---------------------------
module purge
module load CDO/2.4.4-gompi-2024a
module load GDAL/3.10.0-foss-2024a
module load PROJ/9.4.1-GCCcore-13.3.0

# ---------------------------
# Job info
# ---------------------------
echo "=================================================="
echo "Job started on: $(date --rfc-3339=seconds)"
echo "Job ID: $SLURM_JOB_ID"
echo "Job name: $SLURM_JOB_NAME"
echo "Hostname: $(hostname)"
echo "Working directory: $PWD"
echo "=================================================="

# ---------------------------
# Input files
# ---------------------------
sw_in_450m="/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_uneven_450m.nc"
sw_in_flat_450m="/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_flat_450m.nc"
twi_mask="/storage/scratch/giub_geco/tting/global_twi_450m_clean/ga2_clean.nc"

# ---------------------------
# Cleaned input files
# ---------------------------
sw_in_450m_clean="/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_450m_clean.nc"
sw_in_flat_450m_clean="/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_flat_450m_clean.nc"
twi_mask_clean="/storage/scratch/giub_geco/tting/global_sw_in_450m/twi_mask_clean.nc"

echo "👉 Cleaning input files with nccopy..."
nccopy -k 4 -d 0 -s "${sw_in_450m}" "${sw_in_450m_clean}"
nccopy -k 4 -d 0 -s "${sw_in_flat_450m}" "${sw_in_flat_450m_clean}"
nccopy -k 4 -d 0 -s "${twi_mask}" "${twi_mask_clean}"

# ---------------------------
# Output file
# ---------------------------
sw_in_terrain_effect="/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_terrain_effect_450m.nc"

# ---------------------------
# Calculation (one line)
# ---------------------------
echo "👉 Step 1: Compute terrain effect (sw_in / sw_in_flat) with land mask..."
cdo -P 16 -L chvar,sw_in,sw_in_terrain_effect \
    -ifthen "${twi_mask_clean}" \
    -div "${sw_in_450m_clean}" "${sw_in_flat_450m_clean}" \
    "${sw_in_terrain_effect}"

echo "👉 Cleaning temporary files..."
rm -f "${sw_in_450m_clean}" "${sw_in_flat_450m_clean}" "${twi_mask_clean}"

# ---------------------------
# Finish
# ---------------------------
EXIT_STATUS=$?
echo "=================================================="
echo "Job finished on: $(date --rfc-3339=seconds)"
echo "Exit status: $EXIT_STATUS"
echo "Job name: $SLURM_JOB_NAME"
echo "Output file: ${sw_in_terrain_effect}"
echo "=================================================="

echo "👉 Resource usage (from sacct):"
sacct -j $SLURM_JOB_ID --format=JobID,JobName,MaxRSS,Elapsed

echo "👉 Resource usage (from scontrol):"
scontrol show job $SLURM_JOB_ID | grep -E "NumCPUs|AveRSS|MaxRSS|State|RunTime"

exit $EXIT_STATUS
