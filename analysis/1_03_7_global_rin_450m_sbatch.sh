#!/usr/bin/bash -l
#SBATCH --job-name="global_sw_in_terrain_effect_450m"
#SBATCH --time=1:00:00 # ~4 min
#SBATCH --account=invest
#SBATCH --qos=job_icpu-stocker
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=icpu-stocker
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --mail-user=ting.tan@students.unibe.ch
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --chdir=/storage/homefs/tt22k003/veg_topo/analysis

# ---------------------------
# Load required modules
# ---------------------------
module purge
module load GDAL/3.10.0-foss-2024a

# ---------------------------
# Job info
# ---------------------------
echo "=================================================="
echo "Job started on: $(date --rfc-3339=seconds)"
echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $(hostname)"
echo "CPU cores: $SLURM_CPUS_PER_TASK"
echo "Memory: $SLURM_MEM_PER_NODE"
echo "=================================================="

# ---------------------------
# Input files
# ---------------------------
sw_in_450m="/storage/scratch/giub_geco/tting/data/global_sw_in_450m/sw_in_uneven_450m.tif"
sw_in_flat_450m="/storage/scratch/giub_geco/tting/data/global_sw_in_450m/sw_in_flat_450m.tif"

# ---------------------------
# Output file
# ---------------------------
output_dir="/storage/scratch/giub_geco/tting/data/global_sw_in_450m"
sw_in_terrain_effect="${output_dir}/sw_in_terrain_effect_450m.tif"

# ---------------------------
# Step 1: Calculate terrain effect (sw_in / sw_in_flat)
# ---------------------------
echo "👉 Step 1: Calculating terrain effect ratio (sw_in / sw_in_flat)..."
echo "   Using $SLURM_CPUS_PER_TASK CPU cores for parallel processing"

gdal_calc.py \
    --calc="where(B > 0, A/B, -9999)" \
    -A "${sw_in_450m}" \
    -B "${sw_in_flat_450m}" \
    --outfile="${sw_in_terrain_effect}" \
    --type=Float32 \
    --NoDataValue=-9999 \
    --co="COMPRESS=LZW" \
    --co="PREDICTOR=2" \
    --co="BIGTIFF=YES" \
    --co="TILED=YES" \
    --co="BLOCKXSIZE=256" \
    --co="BLOCKYSIZE=256" \
    --co="NUM_THREADS=$SLURM_CPUS_PER_TASK" \
    --quiet

# ---------------------------
# Step 2: Build statistics and overviews
# ---------------------------
echo "👉 Step 2: Building statistics and overview pyramids..."
gdalinfo -stats "${sw_in_terrain_effect}" > /dev/null 2>&1
gdaladdo -r average "${sw_in_terrain_effect}" 2 4 8 16

# ---------------------------
# Verification
# ---------------------------
echo "👉 Verification:"
if [ -f "${sw_in_terrain_effect}" ]; then
    echo "   ✅ Output file created successfully"
    echo "   📏 File size: $(ls -lh "${sw_in_terrain_effect}" | awk '{print $5}')"

    # Get basic metadata
    size_info=$(gdalinfo "${sw_in_terrain_effect}" | grep "Size is" | head -1)
    nodata_info=$(gdalinfo "${sw_in_terrain_effect}" | grep "NoData Value" | head -1)

    echo "   📐 Dimensions: ${size_info:-Not available}"
    echo "   🚫 NoData value: ${nodata_info:-Not available}"

    # Calculate basic statistics
    echo "   📊 Calculating basic statistics..."
    stats_output=$(gdalinfo -stats "${sw_in_terrain_effect}" 2>/dev/null | grep -A1 "STATISTICS_MEAN\|STATISTICS_MINIMUM\|STATISTICS_MAXIMUM")
    if [ -n "$stats_output" ]; then
        echo "$stats_output" | while read line; do
            echo "   $line"
        done
    fi

    EXIT_STATUS=0
else
    echo "   ❌ ERROR: Output file not created!"
    EXIT_STATUS=1
fi

# ---------------------------
# Finish
# ---------------------------
echo "=================================================="
echo "Job completed on: $(date --rfc-3339=seconds)"
echo "Total runtime: $(($SECONDS / 3600))h $((($SECONDS % 3600) / 60))m $(($SECONDS % 60))s"
echo "Exit status: $EXIT_STATUS"
echo "Output file: $(basename "${sw_in_terrain_effect}")"
echo "=================================================="

# Optional: Show resource usage
echo "👉 Resource usage summary:"
sacct -j $SLURM_JOB_ID --format=JobID,JobName,MaxRSS,Elapsed,State -P 2>/dev/null || echo "   Resource information not available"

exit $EXIT_STATUS
