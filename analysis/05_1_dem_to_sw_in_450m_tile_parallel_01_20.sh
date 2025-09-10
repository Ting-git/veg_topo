#! /usr/bin/bash -l
#SBATCH --job-name="DEM-missing"
#SBATCH --time=40:00:00
#SBATCH --ntasks=1
#SBATCH --partition=icpu-stocker
#SBATCH --cpus-per-task=40
#SBATCH --mail-user=ting.tan@students.unibe.ch
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --chdir=/storage/homefs/tt22k003/veg_topo/analysis

# Set personal R library
export R_LIBS_USER=/storage/homefs/tt22k003/R/x86_64-pc-linux-gnu-library/4.4

# Load modules
module load foss/2024a
module load PROJ/9.4.1-GCCcore-13.3.0
module load GDAL/3.10.0-foss-2024a
module load R/4.4.2-gfbf-2024a

# Job information
echo "Started on: $(date --rfc-3339=seconds)"
echo "Hostname: $(hostname)"
echo "Working directory: $PWD"
echo "R_LIBS_USER: $R_LIBS_USER"

# Force Rscript to use the same library paths as RStudio Server
Rscript -e '.libPaths(c(
  "/storage/homefs/tt22k003/R/x86_64-pc-linux-gnu-library/4.4",
  "/storage/software/epyc2.9/software/R-bundle-CRAN/2024.11-foss-2024a",
  "/storage/software/epyc2.9/software/R/4.4.2-gfbf-2024a/lib64/R/library"
)); source("05_1_dem_to_sw_in_450m_tile.R")'

echo "Finished on: $(date --rfc-3339=seconds)"
