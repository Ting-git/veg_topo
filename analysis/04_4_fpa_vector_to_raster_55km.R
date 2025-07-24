# ---- Setup -------------------------------------------------------------------

# load library
library(terra)
# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/aggregate_byfile.R"))

# ----convert vect to raster with bash -----------------------------------------

# Define input shapefile paths
# shp_files <- c(
#   "/data_2/scratch/ting/veg_topo_data/data_raw/wdpa_2025/WDPA_WDOECM_Jul2025_Public_all_shp/WDPA_WDOECM_Jul2025_Public_all_shp_0/WDPA_WDOECM_Jul2025_Public_all_shp-polygons.shp",
#   "/data_2/scratch/ting/veg_topo_data/data_raw/wdpa_2025/WDPA_WDOECM_Jul2025_Public_all_shp/WDPA_WDOECM_Jul2025_Public_all_shp_1/WDPA_WDOECM_Jul2025_Public_all_shp-polygons.shp",
#   "/data_2/scratch/ting/veg_topo_data/data_raw/wdpa_2025/WDPA_WDOECM_Jul2025_Public_all_shp/WDPA_WDOECM_Jul2025_Public_all_shp_2/WDPA_WDOECM_Jul2025_Public_all_shp-polygons.shp"
# )

shp_files <- c(
  pa_shp0,
  pa_shp1,
  pa_shp2
)

# Define output raster paths
out_dir <- file.path(veg_topo_extr_dir, "/data_temp/pa_shp")
out_files <- file.path(out_dir, paste0("pa_", 0:2, ".tif"))
pa_merged_22km_path <- file.path(out_dir, "pa_merged_22km.tif")

# Rasterize function
rasterize_shp <- function(shp_path, out_tif) {
  cmd <- sprintf(
    "gdal_rasterize -burn 1 -tr 0.02 0.02 -te -180 -60 180 90 -ot Byte -of GTiff '%s' '%s'",
    shp_path, out_tif
  )
  cat("Running:", cmd, "\n")
  system(cmd)
}

# Rasterize all shapefiles
mapply(rasterize_shp, shp_files, out_files)

# Merge rasters with gdal_calc.py
merge_cmd <- sprintf(
  paste(
    "gdal_calc.py",
    "-A '%s'",
    "-B '%s'",
    "-C '%s'",
    "--outfile='%s'",
    '--calc=\"maximum(A,B,C)\"',
    "--NoDataValue=0",
    "--overwrite"
  ),
  out_files[1], out_files[2], out_files[3],
  pa_merged_22km_path
)

cat("Running:", merge_cmd, "\n")
system(merge_cmd)

cat("All done! Merged raster saved at:\n", pa_merged_22km_path, "\n")


# ---- Aggregate to 55km -------------------------------------------------------------

# Aggregation
aggregate_byfile(
  input_path = pa_merged_22km_path,
  output_path = fpa_55km_path,
  target_path = ai_55km_file,
  varname = "fpa",
  if_resample = TRUE,
  fun = function(x, na.rm) {
    if (all(is.na(x))) {
      return(NA)
    } else {
      return(sum(x == 1, na.rm = na.rm) / length(x))
    }
  }
)

# # check the data
# r_out <- rast(fpa_55km_path)
# r_out
#
# plot(r_out)
#
# summary(r_out)

# ------ Cleanup ---------------------------------------------------------------
rm(list = ls())
gc()

