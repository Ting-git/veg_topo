# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------
library(terra)
library(future)
library(tidyverse)
library(furrr)
library(scico)
library(viridis)
library(fs)
library(sf)
library(progressr)
library(tidyterra)
handlers(handler_txtprogressbar(style = 3))

source(here::here("R/split_window_correlation.R"))

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

file_vegh_450m_mosaic <- file.path("/data_2/scratch/ting/data/vegh_450m/vegh_450m_2020_mosaic.nc")
file_ga2 <- file.path("/data/archive/gti_marthews_2015/data/ga2.nc")  # Target raster file path
cor_mosaic_file <- file.path("/data_2/scratch/ting/data/corr_map/corr_twi_vegh_5km_mosaic.nc")  # Output file
temp_dir <- file.path("/data_2/scratch/ting/data_temp")
cor_tiles_dir <- file.path("/data_2/scratch/ting/data/corr_map/30_30_deg")
ecoregions_path <- file.path("/data_2/scratch/ting/data_raw/ecoregion2017/Ecoregions2017/Ecoregions2017.shp")
cci_landcover_path <- file.path("/data_2/scratch/ting/data_raw/CCI_landcover_2020/C3S-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1.nc")
# ------------------------------------------------------------------------------
# Preprocess --> data preparing
# ------------------------------------------------------------------------------

# ext_list <- generate_global_extents()
# # this process need 30 mins
# prep_tiles_info <- generate_prep_tiles_info(
#   ext_list, file_ga2, file_vegh_450m_mosaic, temp_dir)
#
# str(prep_tiles_info, max.level = 2)

# ------------------------------------------------------------------------------
# Implementation --> single test 2
# ------------------------------------------------------------------------------
source(here::here("R/split_window_correlation.R"))
# Clear memory and set up parallel processing
prep_tiles_info <- readr::read_csv(here::here("data/preprocessed_tiles_info.csv"))

name <- as.character(prep_tiles_info[39, 1])
xmin <- as.numeric(prep_tiles_info[39, 2])
xmax <- as.numeric(prep_tiles_info[39, 3])
ymin <- as.numeric(prep_tiles_info[39, 4])
ymax <- as.numeric(prep_tiles_info[39, 5])
file <- as.character(prep_tiles_info[39, 6])

# Load pre-processed raster
merged_raster <- terra::rast(file)

# Generate spatial windows and correlation data frames
windowed_data <- create_spatial_windows(merged_raster)
correlation_df <- calculate_window_correlations(windowed_data)

# Convert to raster and save output
correlation_raster <- terra::rast(
  correlation_df[, c("lon_mid", "lat_mid", "correlation", "cor_pval")],
  type = "xyz",
  crs = "EPSG:4326"
)
names(correlation_raster) <- c("correlation", "cor_pval")
nc_path <- file.path(cor_tiles_dir, paste0(name, "_corr_twi_vegh_5km.nc"))
terra::writeCDF(correlation_raster, nc_path, overwrite = TRUE)

# Generate plots
plot1 <- plot_twi(windowed_data, xmin, xmax, ymin, ymax)
plot2 <- plot_vegh(windowed_data, xmin, xmax, ymin, ymax)
plot3 <- plot_corr(correlation_df, xmin, xmax, ymin, ymax)
plot4 <- plot_overview(windowed_data)
plot5 <- plot_landcover2(cci_landcover_path, xmin, xmax, ymin, ymax)
plot6 <- plot_biomes_by_extent(ecoregions_path, xmin, xmax, ymin, ymax)


png_path <- save_combined_plot(
  plots = list(plot1, plot2, plot3, plot4, plot5, plot6),
  region_name = name,
  title_text = "VEGH and TWI Correlation Analysis",
  ncol = 2,
  width = 13,
  height = 20,
  file_index = "02_3"
)

# Clear memory for large objects including plots
rm(
  merged_raster,
  windowed_data,
  correlation_df,
  correlation_raster,
  plot1, plot2, plot3, plot4, plot5, plot6
)
gc()
