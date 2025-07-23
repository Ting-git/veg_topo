# ------------ Set Up ----------------------------------------------------------
library(terra)
library(tidyr)
library(dplyr)
library(ggplot2)
library(patchwork)
library(DataExplorer)
library(RColorBrewer)


# Load configuration and functions
source(here::here("config.R"))

# ------------ Data Pre for whole-----------------------------------------------

# Load resampled raster datasets (AI, TWI, fused)
ai_5km_r <- terra::rast(ai_5km_file) * 0.0001 # Multiply all values by 0.0001 to get original value
fused_5km_r <- terra::rast(fused_5km_file)
cor_twi_vegh_5km_r <- terra::rast(cor_twi_vegh_mosaic_file)[[1]]
kmeans_8c_r <- terra::rast(kmeans_map_8c_path)
kmeans_12c_r <- terra::rast(kmeans_map_12c_path)

# Crop FLC raster to match extent of AI raster
fused_5km_r <- terra::crop(fused_5km_r, ai_5km_r)

# Stack rasters into a single SpatRaster
stacked <- c(cor_twi_vegh_5km_r,
             fused_5km_r,
             ai_5km_r,
             kmeans_8c_r,
             kmeans_12c_r)

# Convert to data frame for k-means clustering
df <- as.data.frame(stacked, xy = TRUE, na.rm = TRUE)
colnames(df) <- c("lon", "lat", "cor", "fused", "ai", "cluster8c", "cluster12c")

rm(ai_5km_r, fused_5km_r, cor_twi_vegh_5km_r, kmeans_8c_r, kmeans_12c_r)
gc()

# ----------- Overviw: plot the Density for all variables  ---------------------

# plot density
p_ds <- DataExplorer::plot_density(df)

# Combine the plot
cp_ds <- patchwork::wrap_plots(p_ds)

# Save plot
ggsave(
  filename = here::here("data/figures/03_kmeans_ds.png"),
  plot = cp_ds,
  width = 10,
  height = 5,
  dpi = 300,
  units = "in"
)

# ----------- Overviw: plot the histogram for all variables  -------------------

# Ploting
p_hg <- DataExplorer::plot_histogram(df)

# combine the plots
cp_hg <- patchwork::wrap_plots(p_hg)

# Save histogram plot
ggsave(
  filename = here::here("data/figures/03_kmeans_hg.png"),
  plot = cp_hg,
  width = 10,
  height = 5,
  dpi = 300,
  units = "in"
)


