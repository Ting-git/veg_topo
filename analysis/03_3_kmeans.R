
# ------------ Set Up ----------------------------------------------------------
library(terra)
library(tidyr)
library(dplyr)


# Load configuration and functions
source(here::here("config.R"))

# ------------ Data Pre --------------------------------------------------------

# Load resampled raster datasets (AI, TWI, fused)
ai_5km_r <- terra::rast(ai_5km_file) * 0.0001 # Multiply all values by 0.0001 to get original value
flc_5km_r <- terra::rast(flc_5km_mosacic_file)[[1]]
cor_twi_vegh_5km_r <- terra::rast(cor_twi_vegh_mosaic_file)[[1]]

# Crop FLC raster to match extent of AI raster
flc_5km_r_crop <- terra::crop(flc_5km_r, ai_5km_r)

# Stack rasters into a single SpatRaster
stacked <- c(cor_twi_vegh_5km_r,
             flc_5km_r_crop,
             ai_5km_r)

# Convert to data frame for k-means clustering
df <- as.data.frame(stacked, xy = TRUE, na.rm = TRUE)
colnames(df) <- c("lon", "lat", "cor", "fused", "ai")

set.seed(123)
df_k <- as.data.frame(scale(df[, 3:5])) #

# ------------------------------------------------------------------------------
# ------------ K-means clustering (K=8)-----------------------------------------
# ------------------------------------------------------------------------------

# -------- kmeans clustering (k=8) ---------------------------------------------
km8c <- kmeans(df_k, centers = 8, nstart = 30, algorithm = "Lloyd")
# Add clustering results to data frame
df$cluster8c <- km8c$cluster
df_k$cluster8c <- km8c$cluster

# ------- Save cluster map (k=8)------------------------------------------------
# Convert clustering results back to raster
cluster8c_r <- terra::rast(df[, c("lon", "lat", "cluster8c")],
                         type = "xyz",
                         crs = terra::crs(ai_5km_r))


# Define output path and write to NetCDF
terra::writeCDF(cluster8c_r,
                filename = kmeans_map_8c_path,
                overwrite = TRUE)

message(paste0("Cluster map saved to: ", kmeans_map_8c_path))


# ------------------------------------------------------------------------------
# ------------ K-means clustering (K=12)----------------------------------------
# ------------------------------------------------------------------------------

# -------- kmeans clustering (k=12)
km12c <- kmeans(df_k, centers = 12, nstart = 30, algorithm = "Lloyd")

# Add clustering results to data frame
df$cluster12c <- km12c$cluster
df_k$cluster12c <- km12c$cluster

# ------- Save cluster12c map --------------------------------------------------
# Convert clustering results back to raster
cluster12c_r <- terra::rast(df[, c("lon", "lat", "cluster12c")],
                         type = "xyz",
                         crs = terra::crs(ai_5km_r))

# Define output path and write to NetCDF
terra::writeCDF(cluster12c_r,
                filename = kmeans_map_12c_path,
                overwrite = TRUE)

message(paste0("Cluster map saved to: ", kmeans_map_12c_path))


rm(ai_5km_r, flc_5km_r, cor_twi_vegh_5km_r, stacked, df, df_k, km8c, km12c)
gc

