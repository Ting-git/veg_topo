
# ------------ Set Up ----------------------------------------------------------
library(terra)
library(tidyr)
library(dplyr)
library(car) # Box-Cox Transformation

# Load configuration and functions
source(here::here("config.R"))

# ------------ Data Load -------------------------------------------------------

# Load resampled raster datasets (AI, fused, r(H~TWI))
ai_5km_r <- terra::rast(ai_5km_file) * 0.0001 # Multiply all values by 0.0001 to get original value
ai_5km_r[ai_5km_r == 0] <- NA

fused_5km_r <- terra::rast(fused_5km_file)
fused_5km_r_crop <- terra::crop(fused_5km_r, ai_5km_r) # Crop FLC raster to match extent of AI raster

cor_twi_vegh_5km_r <- terra::rast(cor_twi_vegh_mosaic_file)[[1]]

# Stack rasters into a single SpatRaster
stacked <- c(cor_twi_vegh_5km_r,
             fused_5km_r_crop,
             ai_5km_r)

# Convert to data frame for k-means clustering
df <- as.data.frame(stacked, xy = TRUE, na.rm = TRUE)
colnames(df) <- c("lon", "lat", "cor", "fused", "ai")

# ------------ Data Pre --------------------------------------------------------

# Use Box-Cox transformation to select the optimal lambda (power)
# This helps make the 'ai' variable more normally distributed
pt <- powerTransform(df$ai)
lambda <- pt$lambda
cat("Optimal lambda:", lambda, "\n")

# Apply the Box-Cox transformation using the selected lambda
df$ai_boxcox <- bcPower(df$ai, lambda)

# Plot histograms before and after transformation to compare distributions
# Save as PNG file
png(here::here("data/figures/03_ai_histograms.png"), width = 800, height = 400, res = 150)

# Create histograms side by side
par(mfrow = c(1, 2))
hist(df$ai, main = "Original AI", col = "red")
hist(df$ai_boxcox, main = "Box-Cox Transformed AI", col = "blue")

# Close the graphics device
dev.off()

# Standardize the input variables for k-means clustering
# This centers the data (mean = 0) and scales it (SD = 1)
df_k <- as.data.frame(scale(df[, c("cor", "fused", "ai_boxcox")]))

# -------- kmeans clustering (k=8) ---------------------------------------------

set.seed(123)
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
                names = "cluster8c",
                overwrite = TRUE)

message(paste0("Cluster map saved to: ", kmeans_map_8c_path))


# -------- kmeans clustering (k=7) ---------------------------------------------

km7c <- kmeans(df_k, centers = 7, nstart = 30, algorithm = "Lloyd")

# Add clustering results to data frame
df$cluster7c <- km7c$cluster
df_k$cluster7c <- km7c$cluster

# ------- Save cluster map (k=7) -----------------------------------------------

# Convert clustering results back to raster
cluster7c_r <- terra::rast(df[, c("lon", "lat", "cluster7c")],
                         type = "xyz",
                         crs = terra::crs(ai_5km_r))

# Define output path and write to NetCDF
terra::writeCDF(cluster7c_r,
                filename = kmeans_map_7c_path,
                names = "cluster7c",
                overwrite = TRUE)

message(paste0("Cluster map saved to: ", kmeans_map_7c_path))

# ------ Cleanup ---------------------------------------------------------------

rm(list = ls())
gc

