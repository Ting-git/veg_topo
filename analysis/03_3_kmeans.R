
# ----Set-Up----

library(terra)
library(tidyr)
library(dplyr)
library(car) # Box-Cox Transformation
library(ClusterR) # MiniBatchKmeans

library(ggplot2)
library(patchwork)
library(tidyterra)
library(RColorBrewer)
library(rnaturalearth)
library(sf)
library(khroma)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/plot_density_grid.R"))
source(here::here("R/plot_box_or_violin.R"))
source(here::here("R/plot_kmeans_map.R"))

# ----Data-Load-----

# Load resampled raster datasets (AI, TWI, fused)
mi_5km_r <- terra::rast(mi_5km_file) * 0.0001 # Multiply all values by 0.0001 to get original value
fused_5km_r <- terra::rast(fused_5km_file)
cor_twi_vegh_5km_r <- terra::rast(cor_twi_vegh_mosaic_file)

# Crop FLC raster to match extent of AI raster
fused_5km_r <- terra::crop(fused_5km_r, mi_5km_r)

# Stack rasters into a single SpatRaster
stacked <- c(cor_twi_vegh_5km_r,
             fused_5km_r,
             mi_5km_r)

# Convert to data frame for k-means clustering
df <- as.data.frame(stacked, xy = TRUE, na.rm = TRUE)
colnames(df) <- c("lon", "lat", "cor", "fused", "mi")

# save crs for write NetCDF document
tar_crs = terra::crs(mi_5km_r)
rm(mi_5km_r, fused_5km_r, cor_twi_vegh_5km_r, fused_5km_r, stacked); gc()

# ----Data-Raw-Density----

# source(here::here("R/plot_density_grid.R"))
# Select columns
cols <- c("mi", "cor", "fused")
combined_plot <- plot_density_grid(
  df,
  cols,
  nrow = 1,
  width = 6,
  height = 2,
  save_path = here::here("data/figures/03_4_kmeans_data_raw_ds.png")
)

# ----Data-Pre----

# Use Box-Cox transformation to select the optimal lambda (power)
# This helps make the 'mi' variable more normally distributed
pt <- powerTransform(df$mi)
lambda <- pt$lambda
cat("Optimal lambda:", lambda, "\n")

# Apply the Box-Cox transformation using the selected lambda
df$mi_trans <- bcPower(df$mi, lambda)

# Standardize the input variables for k-means clustering
# This centers the data (mean = 0) and scales it (SD = 1)
df_k <- as.data.frame(scale(df[, c("cor", "fused", "mi_trans")]))

# ----Density-Data-Pre----

# Select columns
cols <- c("mi_trans", "cor", "fused")
plot_density_grid(
  df_k,
  cols,
  main_title = "Standardized variables distribution",
  nrow = 1,
  width = 6,
  height = 2,
  save_path = here::here("data/figures/03_4_kmeans_data_pre_ds.png")
)

# ----K-Means----

tictoc::tic()
set.seed(123)

# Use Mini-Batch K-Means
mb_km <- MiniBatchKmeans(
  df_k,                # scaled data
  clusters = 8,         # number of clusters
  batch_size = 10000,   # size of each mini-batch
  num_init = 10,        # number of random initializations
  max_iters = 100       # max iterations
)

# Add clustering results to data frame
df$cluster8c <- predict(mb_km, df_k)
tictoc::toc()

rm(mb_km, df_k); gc()

# ----Save-Cluster-Raster----

colnames(df)
# Convert clustering results back to raster
kmeans_8c_r  <- terra::rast(df[, c("lon", "lat", "cluster8c")],
                            type = "xyz",
                            crs = tar_crs)

# Define output path and write to NetCDF
terra::writeCDF(kmeans_8c_r ,
                filename = kmeans_map_8c_path,
                names = "cluster8c",
                overwrite = TRUE)

message(paste0("Cluster map saved to: ", kmeans_map_8c_path))

# ----Cluster-Box_Vio-----
tictoc::tic()

# define colors
fill_colors = RColorBrewer::brewer.pal(8, "Set2")

# Generate plots for comparing different clusters
pbox_cor    <- plot_box_or_violin(df, "cluster8c", "cor",    "boxplot", "Correlation", fill_colors)
pvio_cor    <- plot_box_or_violin(df, "cluster8c", "cor",    "violin",  NULL, fill_colors)
pbox_mi     <- plot_box_or_violin(df, "cluster8c", "mi",     "boxplot", "MI", fill_colors)
pvio_mi     <- plot_box_or_violin(df, "cluster8c", "mi",     "violin",  NULL, fill_colors)
pbox_fused  <- plot_box_or_violin(df, "cluster8c", "fused",  "boxplot", "Fused", fill_colors)
pvio_fused  <- plot_box_or_violin(df, "cluster8c", "fused",  "violin",  NULL, fill_colors)

# Combine plots into a 3x2 grid
combined_plot <- (
  (pbox_cor | pvio_cor) /
    (pbox_mi  | pvio_mi)  /
    (pbox_fused | pvio_fused)
) +
  plot_layout(guides = "collect") &
  theme(
    legend.position = NULL,
    plot.margin = margin(5, 5, 5, 5),
    axis.text = element_text(size = 10),
    axis.title = element_text(size = 12)
  )

# Save the combined figure
ggsave(
  filename = here::here("data/figures/03_kmeans_8c_combined_plot.png"),
  plot = combined_plot,
  width = 8, height = 6, dpi = 300
)

tictoc::toc()

# ----Plot-K-Means-Global-Map----

tictoc::tic()

# kmeans_8c_r <- terra::rast(kmeans_map_8c_path)
# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

p_8c  <- plot_kmeans_map(kmeans_8c_r,
                         fill_colors = RColorBrewer::brewer.pal(8, "Set2"),
                         title_text = "K-means Classification Map (K=8)") +
  geom_sf(data = coast,colour = 'black', linewidth = 0.1)

ggsave(
  filename = here::here("data/figures/03_kmeans_gl_map_8c.png"),
  plot = p_8c, width = 24, height = 11.5, dpi = 300, units = "in"
)
tictoc::toc()

# ----Plot-Map-Each-Cluster----

tictoc::tic()
# Loop over 8 clusters and save a map for each one
for (i in 1:8) {
  p <- plot_kmeans_map(
    raster = kmeans_8c_r,
    fill_colors = RColorBrewer::brewer.pal(8, "Set2"),
    title_text = paste("Cluster", i),
    highlight_cluster = i
  ) +
    geom_sf(data = coast,colour = 'black', linewidth = 0.1)

  # Save plot
  ggsave(
    filename = here::here(paste0("data/figures/03_kmeans_gl_map_8c_", i, ".png")),
    plot = p, width = 24, height = 11.5, dpi = 300, units = "in"
  )

}
tictoc::toc()

