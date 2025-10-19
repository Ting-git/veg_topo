# ----Set-Up----

library(terra)
# library(tidyr)
# library(dplyr)

library(ggplot2)
# library(patchwork)
library(tidyterra)
# library(RColorBrewer)
library(rnaturalearth)
library(sf)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/plot_kmeans_map.R"))

# ----Result-Data-Load-----
# Load cluster8c raster
kmeans_8c_r <- terra::rast(kmeans_map_8c_path)

# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# Reorder cluster_value and cluster_labels with an defined order
load(here::here("data/cluster_data.RData")) # cluster_values, cluster_labels

# fill_color for dry to wet cluster
fill_colors <- c(
  "#E78AC3",  # Pink - Arid
  "#FC8D62",  # Orange - Semi-arid
  "#FFD92F",  # Yellow - Semi-arid
  "#E5C494",  # Light brown - Dry-sub-humid
  "#B3B3B3",  # Gray - Humid
  # "#1B9E77",  # Dark-green - Humid
  "#66C2A5", # Blue-green - Humid
  "#8DA0CB",  # Blue - Humid
  "#A6D854"   # Green - Humid
)

# ----Plot-K-Means-Global-Map----

tictoc::tic()


# plot k-means map
p_8c  <- plot_kmeans_map(
  kmeans_8c_r,
  fill_colors = fill_colors,
  cluster_labels = cluster_labels,
  title_text = "K-means Cluster Map (K=8)"
) +
  geom_sf(data = coast, colour = 'black', linewidth = 0.1)

# Save plot
ggsave(
  filename = here::here("data/figures/03_kmeans_gl_map_8c.png"),
  plot = p_8c, width = 14, height = 7, dpi = 600, units = "in"
)
tictoc::toc()

# ----Plot-Map-Each-Cluster----

tictoc::tic()
# Loop over 8 clusters and save a map for each one
for (i in 1:length(cluster_labels)) {

  cluster <- gsub("\n", " | ", cluster_labels[i])

  p <- plot_kmeans_map(
    kmeans_8c_r,
    fill_colors = fill_colors,
    cluster_labels = cluster_labels,
    title_text = paste0("Cluster: ", cluster),
    highlight_cluster = cluster_labels[i]
  ) +
    geom_sf(data = coast, colour = 'black', linewidth = 0.1)

  ggsave(
    filename = here::here(paste0("data/figures/03_kmeans_gl_map_8c_", i, ".png")),
    plot = p,
    width = 14,
    height = 7,
    dpi = 600,
    units = "in"
  )
}
tictoc::toc()
