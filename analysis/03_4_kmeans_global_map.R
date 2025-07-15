# ------------ Set Up ----------------------------------------------------------
library(terra)
library(ggplot2)
library(tidyterra)
library(rnaturalearth)
library(sf)


# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/plot_kmeans_map.R"))

# ------- Load coastal outline

# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# ------ Load rasters
kmeans_12c_r <- terra::rast(kmeans_map_12c_path)
kmeans_8c_r  <- terra::rast(kmeans_map_8c_path)

plot(kmeans_12c_r)
plot(kmeans_8c_r)
# ------ Create plots
p_12c <- plot_kmeans_map(kmeans_12c_r, k = 12,
                         palette_name = "Paired",
                         title_text = "K-means Classification Map (K=12)")

p_8c  <- plot_kmeans_map(kmeans_8c_r,  k = 8,
                         palette_name = "Set2",
                         title_text = "K-means Classification Map (K=8)")

# ------ Save plots
ggsave(
  filename = here::here("data/figures/03_kmeans_12c_gl_map.png"),
  plot = p_12c, width = 24, height = 11.5, dpi = 300, units = "in"
)

ggsave(
  filename = here::here("data/figures/03_kmeans_8c_gl_map.png"),
  plot = p_8c, width = 24, height = 11.5, dpi = 300, units = "in"
)

