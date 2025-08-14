# ------------ Set Up ----------------------------------------------------------
library(terra)
library(ggplot2)
library(tidyterra)
library(rnaturalearth)
library(sf)
library(khroma)
library(RColorBrewer)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/plot_kmeans_map.R"))

# ------- Load Data ------------------------------------------------------------

# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# Load rasters
kmeans_7c_r <- terra::rast(kmeans_map_7c_path)
kmeans_8c_r  <- terra::rast(kmeans_map_8c_path)

# ---------- Create and save global maps (K=7) --------------------------------

# Get Okabe & Ito's colorblind-friendly palette with 7 distinct colors
okabe <- color("okabe ito")
fill_colors <- okabe(7)

# Plot map (requires a valid kmeans_7c_r object and a defined plot_kmeans_map function)
p_7c <- plot_kmeans_map(
  kmeans_7c_r,
  fill_colors = fill_colors,
  title_text = "K-means Classification Map (K=7)"
)

ggsave(
  filename = here::here("data/figures/03_kmeans_gl_map_7c.png"),
  plot = p_7c, width = 24, height = 11.5, dpi = 300, units = "in"
)

# ---------- Create and save single cluster maps (K=7) ------------------------

# Loop over 7 clusters and save a map for each one
for (i in 1:7) {
  p <- plot_kmeans_map(
    raster = kmeans_7c_r,
    fill_colors = fill_colors,
    title_text = paste("Cluster", i),
    highlight_cluster = i
  )

  # Save plot
  ggsave(
    filename = here::here(paste0("data/figures/03_kmeans_gl_map_7c_", i, ".png")),
    plot = p, width = 24, height = 11.5, dpi = 300, units = "in"
  )
}

# ---------- Create and save global maps with 8 clusters (K=8) -----------------
p_8c  <- plot_kmeans_map(kmeans_8c_r,
                         fill_colors = RColorBrewer::brewer.pal(8, "Set2"),
                         title_text = "K-means Classification Map (K=8)")

ggsave(
  filename = here::here("data/figures/03_kmeans_gl_map_8c.png"),
  plot = p_8c, width = 24, height = 11.5, dpi = 300, units = "in"
)


# ---------- Create and save single cluster maps (K=8) -------------------------

# Loop over 8 clusters and save a map for each one
for (i in 1:8) {
  p <- plot_kmeans_map(
    raster = kmeans_8c_r,
    fill_colors = RColorBrewer::brewer.pal(8, "Set2"),
    title_text = paste("Cluster", i),
    highlight_cluster = i
  )

  # Save plot
  ggsave(
    filename = here::here(paste0("data/figures/03_kmeans_gl_map_8c_", i, ".png")),
    plot = p, width = 24, height = 11.5, dpi = 300, units = "in"
  )
}

# ------------ Cleanup ---------------------------------------------------------
rm(list = ls())
gc()
