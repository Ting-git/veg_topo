# ----Set-Up----

library(terra)
# library(tidyr)
# library(dplyr)

library(ggplot2)
# library(patchwork)
library(tidyterra)
library(RColorBrewer)
library(rnaturalearth)
library(sf)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/plot_kmeans_map.R"))

# ----Result-Data-Load-----

kmeans_8c_r <- terra::rast(kmeans_map_8c_path)

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
