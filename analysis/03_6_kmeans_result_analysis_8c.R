# ------------ Set Up ----------------------------------------------------------
library(terra)
library(tidyr)
library(dplyr)
library(ggplot2)
library(patchwork)
library(RColorBrewer)
# library(khroma)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/plot_box_or_violin.R"))
source(here::here("R/plot_density_by_cluster.R"))

# ------------ Load Data -------------------------------------------------------

# Load resampled raster datasets (AI, TWI, fused)
ai_5km_r <- terra::rast(ai_5km_file) * 0.0001 # Multiply all values by 0.0001 to get original value
ai_5km_r[ai_5km_r == 0] <- NA

cor_twi_vegh_5km_r <- terra::rast(cor_twi_vegh_mosaic_file)[[1]]

kmeans_8c_r <- terra::rast(kmeans_map_8c_path)
kmeans_7c_r <- terra::rast(kmeans_map_7c_path)

# Crop FLC raster to match extent of AI raster
fused_5km_r <- terra::rast(fused_5km_file)
fused_5km_r <- terra::crop(fused_5km_r, ai_5km_r)

# Stack rasters into a single SpatRaster
stacked <- c(cor_twi_vegh_5km_r,
             fused_5km_r,
             ai_5km_r,
             kmeans_8c_r,
             kmeans_7c_r)

# Convert to data frame for k-means clustering
df <- as.data.frame(stacked, xy = TRUE, na.rm = TRUE)
colnames(df) <- c("lon", "lat", "cor", "fused", "ai", "cluster8c", "cluster7c")

rm(ai_5km_r, fused_5km_r, cor_twi_vegh_5km_r, kmeans_8c_r, kmeans_7c_r)
gc()

# ------------ Data Pre -------------------------------------------------------

df$cluster8c <- factor(df$cluster8c)
df$cluster7c <- factor(df$cluster7c)

df_long <- df |>
  pivot_longer(
    cols = c(cor, fused, ai),
    names_to = "variable",
    values_to = "value"
  )

mean_df <- df |>
  group_by(cluster8c) |>
  summarise(mean_cor = mean(cor, na.rm = TRUE),
            mean_ai = mean(ai, na.rm = TRUE),
            mean_fused = mean(fused, na.rm = TRUE))

# --------- plot density by cluster --------------------------------------------
p_cor <- plot_density_by_cluster(df, "cor", "cluster8c", mean_df, "mean_cor", "Correlation distribution",
                                 facet_ncol = 1, scales = "fixed")

p_ai <- plot_density_by_cluster(df, "ai", "cluster8c", mean_df, "mean_ai", "Aridity index distribution",
                                    facet_ncol = 1, scales = "fixed")

p_fused <- plot_density_by_cluster(df, "fused", "cluster8c", mean_df, "mean_fused", "Fraction of used land distribution",
                                   facet_ncol = 4, scales = "free")

ggsave(filename = here::here("data/figures/03_kmeans_8c_cor_density.png"),
       plot = p_cor, width = 3, height = 8, dpi = 300)

ggsave(filename = here::here("data/figures/03_kmeans_8c_ai_density.png"),
       plot = p_ai, width = 3, height = 8, dpi = 300)

ggsave(filename = here::here("data/figures/03_kmeans_8c_fused_density.png"),
       plot = p_fused, width = 8, height = 6, dpi = 300)


# ----------- plots for cluster comparing -------------------------

# define colors
fill_colors = RColorBrewer::brewer.pal(8, "Set2")

# Generate plots for comparing different clusters
pbox_cor    <- plot_box_or_violin(df, "cluster8c", "cor",    "boxplot", "Correlation", fill_colors)
pvio_cor    <- plot_box_or_violin(df, "cluster8c", "cor",    "violin",  NULL, fill_colors)
pbox_ai     <- plot_box_or_violin(df, "cluster8c", "ai",     "boxplot", "AI", fill_colors)
pvio_ai     <- plot_box_or_violin(df, "cluster8c", "ai",     "violin",  NULL, fill_colors)
pbox_fused  <- plot_box_or_violin(df, "cluster8c", "fused",  "boxplot", "Fused", fill_colors)
pvio_fused  <- plot_box_or_violin(df, "cluster8c", "fused",  "violin",  NULL, fill_colors)

# Combine plots into a 3x2 grid
combined_plot <- (
  (pbox_cor | pvio_cor) /
    (pbox_ai  | pvio_ai)  /
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

# ------ Cleanup ---------------------------------------------------------------

rm(list = ls())
gc
