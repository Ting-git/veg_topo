# ------------ Set Up ----------------------------------------------------------
library(terra)
library(tidyr)
library(dplyr)
library(ggplot2)
library(patchwork)
library(RColorBrewer)


# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/plot_box_or_violin.R"))
source(here::here("R/plot_density_by_cluster.R"))

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


df$cluster8c <- factor(df$cluster8c)
df$cluster12c <- factor(df$cluster12c)

df_long <- df |>
  pivot_longer(
    cols = c(cor, fused, ai),
    names_to = "variable",
    values_to = "value"
  )

mean_df <- df |>
  group_by(cluster12c) |>
  summarise(mean_cor = mean(cor, na.rm = TRUE),
            mean_ai = mean(ai, na.rm = TRUE),
            mean_fused = mean(fused, na.rm = TRUE))

# --------- plot density by cluster --------------------------------------------
p_cor <- plot_density_by_cluster(df, "cor", "cluster12c", mean_df, "mean_cor", "Correlation distribution",
                                 facet_ncol = 1, scales = "fixed")

p_ai <- plot_density_by_cluster(df, "ai", "cluster12c", mean_df, "mean_ai", "Aridity index distribution",
                                facet_ncol = 1, scales = "fixed")

p_fused <- plot_density_by_cluster(df, "fused", "cluster12c", mean_df, "mean_fused", "Fraction of used land distribution",
                                   facet_ncol = 4, scales = "free")

ggsave(filename = here::here("data/figures/03_kmeans_12c_cor_density.png"),
       plot = p_cor, width = 6, height = 12, dpi = 300)

ggsave(filename = here::here("data/figures/03_kmeans_12c_ai_density.png"),
       plot = p_ai, width = 6, height = 12, dpi = 300)

ggsave(filename = here::here("data/figures/03_kmeans_12c_fused_density.png"),
       plot = p_fused, width = 12, height = 10, dpi = 300)

# ----------- plots for cluster comparing -------------------------

# Define fill colors
fill_colors <- RColorBrewer::brewer.pal(12, "Paired")

# Generate plots for comparing different clusters
pbox_cor    <- plot_box_or_violin(df, "cluster12c", "cor",    "boxplot", "Correlation")
pvio_cor    <- plot_box_or_violin(df, "cluster12c", "cor",    "violin",  NULL)
pbox_ai     <- plot_box_or_violin(df, "cluster12c", "ai",     "boxplot", "AI")
pvio_ai     <- plot_box_or_violin(df, "cluster12c", "ai",     "violin",  NULL)
pbox_fused  <- plot_box_or_violin(df, "cluster12c", "fused",  "boxplot", "Fused")
pvio_fused  <- plot_box_or_violin(df, "cluster12c", "fused",  "violin",  NULL)

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
  filename = here::here("data/figures/03_kmeans_12c_combined_plot.png"),
  plot = combined_plot,
  width = 15, height = 10, dpi = 300
)

