# ------------ Set Up ----------------------------------------------------------
library(terra)
library(tidyr)
library(dplyr)
library(ggplot2)
library(patchwork)
# library(RColorBrewer)
library(khroma)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/plot_box_or_violin.R"))
source(here::here("R/plot_density_by_cluster.R"))

# ------------ Data Pre for whole-----------------------------------------------

# Load resampled raster datasets (AI, TWI, fused)
ai_5km_r <- terra::rast(ai_5km_file) * 0.0001 # Multiply all values by 0.0001 to get original value
ai_5km_r[ai_5km_r == 0] <- NA
log_ai_5km_r <- log(ai_5km_r)

cor_twi_vegh_5km_r <- terra::rast(cor_twi_vegh_mosaic_file)[[1]]
kmeans_8c_r <- terra::rast(kmeans_map_8c_path)
kmeans_7c_r <- terra::rast(kmeans_map_7c_path)

# Crop FLC raster to match extent of AI raster
fused_5km_r <- terra::rast(fused_5km_file)
fused_5km_r <- terra::crop(fused_5km_r, ai_5km_r)

# Stack rasters into a single SpatRaster
stacked <- c(cor_twi_vegh_5km_r,
             fused_5km_r,
             log_ai_5km_r,
             kmeans_8c_r,
             kmeans_7c_r)

# Convert to data frame for k-means clustering
df <- as.data.frame(stacked, xy = TRUE, na.rm = TRUE)
colnames(df) <- c("lon", "lat", "cor", "fused", "log_ai", "cluster8c", "cluster7c")

rm(ai_5km_r, fused_5km_r, cor_twi_vegh_5km_r, kmeans_8c_r, kmeans_7c_r)
gc()

df$cluster8c <- factor(df$cluster8c)
df$cluster7c <- factor(df$cluster7c)

df_long <- df |>
  pivot_longer(
    cols = c(cor, fused, log_ai),
    names_to = "variable",
    values_to = "value"
  )

mean_df <- df |>
  group_by(cluster7c) |>
  summarise(mean_cor = mean(cor, na.rm = TRUE),
            mean_log_ai = mean(log_ai, na.rm = TRUE),
            mean_fused = mean(fused, na.rm = TRUE))

# --------- plot density by cluster --------------------------------------------
p_cor <- plot_density_by_cluster(df, "cor", "cluster7c", mean_df, "mean_cor", "Correlation distribution",
                                 facet_ncol = 1, scales = "fixed")

p_log_ai <- plot_density_by_cluster(df, "log_ai", "cluster7c", mean_df, "mean_log_ai", "Logarithmic aridity index distribution",
                                facet_ncol = 1, scales = "fixed")

p_fused <- plot_density_by_cluster(df, "fused", "cluster7c", mean_df, "mean_fused", "Fraction of used land distribution",
                                   facet_ncol = 4, scales = "free")

ggsave(filename = here::here("data/figures/03_kmeans_7c_cor_density.png"),
       plot = p_cor, width = 6, height = 12, dpi = 300)

ggsave(filename = here::here("data/figures/03_kmeans_7c_log_ai_density.png"),
       plot = p_log_ai, width = 6, height = 12, dpi = 300)

ggsave(filename = here::here("data/figures/03_kmeans_7c_fused_density.png"),
       plot = p_fused, width = 12, height = 10, dpi = 300)

# ----------- plots for cluster comparing -------------------------

# Get Okabe & Ito's colorblind-friendly palette with 7 distinct colors
okabe <- color("okabe ito")
fill_colors <- okabe(7)

# Generate plots for comparing different clusters
pbox_cor    <- plot_box_or_violin(df, "cluster7c", "cor",    "boxplot", "Correlation", fill_colors)
pvio_cor    <- plot_box_or_violin(df, "cluster7c", "cor",    "violin",  NULL, fill_colors)
pbox_log_ai     <- plot_box_or_violin(df, "cluster7c", "log_ai",     "boxplot", "log(AI)", fill_colors)
pvio_log_ai     <- plot_box_or_violin(df, "cluster7c", "log_ai",     "violin",  NULL, fill_colors)
pbox_fused  <- plot_box_or_violin(df, "cluster7c", "fused",  "boxplot", "Fused", fill_colors)
pvio_fused  <- plot_box_or_violin(df, "cluster7c", "fused",  "violin",  NULL, fill_colors)

# Combine plots into a 3x2 grid
combined_plot <- (
  (pbox_cor | pvio_cor) /
    (pbox_log_ai  | pvio_log_ai)  /
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
  filename = here::here("data/figures/03_kmeans_7c_combined_plot.png"),
  plot = combined_plot,
  width = 15, height = 10, dpi = 300
)

