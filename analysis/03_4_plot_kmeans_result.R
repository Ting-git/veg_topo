# ========================================================
#  Script: K-means 8-cluster analysis and biome composition
#  Purpose: Load raster data, summarize clusters, generate box/violin plots,
#           and plot absolute biome composition per cluster.
#  Author: Ting Tan
#  Date: 2025-09-01
# ========================================================

# ---- 1. Load Packages ----
library(terra)        # raster processing
library(dplyr)        # data manipulation
library(ggplot2)      # plotting
library(patchwork)    # combine multiple plots
library(tidyterra)    # ggplot-friendly terra functions
library(RColorBrewer) # color palettes

# Load custom config and plotting functions
source(here::here("config.R"))
source(here::here("R/plot_box_or_violin.R"))

# ---- 2. Load Raster Data ----
mi_5km_r <- terra::rast(mi_5km_file) * 0.0001
fused_5km_r <- terra::rast(fused_5km_file)
cor_twi_vegh_5km_r <- terra::rast(cor_twi_vegh_mosaic_file)
kmeans_8c_r <- terra::rast(kmeans_map_8c_path)
ecoregion_r <- terra::rast(ecoregion_5km_path)

# Stack rasters and convert to dataframe
stacked <- c(cor_twi_vegh_5km_r, fused_5km_r, mi_5km_r, kmeans_8c_r, ecoregion_r)
df <- as.data.frame(stacked, xy = TRUE, na.rm = TRUE)
colnames(df) <- c("lon", "lat", "cor", "fused", "mi", "cluster8c", "BIOME_NUM")

# Clean memory
rm(mi_5km_r, fused_5km_r, cor_twi_vegh_5km_r, kmeans_8c_r, ecoregion_r, stacked); gc()

# ---- 3. Rename Clusters ----
df_summary <- df %>%
  group_by(cluster8c) %>%
  summarise(
    Q1_mi = quantile(mi, 0.25),
    median_mi = median(mi),
    Q3_mi = quantile(mi, 0.75),
    Q1_cor = quantile(cor, 0.25),
    median_cor = median(cor),
    Q3_cor = quantile(cor, 0.75),
    Q1_fused = quantile(fused, 0.25),
    median_fused = median(fused),
    Q3_fused = quantile(fused, 0.75),
    .groups = "drop"
  ) %>%
  arrange(median_mi)

cluster_values <- c(4, 8, 2, 6, 7, 5, 1, 3)

cluster_labels <- c(
  "Arid\nN\nLowLU",
  "Semi-arid\nS+\nLowLU",
  "Semi-arid\n+\nHighLU",
  "Dry-sub-humid\nW-\nLowLU",
  "Humid\n-\nHighLU",
  "Humid\nS-\nLowLU",
  "Humid\n+\nLowLU",
  "Humid\nW-\nLowLU"
)

# Assign descriptive labels and order for clusters
df$cluster8c <- factor(df$cluster8c,
                       levels = cluster_values,
                       labels = cluster_labels)

# ---- 4. Cluster Box/Violin Plots ----
set2_colors <- RColorBrewer::brewer.pal(8, "Set2")
cluster_num_for_colors <- cluster_values
fill_colors <- setNames(set2_colors[cluster_num_for_colors], cluster_labels)

pbox_cor    <- plot_box_or_violin(df, "cluster8c", "cor",    "boxplot", "Correlation", fill_colors, show_legend = FALSE)
pvio_cor    <- plot_box_or_violin(df, "cluster8c", "cor",    "violin",  NULL, fill_colors, show_legend = TRUE)
pbox_mi     <- plot_box_or_violin(df, "cluster8c", "mi",     "boxplot", "MI", fill_colors, show_legend = FALSE)
pvio_mi     <- plot_box_or_violin(df, "cluster8c", "mi",     "violin",  NULL, fill_colors, show_legend = TRUE)
pbox_fused  <- plot_box_or_violin(df, "cluster8c", "fused",  "boxplot", "Fused", fill_colors, show_legend = FALSE)
pvio_fused  <- plot_box_or_violin(df, "cluster8c", "fused",  "violin",  NULL, fill_colors, show_legend = TRUE)

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

ggsave(
  filename = here::here("data/figures/03_kmeans_8c_data_distribution.png"),
  plot = combined_plot,
  width = 8, height = 6, dpi = 300
)

# ---- 5. Cluster Biome Composition (Absolute Counts) ----
# Load biome info
ecoregion <- vect(ecoregion_path)
biomes_info <- ecoregion %>%
  as.data.frame() %>%
  select(BIOME_NUM, BIOME_NAME, COLOR_BIO) %>%
  distinct() %>%
  arrange(BIOME_NUM)

# Summarize absolute counts per cluster × biome
df_biome_summary_counts <- df %>%
  group_by(cluster8c, BIOME_NUM) %>%
  summarise(count = n(), .groups = "drop") %>%
  left_join(
    biomes_info %>% distinct(BIOME_NUM, .keep_all = TRUE) %>% select(BIOME_NUM, BIOME_NAME, COLOR_BIO),
    by = "BIOME_NUM"
  ) %>%
  # Order factors for consistent plotting
  mutate(
    cluster8c = factor(cluster8c, levels = levels(df$cluster8c))
  ) %>%
  group_by(cluster8c) %>%
  arrange(cluster8c, desc(count)) %>%
  mutate(BIOME_NAME = factor(BIOME_NAME, levels = unique(BIOME_NAME))) %>%
  ungroup()

# Plot stacked bar chart using absolute counts
p_8c_biome_counts <- ggplot(df_biome_summary_counts, aes(x = cluster8c, y = count, fill = BIOME_NAME)) +
  geom_bar(stat = "identity") +
  scale_fill_manual(
    values = setNames(biomes_info$COLOR_BIO, biomes_info$BIOME_NAME),
    name = "Biome"
  ) +
  labs(
    x = "Cluster",
    y = "Number of Observations",
    title = "Absolute Biome Composition of Each Cluster"
  ) +
  theme_bw() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    plot.title = element_text(hjust = 0.5),
    legend.position = "bottom",
    legend.key.size = unit(0.8, "lines"),
    legend.text = element_text(size = 10)
  ) +
  guides(fill = guide_legend(ncol = 1))

# Save final biome composition plot
ggsave(
  filename = here::here("data/figures/03_kmeans_8c_biome_counts.png"),
  plot = p_8c_biome_counts,
  width = 8,
  height = 10,
  dpi = 300
)
