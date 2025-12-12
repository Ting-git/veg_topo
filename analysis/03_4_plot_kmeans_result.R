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
# library(RColorBrewer) # color palettes
library(rnaturalearth)
library(sf)

# Load custom config and plotting functions
source(here::here("config.R"))
source(here::here("R/plot_box_or_violin.R"))
source(here::here("R/plot_kmeans_map.R"))
# ---- 2. Load Raster Data ----
mi_5km_r <- terra::rast(mi_5km_file) * 0.0001
fused_5km_r <- terra::rast(fused_5km_file)
cor_twi_vegh_5km_r <- terra::rast(cor_twi_vegh_mosaic_file)
kmeans_8c_r <- terra::rast(kmeans_map_8c_path)
ecoregion_r <- terra::rast(ecoregion_5km_path)

mi_5km_r
fused_5km_r
cor_twi_vegh_5km_r
kmeans_8c_r
ecoregion_r

# Stack rasters and convert to dataframe
stacked <- c(cor_twi_vegh_5km_r, fused_5km_r, mi_5km_r, kmeans_8c_r, ecoregion_r)
df <- as.data.frame(stacked, xy = TRUE, na.rm = TRUE)
colnames(df) <- c("lon", "lat", "cor", "fused", "mi", "cluster8c", "BIOME_NUM")

# Clean memory
rm(mi_5km_r, fused_5km_r, cor_twi_vegh_5km_r, ecoregion_r, stacked); gc()

# ---- 3. Rename Clusters ----
df_summary <- df |>
  group_by(cluster8c) |>
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
  ) |>
  arrange(median_mi)

df_summary$median_mi
df_summary$cluster8c

# Manually set the cluster labels according to their median_mi value
cluster_values <- df_summary$cluster8c
cluster_labels <- c(
  "Arid downslope \nsuppression",
  "Arid downslope \nsupport",
  "Upslope \nland use",
  "Transition",
  "Downslope \nland use",
  "Moist downslope \nsupression",
  "Moist downslope \nsupport",
  "Super-moist downslope \nsuppression"
)

# Save the cluster name and value for following global mapping
names(cluster_labels) = cluster_values
save(cluster_values, cluster_labels, file = here::here("data/cluster_data.RData"))

# Assign descriptive labels and order for clusters
df$cluster8c <- factor(df$cluster8c,
                       levels = cluster_values,
                       labels = cluster_labels)

# ---- 4. Cluster Box/Violin Plots ----

text_size = 14
# sub-plotting
pbox_cor    <- plot_box_or_violin(df, "cluster8c", "cor",    "boxplot", bquote(r[.("H, TWI")]), text_size = text_size, show_legend = FALSE)
# pvio_cor    <- plot_box_or_violin(df, "cluster8c", "cor",    "violin",  NULL, show_legend = TRUE)
pbox_mi     <- plot_box_or_violin(df, "cluster8c", "mi",     "boxplot", expression(MI), text_size = text_size, show_legend = FALSE)
# pvio_mi     <- plot_box_or_violin(df, "cluster8c", "mi",     "violin",  NULL, show_legend = TRUE)
pbox_fused  <- plot_box_or_violin(df, "cluster8c", "fused",  "boxplot", bquote(f[.("used")]), text_size = text_size, show_legend = FALSE)
# pvio_fused  <- plot_box_or_violin(df, "cluster8c", "fused",  "violin",  NULL, show_legend = TRUE)

# combined_plot <- (
#   (pbox_cor | pvio_cor) /
#     (pbox_mi  | pvio_mi)  /
#     (pbox_fused | pvio_fused)
# ) +
#   plot_layout(guides = "collect") &
#   theme(
#     legend.position = NULL,
#     plot.margin = margin(5, 5, 5, 5),
#     axis.text = element_text(size = 10),
#     axis.title = element_text(size = 12)
#   )
# ggsave(
#   filename = here::here("data/figures/03_kmeans_8c_data_distribution.png"),
#   plot = combined_plot,
#   width = 14, height = 10, dpi = 300
# )

boxplot <- (
  (pbox_mi) /
    (pbox_cor)  /
    (pbox_fused)
) +
  plot_layout(guides = "collect") &
  theme(
    legend.position = NULL,
    plot.margin = margin(5, 5, 5, 5),
    axis.text = element_text(size = 10),
    axis.title = element_text(size = 14),
    panel.background = element_rect(fill = "white", color = NA),  # panel 内部白色
    plot.background  = element_blank(),                            # plot 外部透明
    legend.background = element_blank(),
    legend.box.background = element_blank()
  )

ggsave(
  filename = here::here("data/figures/03_kmeans_8c_boxplot.png"),
  plot = boxplot,
  width = 3, height = 4, dpi = 300
)

# ----5. Plot-K-Means-Global-Map----

tictoc::tic()
# ----Result-Data-Load-----
# Load cluster8c raster
# kmeans_8c_r <- terra::rast(kmeans_map_8c_path)

# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# plot k-means map
p_8c  <- plot_kmeans_map(
  kmeans_8c_r,
  text_size = text_size,
  title_text = "K-means Cluster Map (K=8)"
) +
  geom_sf(data = coast, colour = 'black', linewidth = 0.1) +
  ggplot2::theme(
    panel.background = element_rect(fill = "white", color = NA),  # panel 内部白色
    plot.background  = element_blank(),                            # plot 外部透明
    legend.background = element_blank(),
    legend.box.background = element_blank()
  )

p_8c_add <- p_8c +
  inset_element(boxplot,
                left = 0,   # 左边位置 (0-1)
                bottom = 0, # 底部位置
                right = 0.19, # 右边位置
                top = 0.66)   # 顶部位置

# Save plot
ggsave(
  filename = here::here("data/figures/03_kmeans_gl_map_8c.png"),
  plot = p_8c_add, width = 14, height = 7.3, dpi = 600, units = "in"
)
tictoc::toc()

#
# # ----Plot-Map-Each-Cluster----
#
# tictoc::tic()
# # Loop over 8 clusters and save a map for each one
# for (i in 1:length(cluster_labels)) {
#
#   cluster <- gsub("\n", " | ", cluster_labels[i])
#
#   p_8c  <- plot_kmeans_map(
#     kmeans_8c_r,
#     text_size = text_size,
#     title_text = paste0("Cluster: ", cluster),
#     highlight_cluster = cluster_labels[i]
#   ) +
#     geom_sf(data = coast, colour = 'black', linewidth = 0.1) +
#     ggplot2::theme(
#       panel.background = element_rect(fill = "white", color = NA),  # panel 内部白色
#       plot.background  = element_blank(),                            # plot 外部透明
#       legend.background = element_blank(),
#       legend.box.background = element_blank()
#     )
#
  # p <- p_8c +
  # inset_element(boxplot,
  #               left = 0,   # 左边位置 (0-1)
  #               bottom = 0, # 底部位置
  #               right = 0.19, # 右边位置
  #               top = 0.66)   # 顶部位置
#   ggsave(
#     filename = here::here(paste0("data/figures/03_kmeans_gl_map_8c_", i, ".png")),
#     plot = p,
#     width = 14,
#     height = 8.5,
#     dpi = 600,
#     units = "in"
#   )
# }
# tictoc::toc()


# ---- 5. Cluster Biome Composition (Absolute Counts) ----
# Load biome info
ecoregion <- vect(ecoregion_path)
biomes_info <- ecoregion |>
  as.data.frame() |>
  select(BIOME_NUM, BIOME_NAME, COLOR_BIO) |>
  distinct() |>
  arrange(BIOME_NUM)

# Summarize absolute counts per cluster × biome
df_biome_summary_counts <- df |>
  group_by(cluster8c, BIOME_NUM) |>
  summarise(count = n(), .groups = "drop") |>
  left_join(
    biomes_info |> distinct(BIOME_NUM, .keep_all = TRUE) |> select(BIOME_NUM, BIOME_NAME, COLOR_BIO),
    by = "BIOME_NUM"
  ) |>
  # Order factors for consistent plotting
  mutate(
    cluster8c = factor(cluster8c, levels = levels(df$cluster8c))
  ) |>
  group_by(cluster8c) |>
  arrange(cluster8c, desc(count)) |>
  mutate(BIOME_NAME = factor(BIOME_NAME, levels = unique(BIOME_NAME))) |>
  ungroup()

# 先计算每个cluster的总数和总百分比
df_total_percent <- df_biome_summary_counts %>%
  group_by(cluster8c) %>%
  summarise(total_count = sum(count)) %>%
  ungroup() %>%
  mutate(
    total_percent = total_count / sum(total_count) * 100,
    label = paste0(round(total_percent, 1), "%")
  )

p_8c_biome_counts <- ggplot(df_biome_summary_counts, aes(x = cluster8c, y = count, fill = BIOME_NAME)) +
  geom_bar(stat = "identity") +
  # 在柱子顶部添加总百分比标签
  geom_text(
    data = df_total_percent,
    aes(x = cluster8c, y = total_count, label = label),
    inherit.aes = FALSE,
    vjust = -0.5,  # 在柱子上方
    size = 3.5,
    fontface = "bold"
  ) +
  scale_fill_manual(
    values = setNames(biomes_info$COLOR_BIO, biomes_info$BIOME_NAME),
    name = "Biome"
  ) +
  labs(
    x = "Cluster",
    y = "Number of Observations",
    title = "Absolute Biome Composition of Each Cluster"
  ) +
  ggplot2::theme_bw(base_size = text_size) +
  ggplot2::theme(
    legend.position = "bottom",
    legend.key.size = unit(0.8, "lines"),
    legend.text = ggplot2::element_text(size = text_size),
    legend.title = ggplot2::element_text(size = text_size),
    axis.title = ggplot2::element_text(size = text_size),
    axis.text = ggplot2::element_text(size = text_size  * 0.9),
    axis.text.y = element_text(
      angle = 90,
      hjust = 1,  # 水平对齐：右对齐
      vjust = 0.5  # 垂直对齐：居中
    ),
    plot.title = ggplot2::element_text(size = text_size * 1.2, face = "bold"),
    plot.title.position = "panel"
  ) +
  guides(fill = guide_legend(ncol = 2))

# Save final biome composition plot
ggsave(
  filename = here::here("data/figures/03_kmeans_8c_biome_counts.png"),
  plot = p_8c_biome_counts,
  width = 14,
  height = 14,
  dpi = 300
)


