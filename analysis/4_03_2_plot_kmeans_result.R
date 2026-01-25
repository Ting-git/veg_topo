# ========================================================
#  Script: K-means 8-cluster analysis and biome composition
#  Purpose:
#    - Load raster datasets
#    - Summarize K-means (K=8) clusters
#    - Visualize spatial patterns and statistical distributions
#    - Quantify absolute biome composition per cluster
# ========================================================

# ========================================================
# 1. Load required packages
# ========================================================

library(terra)        # raster processing
library(dplyr)        # data manipulation
library(ggplot2)      # plotting
library(patchwork)    # multi-panel layouts
library(tidyterra)    # terra + ggplot2 bridge
library(rnaturalearth)# coastline data
library(sf)           # vector data handling


# ========================================================
# 2. Load configuration and custom functions
# ========================================================

# Paths, global extent, and shared parameters
source(here::here("config.R"))

# Reusable plotting utilities
source(here::here("R/plot_box_or_violin.R"))
source(here::here("R/plot_kmeans_map.R"))


# ========================================================
# 3. Load raster datasets
# ========================================================

# Environmental variables and cluster map (5 km resolution)
mi_5km_r           <- terra::rast(mi_5km_file) * 0.0001
fused_5km_r        <- terra::rast(fused_5km_file)
cor_twi_vegh_5km_r <- terra::rast(cor_twi_vegh_mosaic_file)

# K-means (K = 8) result and biome classification
kmeans_8c_r  <- terra::rast(kmeans_map_8c_path)
ecoregion_r  <- terra::rast(ecoregion_5km_path)

# Quick sanity checks
mi_5km_r
fused_5km_r
cor_twi_vegh_5km_r
kmeans_8c_r
ecoregion_r


# ========================================================
# 4. Raster stacking and dataframe conversion
# ========================================================

# Stack all rasters into a single object
stacked <- c(
  cor_twi_vegh_5km_r,
  fused_5km_r,
  mi_5km_r,
  kmeans_8c_r,
  ecoregion_r
)

# Convert raster stack to point-wise dataframe
df <- as.data.frame(stacked, xy = TRUE, na.rm = TRUE)

# Explicit column naming for downstream clarity
colnames(df) <- c(
  "lon", "lat",
  "cor", "fused", "mi",
  "cluster8c",
  "BIOME_NUM"
)

# Free memory (large rasters no longer needed)
rm(
  mi_5km_r,
  fused_5km_r,
  cor_twi_vegh_5km_r,
  ecoregion_r,
  stacked
)
gc()


# ========================================================
# 5. Cluster-level statistics
# ========================================================

# Summarize distribution of key variables per cluster
df_summary <- df |>
  group_by(cluster8c) |>
  summarise(
    count      = n(),
    percentage = n() / nrow(df) * 100,

    Q1_mi      = quantile(mi, 0.25),
    median_mi  = median(mi),
    Q3_mi      = quantile(mi, 0.75),

    Q1_cor     = quantile(cor, 0.25),
    median_cor = median(cor),
    Q3_cor     = quantile(cor, 0.75),

    Q1_fused   = quantile(fused, 0.25),
    median_fused = median(fused),
    Q3_fused   = quantile(fused, 0.75),

    .groups = "drop"
  ) |>
  mutate(percentage = round(percentage, 2)) |>
  arrange(median_mi)   # order clusters along MI gradient

# Inspect ordering
df_summary$median_mi
df_summary$cluster8c
df_summary$percentage


# ========================================================
# 6. Assign descriptive cluster labels and colors
# ========================================================

# Cluster IDs ordered by median MI
cluster_values <- df_summary$cluster8c

# Human-readable labels (interpretation-driven)
cluster_labels <- c(
  "Arid downslope \nsuppression",
  "Arid downslope \nsupport",
  "Upslope \nland use",
  "Transition",
  "Downslope \nland use",
  "Moist downslope \nsupression",
  "Moist downslope \nsupport",
  "Super-moist \ndownslope suppression"
)

# Map labels to original cluster IDs
names(cluster_labels) <- cluster_values

# Fixed color palette for all figures
fill_colors <- setNames(
  c(
    "#E78AC3", "#FC8D62", "#FFD92F", "#E5C494",
    "#B3B3B3", "#66C2A5", "#8DA0CB", "#A6D854"
  ),
  cluster_labels
)

# Apply ordered factor with descriptive labels
df$cluster8c <- factor(
  df$cluster8c,
  levels = cluster_values,
  labels = cluster_labels
)

# Apply the same factor ordering to summary table
df_summary <- df_summary |>
  mutate(
    cluster8c = factor(
      cluster_labels[as.character(cluster8c)],
      levels = cluster_labels
    )
  )


# ========================================================
# 7. Global K-means cluster map
# ========================================================

text_size <- 14

# Load global coastline for map context
coast <- rnaturalearth::ne_coastline(
  scale = 110,
  returnclass = "sf"
)

# Base global cluster map
p_8c <- plot_kmeans_map(
  kmeans_8c_r,
  text_size  = text_size,
  extent     = ext_global,
  title_text = "K-means Cluster Map (K=8)",
  land_color = "white"
) +
  geom_sf(data = coast, colour = "black", linewidth = 0.1) +
  coord_sf(
    xlim   = c(xmin(ext_global), xmax(ext_global)),
    ylim   = c(ymin(ext_global), ymax(ext_global)),
    expand = FALSE,
    clip   = "on"
  ) +
  theme(
    legend.position  = "none",
    axis.title       = element_blank(),
    panel.background = element_rect(fill = "white", color = NA),
    plot.background  = element_blank()
  ) +
  labs(tag = "a)") +
  theme(
    plot.tag = element_text(size = text_size, face = "bold"),
    plot.tag.position = c(0.01, 1)
  )


# ========================================================
# 8. Cluster-wise variable distributions
# ========================================================

# MI distribution
pbox_mi <- plot_box_or_violin(
  df, "cluster8c", "mi", "boxplot",
  expression(MI),
  text_size = text_size,
  show_legend = FALSE
) + labs(tag = "b)")

# Fused index distribution
pbox_fused <- plot_box_or_violin(
  df, "cluster8c", "fused", "boxplot",
  bquote(f[.("used")]),
  text_size = text_size,
  show_legend = FALSE
) + labs(tag = "c)")

# Correlation distribution
pbox_cor <- plot_box_or_violin(
  df, "cluster8c", "cor", "boxplot",
  bquote(r[.("H, TWI")]),
  text_size = text_size,
  show_legend = FALSE
) + labs(tag = "d)")


# ========================================================
# 9. Cluster area percentage
# ========================================================

# Percentage of global pixels per cluster
p_bar <- ggplot(
  df_summary,
  aes(x = cluster8c, y = percentage, fill = cluster8c)
) +
  geom_col(width = 0.9, color = "black", linewidth = 0.2) +
  geom_text(
    aes(label = sprintf("%.1f", percentage)),
    vjust = -0.5,
    size = 3
  ) +
  scale_fill_manual(values = fill_colors) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.1))) +
  theme_bw(base_size = text_size) +
  theme(
    legend.position = "none",
    axis.text.x     = element_blank()
  ) +
  labs(
    y   = "Percentage (%)",
    tag = "e)"
  )


# ========================================================
# 10. Combine map and statistics into one figure
# ========================================================

# Extract legend from map
p_8c_legend <- p_8c + theme(legend.position = "bottom")

# Final multi-panel layout
p_8c_stat <- wrap_plots(
  p_8c,
  wrap_plots(pbox_mi, pbox_fused, pbox_cor, p_bar, nrow = 1),
  wrap_elements(cowplot::get_legend(p_8c_legend)),
  nrow = 3,
  heights = c(6.5, 2.7, 0.8)
)

# Save high-resolution figure
ggsave(
  filename = here::here("data/figures/03_kmeans_gl_map_8c_stat.png"),
  plot     = p_8c_stat,
  width    = 14,
  height   = 10,
  dpi      = 600,
  units    = "in"
)

