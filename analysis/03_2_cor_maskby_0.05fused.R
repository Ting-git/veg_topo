# ---------- SetUp -------------------------------------------------------------
library(terra)
library(ggplot2)
library(tidyterra)
library(scico)
library(rnaturalearth)
library(sf)

source(here::here("config.R"))
source(here::here("R/plot_cor_twi_vegh.R"))

# ---------- data pre ----------------------------------------------------------
# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# load correlation data, raster data
cor_r <- terra::rast(cor_twi_vegh_mosaic_file)[[1]]
ext <- terra::ext(-180, 180, -60, 85)

# load fused data (<0.05), raster data
fused_r <- rast(fused_5km_file)
fused_r <- terra::crop(fused_r, cor_r)
fused_r[fused_r >= 0.05] <- NA

# mask
cor_rm <- terra::mask(cor_r, fused_r)

# ------- Plot global correlation analysis of TWI and VEGH ---------------------

p_cor <- plot_cor_twi_vegh(
  input = cor_rm,
  extent = ext,
  title = "VEGH-TWI Pearson Correlation Map (fused < 0.05)",
  text_size = 14,
  x_breaks = 30,
  y_breaks = 30
) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1)

# save
ggsave(
  filename = file.path(project_root, "data/figures/03_cor_map_within_0.05fused.png"),
  plot = p_cor,
  width = 24,
  height = 11.5,
  dpi = 300,
  units = "in"
)

# ------- Comparison: before and after mask -----------------------------------------------------------

# Convert rasters to data frames
df_cor_r <- as.data.frame(cor_r, xy = FALSE, na.rm = TRUE)
df_cor_rm <- as.data.frame(cor_rm, xy = FALSE, na.rm = TRUE)

# Add source labels
df_cor_r$source <- "r_present (fused < 1)"
df_cor_rm$source <- "r_nature (fused < 0.05)"

# Combine data frames
df_all <- rbind(df_cor_r, df_cor_rm)
colnames(df_all) <- c("value", "source")

# Define consistent colors
my_colors <- c("r" = "#F8766D",             # Example fill color 1 (red-ish)
               "r (fused < 0.05)" = "#00BFC4")  # Example fill color 2 (blue-ish)

# Plot distributions
text_size = 10
p_vs <- ggplot(df_all, aes(x = value, fill = source, color = source)) +
  geom_density(alpha = 0.5, linewidth = 0.8) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "red", size = 1) +
  labs(
    title = "r Value Distribution Before and After Mask (fused < 0.05)",
    x = "r(H~TWI)",
    y = "Density",
    fill = "Group",
    color = "Group"   # 和fill用同一个名字
  ) +
  scale_fill_manual(values = my_colors, guide = "legend") +
  scale_color_manual(values = my_colors, guide = "legend") +  # 让两个图例合并
  theme_bw(base_size = text_size) +
  theme(
    legend.position = "bottom",
    legend.text = element_text(size = text_size),
    legend.title = element_text(size = text_size),
    axis.title = element_text(size = text_size),
    axis.text = element_text(size = text_size * 0.9),
    plot.title = element_text(size = text_size * 1.2, face = "bold"),
    plot.title.position = "panel"
  )


# Save the plot
ggsave(
  filename = file.path(project_root, "data/figures/03_cor_comparison_fused_mask.png"),
  plot = p_vs,
  width = 6,
  height = 4,
  dpi = 300,
  units = "in"
)

# ------- Data Pre for Plot -----------------------------------------------------------

mat_5km_r <- rast(mat_5km_file)
ecoregion_r <- rast(ecoregion_5km_path)

stacked <- c(cor_rm, mat_5km_r, ecoregion_r)

# ------- Plot cor VS MAT -----------------------------------------------------------

# ------- Plot cor VS BIOME -----------------------------------------------------------

