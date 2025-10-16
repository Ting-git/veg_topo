# ==============================================================================
# Script: mask_correlation_with_fused.R
# Author: Ting Tan
# Date: 2025-08-31
#
# Description:
# This script analyzes the correlation between TWI (Topographic Wetness Index)
# and VEGH (Vegetation Height) under different masking conditions. It:
#   1. Loads correlation and fused land use data
#   2. Masks correlation raster to areas where fused < 0.05
#   3. Saves the masked raster as NetCDF
#   4. Plots global correlation map with masking applied
#   5. Compares correlation distributions before and after masking
#
# Dependencies:
#   - terra, ggplot2, tidyterra, scico, rnaturalearth, sf
#   - config.R (file paths), plot_cor_twi_vegh.R (custom plotting function)
# ==============================================================================

# ------------------------- Set Up ---------------------------------------------
library(terra)
library(ggplot2)
library(tidyterra)
library(scico)
library(rnaturalearth)
library(sf)

source(here::here("config.R"))
source(here::here("R/plot_cor_twi_vegh.R"))

# ------------------------- Load Data ------------------------------------------
# Coastline vector
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# Correlation raster (TWI vs VEGH)
cor_r <- terra::rast(cor_twi_vegh_mosaic_file)[[1]]

# Fused raster (< 0.05) for masking
fused_r <- rast(fused_5km_file)
fused_r <- terra::crop(fused_r, cor_r)
fused_r[fused_r >= 0.05] <- NA

# Apply mask
cor_rm <- terra::mask(cor_r, fused_r)

# Save masked raster as NetCDF
writeCDF(cor_rm, cor_twi_vegh_mask_fused0.05_file,
         varname = "r_H_TWI", overwrite = TRUE)
if (file.exists(cor_twi_vegh_mask_fused0.05_file)) {
  message(paste("✅ Saved:", cor_twi_vegh_mask_fused0.05_file))
}

# ------------------------- Plot Global Correlation ----------------------------
p_cor <- plot_cor_twi_vegh(
  input = cor_rm,
  extent = ext_global,
  title = "Pearson's r (H~TWI) (fused < 0.05)",
  text_size = 12,
  x_step = 30,
  y_step = 30
) +
  geom_sf(data = coast, colour = "black", linewidth = 0.1)

# Save global correlation map
ggsave(
  filename = file.path(project_root, "data/figures/03_cor_map_within_0.05fused.png"),
  plot = p_cor, width = 14, height = 7, dpi = 600, units = "in"
)

# ------------------------- Comparison: Before vs After Mask -------------------
# Convert rasters to data frames
df_cor_r <- as.data.frame(cor_r, xy = FALSE, na.rm = TRUE)
df_cor_rm <- as.data.frame(cor_rm, xy = FALSE, na.rm = TRUE)

# Label sources
df_cor_r$source <- "r_present (fused < 1)"
df_cor_rm$source <- "r_nature (fused < 0.05)"

# Merge
df_all <- rbind(df_cor_r, df_cor_rm)
colnames(df_all) <- c("value", "source")

# Define consistent colors for groups
my_colors <- c(
  "r_present (fused < 1)" = "#F8766D",    # red
  "r_nature (fused < 0.05)" = "#00BFC4"   # blue
)

# Density comparison plot
text_size <- 12
p_vs <- ggplot(df_all, aes(x = value, fill = source, color = source)) +
  geom_density(alpha = 0.5, linewidth = 0.5) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "red", linwidth = 1) +
  labs(
    title = "r Value Distribution Before and After Mask (fused < 0.05)",
    x = "r(H~TWI)",
    y = "Density",
    fill = "Group",
    color = "Group"
  ) +
  scale_fill_manual(values = my_colors, guide = "legend") +
  scale_color_manual(values = my_colors, guide = "legend") +
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

# Save comparison plot
ggsave(
  filename = file.path(project_root, "data/figures/03_cor_comparison_fused_mask.png"),
  plot = p_vs, width = 14, height = 7, dpi = 600, units = "in"
)
