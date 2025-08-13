# ------------ Set Up ----------------------------------------------------------
library(terra)
library(tidyr)
library(dplyr)
library(ggplot2)
library(patchwork)
library(DataExplorer)
library(RColorBrewer)

# Load configuration and functions
source(here::here("config.R"))

# ------------ Data Pre for whole-----------------------------------------------

# Load resampled raster datasets (AI, TWI, fused)
ai_5km_r <- terra::rast(ai_5km_file) * 0.0001 # Multiply all values by 0.0001 to get original value
ai_5km_r[ai_5km_r == 0] <- NA

cor_twi_vegh_5km_r <- terra::rast(cor_twi_vegh_mosaic_file)[[1]]
kmeans_7c_r <- terra::rast(kmeans_map_7c_path)

# Crop FLC raster to match extent of AI raster
fused_5km_r <- terra::rast(fused_5km_file)
fused_5km_r <- terra::crop(fused_5km_r, ai_5km_r)

# Stack rasters into a single SpatRaster
stacked <- c(cor_twi_vegh_5km_r,
             fused_5km_r,
             ai_5km_r,
             kmeans_7c_r)

# Convert to data frame for k-means clustering
df <- as.data.frame(stacked, xy = FALSE, na.rm = TRUE)
colnames(df) <- c("cor", "fused", "ai", "cluster7c")

rm(ai_5km_r, fused_5km_r, cor_twi_vegh_5km_r, kmeans_7c_r)
gc()


# ----------- Overview: summary variables for each group ---------------------

df_sum <- df |>
  group_by(cluster7c) |>
  summarise(across(
    where(is.numeric),
    list(
      min = ~min(.x, na.rm = TRUE),
      q1 = ~quantile(.x, 0.25, na.rm = TRUE),
      median = ~median(.x, na.rm = TRUE),
      q3 = ~quantile(.x, 0.75, na.rm = TRUE),
      max = ~max(.x, na.rm = TRUE)
    ),
    .names = "{.col}_{.fn}"
  ))

df_sum_long <- df_sum %>%
  pivot_longer(
    -cluster7c,
    names_to = c("variable", "stat"),
    names_sep = "_"
  ) %>%
  pivot_wider(
    names_from = stat,
    values_from = value
  )

print(df_sum_long, n = Inf)
# ----------- Overview: plot the Density for all variables  ---------------------

# plot density
p_ds <- DataExplorer::plot_density(df)

# Combine the plot
cp_ds <- patchwork::wrap_plots(p_ds)

# Save plot
ggsave(
  filename = here::here("data/figures/03_kmeans_ds.png"),
  plot = cp_ds,
  width = 10,
  height = 5,
  dpi = 300,
  units = "in"
)

# ----------- Overviw: plot the histogram for all variables  -------------------

# Ploting
p_hg <- DataExplorer::plot_histogram(df)

# combine the plots
cp_hg <- patchwork::wrap_plots(p_hg)

# Save histogram plot
ggsave(
  filename = here::here("data/figures/03_kmeans_hg.png"),
  plot = cp_hg,
  width = 10,
  height = 5,
  dpi = 300,
  units = "in"
)

# ----------- summary (K = 7)---------------------------------------------------

# Summarize the data
df_summary <- df |>
  group_by(cluster7c) |>
  summarise(
    mean_cor = mean(cor, na.rm = TRUE),
    mean_fused = mean(fused, na.rm = TRUE),
    mean_ai = mean(ai, na.rm = TRUE)
  )

# Convert from wide to long format for plotting
df_long <- df_summary |>
  pivot_longer(
    cols = c(mean_cor, mean_fused, mean_ai),
    names_to = "Metric",
    values_to = "MeanValue"
  )

# Create the bar plot with value labels
p_sum_7c <- ggplot(df_long, aes(x = factor(cluster7c, levels = sort(unique(cluster7c))), y = MeanValue, fill = Metric)) +
  geom_bar(stat = "identity", position = position_dodge()) +
  geom_text(aes(label = round(MeanValue, 2)),
            position = position_dodge(width = 0.9),
            vjust = -0.3, size = 3) +
  labs(title = "Mean Values by Cluster (k=7)",
       x = "Cluster",
       y = "Mean Value") +
  theme_bw()

p_sum_7c

# Save plot
ggsave(
  filename = here::here("data/figures/03_kmeans_7c_summary.png"),
  plot = p_sum_7c,
  width = 10,
  height = 6,
  dpi = 300,
  units = "in"
)

# ------------ Cleanup ---------------------------------------------------------
rm(list = ls())
gc()
