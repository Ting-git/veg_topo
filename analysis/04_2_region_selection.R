
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

ai_55km_r <- terra::rast(ai_55km_file) * 0.0001 # Multiply all values by 0.0001 to get original value
fused_55km_r <- terra::rast(fused_55km_file)
dem_sd_55km_r <- terra::rast(dem_sd_55km_path)
fpa_55km_r <- terra::rast(fpa_55km_path)

# Stack rasters into a single SpatRaster
stacked <- c(ai_55km_r,
             fused_55km_r,
             dem_sd_55km_r,
             fpa_55km_r)

# Convert to data frame
df <- as.data.frame(stacked, xy = TRUE, na.rm = TRUE)
colnames(df) <- c("lon", "lat", "ai", "fused", "relief", "fpa")

# Clean up
rm(ai_55km_r, fused_55km_r, dem_55km_r, fpa_55km_r)
gc()



# ----------- Overview: plot the histogram for all variables  -------------------

# Ploting
p_hg <- DataExplorer::plot_histogram(df)

# combine the plots
cp_hg <- patchwork::wrap_plots(p_hg)

# Save histogram plot
ggsave(
  filename = here::here("data/figures/04_variables_hg.png"),
  plot = cp_hg,
  width = 10,
  height = 5,
  dpi = 300,
  units = "in"
)


# ------------ create bins -----------------------------------------------

df_binned <- df |>

  # create bins
  mutate(
    ai_bin = cut(ai, breaks = quantile(ai, probs = seq(0, 1, 1/5)), include.lowest = TRUE),
    relief_bin = cut(relief, breaks = quantile(relief, probs = seq(0, 1, 1/3)), include.lowest = TRUE),
    abs_lat = abs(lat),
    abs_lat_bin = cut(abs_lat, breaks = quantile(abs_lat, probs = seq(0, 1, 1/5)), include.lowest = TRUE)
  ) |>

  # use only data where influence of humans on veg height is minimal
  filter(fused < 0.05, fpa > 0.95) |>

  # Filter to retain only data that is in bins 1, 3, and 5 for MI and Lat.
  # and for bins 1 and 3 for relief

  filter(
    # retain only data that is in bins 1, 3, and 5
    ai_bin %in% levels(ai_bin)[c(1, 3, 5)],
    # retain only data that is in bins 1 and 3
    relief_bin %in% levels(relief_bin)[c(1, 3)],
    # retain only data that is in bins 1, 3, and 5
    abs_lat_bin %in% levels(abs_lat_bin)[c(1, 3, 5)]
  ) |>

  # Fig. 6 Down Valley drainage
  mutate(strata_A = interaction(ai_bin, relief_bin, drop = TRUE)) |>

  # Fig. 6 Aspect difference
  mutate(strata_B = interaction(abs_lat_bin, relief_bin, drop = TRUE)) |>

  select(-abs_lat, -ai_bin, -relief_bin, -abs_lat_bin)



# ---- Plotting potential regions -----------------------------------------------------------------
library(rnaturalearth)
library(sf)

# check the bins and the combinations
table(df_binned$strata_A)
table(df_binned$strata_B)

# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

ggplot(df_binned, aes(x = lon, y = lat, fill = strata_A)) +
  geom_tile() +
  geom_sf(data = coast, colour = 'black', linewidth = 0.1, inherit.aes = FALSE) +
  theme_bw() +
  coord_sf()

ggplot(df_binned, aes(x = lon, y = lat, fill = strata_B)) +
  geom_tile() +
  geom_sf(data = coast, colour = 'black', linewidth = 0.1, inherit.aes = FALSE) +
  theme_bw() +
  coord_sf()

# ---- Sampling and Saving----------------------------------------------------------------

# Pick one region (coarse gridcell) representing all combination of bins.
# Should yield 6 samples

set.seed(999)

# for Fig. 6 Down Valley drainage
df_samples_A <- df_binned |>
  group_by(strata_A) |>
  sample_n(size = 1, replace = FALSE) |>
  mutate(
    xmin = lon - 0.25,
    xmax = lon + 0.25,
    ymin = lat - 0.25,
    ymax = lat + 0.25
  ) |>
  select(-strata_B) |>
  ungroup()

# for Fig. 6 Aspect difference
df_samples_B <- df_binned |>
  group_by(strata_B) |>
  sample_n(size = 1, replace = FALSE) |>
  mutate(
    xmin = lon - 0.25,
    xmax = lon + 0.25,
    ymin = lat - 0.25,
    ymax = lat + 0.25
  ) |>
  select(-strata_A) |>
  ungroup()

# Add description column
df_samples_A$strata_A_label <- c(
  "dry_flat",
  "mod_flat",
  "wet_flat",
  "dry_rugged",
  "mod_rugged",
  "wet_rugged"
)

df_samples_B$strata_B_label = c(
  "low_lat_flat",
  "mid_lat_flat",
  "high_lat_flat",
  "low_lat_rugged",
  "mid_lat_rugged",
  "high_lat_rugged"
)

# Save the two data frames as RDS files
saveRDS(df_samples_A, here::here("data/df_samples_A.rds"))
saveRDS(df_samples_B, here::here("data/df_samples_B.rds"))
