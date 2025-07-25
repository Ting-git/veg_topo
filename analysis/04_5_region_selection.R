


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


# ------------ region -----------------------------------------------

df_binned <- df |>

  # create bins
  mutate(
    ai_bin = cut(ai, breaks = quantile(ai, probs = seq(0, 1, 1/5)), include.lowest = TRUE,
                 labels = c("very_dry", "dry", "moderate", "wet", "very_wet")),
    relief_bin = cut(relief, breaks = quantile(relief, probs = seq(0, 1, 1/3)), include.lowest = TRUE,
                     labels = c("low_relief", "moderate_relief", "high_relief")),
    # lat_bin = cut(lat, breaks = quantile(lat, probs = seq(0, 1, 1/5)), include.lowest = TRUE)
    abs_lat = abs(lat),
    lat_bin = cut(abs_lat, breaks = quantile(abs_lat, probs = seq(0, 1, 1/5)), include.lowest = TRUE,
                  labels = c("equatorial", "low_latitude", "mid_latitude", "high_latitude", "polar"))
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
    lat_bin %in% levels(lat_bin)[c(1, 3, 5)]
  ) |>

  # Fig. 6 Down Valley drainage
  mutate(strata_A = interaction(ai_bin, relief_bin, drop = TRUE)) |>

  # Fig. 6 Aspect difference
  mutate(strata_B = interaction(lat_bin, relief_bin, drop = TRUE))

# check the bins and the combinations

table(df_binned$ai_bin)
table(df_binned$relief_bin)
table(df_binned$lat_bin)

table(df_binned$strata_A)
table(df_binned$strata_B)

# ---- sampling ----------------------------------------------------------------

# Pick one region (coarse gridcell) representing all combination of bins.
# Should yield 6 samples

set.seed(999)

# for Fig. 6 Down Valley drainage
df_samples_A <- df_binned |>
  group_by(strata_A) |>
  sample_n(size = 1, replace = FALSE) |>
  ungroup()

# for Fig. 6 Aspect difference
df_samples_B <- df_binned |>
  group_by(strata_B) |>
  sample_n(size = 1, replace = FALSE) |>
  ungroup()



