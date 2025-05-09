library(terra)
library(sf)
library(dplyr)
library(purrr)
library(tidyr)
library(ggplot2)
library(scico)
library(ggpointdensity)
library(ncdf4)
library(cowplot)
library(furrr)
library(progressr)
library(cli)
library(knitr)
library(ggmap)
library(here)
library(segmented)

source(here::here("R/split_window_correlation.R"))

file_vegh_450m_mosaic <- "/data_2/scratch/ting/data/vegh_450m/vegh_450m_2020_mosaic.nc"
file_ga2 <- "/data/archive/gti_marthews_2015/data/ga2.nc"  # Target raster file path
file_correlation_nc <- "/data_2/scratch/ting/data/correlation_analysis.nc"  # Output file
output_dir <- "/data_2/scratch/ting/data_temp"
file_modis_landcover <- "/data/scratch/bstocker/landcover/modis_landcover__LPDAAC__v5.1__0.05deg__2010.nc"

ext_valids <- list(
  "desert_riparian" = ext(-112, -109, 28, 31),
  "mediterranean" = ext(-122.5, -119.5, 35, 38),
  "forest_savanna" = ext(28, 30, -6.5, -4.5),
  "waterlogged_pantanal" = ext(-58.5, -56, -19, -16),
  "white_sand" = ext(-68.5, -66.5, 1, 3)
)
region_names <- names(ext_valids)

merged_files <- preprocess_all_regions(
  ext_list = ext_valids,
  twi_file = file_ga2,
  vegh_file = file_vegh_450m_mosaic,
  output_dir = output_dir
)
# ------------------------------------------------------------------------------
raster_file = merged_files[[1]]
region_ext = ext_valids[[1]]
region_name = region_names[1]

merged_raster <- suppressWarnings(rast(raster_file))
names(merged_raster) <- c("twi", "vegh")
res(merged_raster)

# Process and analyze
windowed_data <- create_spatial_windows(merged_raster, 12)
correlation_df <- calculate_window_correlations(windowed_data)

summary(windowed_data)


correlation_df_peak <- calculate_window_correlations2(windowed_data)
correlation_df_peak %>%
  count(peak)

# Debug output: NA count
message(region_name, " NA count:")
print(colSums(is.na(correlation_df)))

# Generate plots
plot1 <- plot_twi(windowed_data)
plot2 <- plot_vegh(windowed_data)

plot3 <- plot_corr(correlation_df)
plot3

plot4 <- plot_correlation_vs_pixel_count(correlation_df)
# plot5 <- plot_img(region_ext)
plot5 <- plot_peak(correlation_df_peak)
plot6 <- plot_landcover(file_modis_landcover, region_ext)
plots <- plot_random_windows(correlation_df, seed = 123)
plot9 <- plot_overview(windowed_data)


all_plots <- c(list(plot1, plot2, plot3, plot4, plot5, plot6), plots, list(plot9))

combined_plot <- plot_grid(
  plotlist = all_plots,
  ncol = 3,
  align = "hv"
) + theme(plot.background = element_rect(fill = "white", color = "white"))

# Save output plot
output_file <- here("data",
                    "figures",
                    paste0("02_2_combined_plot_", region_name, ".png"))

ggsave(
  output_file,
  combined_plot,
  width = 20,
  height = 13,
  dpi = 300,
  bg = "white"
)




