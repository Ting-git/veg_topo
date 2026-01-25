# ==============================================================================
# Script: plot_fraction_land_use_maps.R
#
# Purpose:
#   Visualize global fraction land cover layers and annual mean temperature (MAT)
#   using rasters produced from the processing pipeline.
#
# Output:
#   - High-resolution PNG maps of:
#       * Fraction of used land (fused)
#       * Bare land fraction (fbare)
#       * Water bodies fraction (fwater)
#       * Permanent snow and ice fraction (fsnow)
#       * Annual mean temperature (mat)
# ==============================================================================

# ------------------------- 1. Setup ---------------------------------------------
library(terra)
library(sf)
library(ggplot2)
library(rnaturalearth)
library(RColorBrewer)
library(tidyterra)
library(here)

# Load configuration and plotting helper
# Automatically select configuration file
hostname <- trimws(tolower(system("hostname", intern = TRUE)))
if (hostname == "dash") {
  message("💻 Detected Worksation: dash → using config.R")
  source(here::here("config.R"))
} else {
  message("🖥️ Detected HPC environment (", hostname, ") → using config_ubelix.R")
  source(here::here("config_ubelix.R"))
}

source(here::here("R/plot_var.R"))

message("🌍 Starting visualization of fraction land cover and MAT maps...")

# ------------------------- 2. Load Data ----------------------------------------
# Load coastlines and land polygons
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")
land  <- rnaturalearth::ne_countries(scale = 110, returnclass = "sf")
land_vect <- vect(land)

# Load raster outputs
input_files <- c(fused_5km_file, fbare_5km_file, fwater_5km_file, fsnow_5km_file, mat_5km_file)
rasters <- lapply(input_files, rast)
stacked <- rast(rasters)
names(stacked) <- c("fused", "fbare", "fwater", "fsnow", "mat")

# Mask to land extent
stacked_masked <- mask(crop(stacked, land_vect), land_vect)

# ------------------------- 3. Plot & Save --------------------------------------
plot_map <- function(r_layer, title_text, output_file) {
  p <- plot_var(
    input     = r_layer,
    title_text = title_text,
    extent    = ext_global,
    text_size = 12,
    x_step    = 30,
    y_step    = 30
  ) +
    guides(fill = guide_colorbar(
      title.position = "left",
      barwidth = grid::unit(0.2, "in"),
      barheight = grid::unit(5.3, "in")
    )) +
    geom_sf(data = coast,
            colour = 'black',
            linewidth = 0.1) +
    coord_sf(
      xlim = c(terra::xmin(ext_global), terra::xmax(ext_global)),
      ylim = c(terra::ymin(ext_global), terra::ymax(ext_global)),
      expand = FALSE,
      clip = "on"
    ) +
    ggplot2::theme(
      legend.margin = margin(0, 0, 0, 0),
      legend.box.margin = margin(0, 0, 0, -10),
      axis.title.x = ggplot2::element_blank(),
      axis.title.y = ggplot2::element_blank(),
      panel.spacing = unit(0, "pt"),
      plot.margin = margin(t = 0, r = 0, b = 0, l = 0, unit = "pt"),
      panel.background = element_rect(fill = "white", color = NA),  # panel 内部白色
      plot.background  = element_blank(),                            # plot 外部透明
      legend.background = element_blank(),
      legend.box.background = element_blank()
    )

  ggsave(
    filename = output_file,
    plot     = p,
    width    = 14,
    height   = 6,
    dpi      = 600,
    units    = "in"
  )

  if (file.exists(output_file)) message("✅ Saved map: ", output_file)
}

# Plot maps
plot_map(stacked_masked[["fused"]], "Fraction of used land", here::here("data/figures/03_fused_5km_map.png"))
plot_map(stacked_masked[["fbare"]], "Bare land fraction", here::here("data/figures/03_fbare_5km_map.png"))
plot_map(stacked_masked[["fwater"]], "Water body fraction", here::here("data/figures/03_fwater_5km_map.png"))
plot_map(stacked_masked[["fsnow"]], "Permanent snow and ice fraction", here::here("data/figures/03_fsnow_5km_map.png"))
plot_map(stacked_masked[["mat"]], "Annual mean air temperature (℃)", here::here("data/figures/03_mat_5km_map.png"))
