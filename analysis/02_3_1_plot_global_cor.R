# ---------- SetUp -------------------------------------------------------------
library(terra)
library(ggplot2)
library(tidyterra)
library(scico)
library(rnaturalearth)
library(sf)

source(here::here("config.R"))

# ---------- data pre ----------------------------------------------------------
# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# load correlation data, raster data
cor_r <- terra::rast(cor_twi_vegh_mosaic_file)

# ------- Plot global correlation analysis of TWI and VEGH ---------------------

p_cor <- ggplot() +
  tidyterra::geom_spatraster(data = cor_r[[1]], maxcell = Inf) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1) +
  scale_fill_scico(
    palette = "bam",
    direction = 1,
    na.value = NA,
    name = "r",
    guide = guide_colorbar(
      title.position = "left",
      label.position = "bottom",
      barwidth = unit(12, "cm"),
      barheight = unit(0.5, "cm")
    )
  ) +
  labs(
    title = "VEGH-TWI Pearson Correlation"
  ) +
  scale_x_continuous(
    expand = c(0, 0),
    breaks = seq(-180, 180, by = 30)
  ) +
  scale_y_continuous(
    expand = c(0, 0),
    limits = c(-60, 85),
    breaks = seq(-60, 90, by = 30)
  ) +
  theme(
    plot.title = element_text(
      size = 24,
      face = "bold",
      hjust = 0,
      margin = margin(b = 5)
    ),
    plot.title.position = "panel",
    axis.title = element_text(size = 18),
    axis.text = element_text(size = 14),

    legend.position = "bottom",
    legend.box = "horizontal",

    legend.text = element_text(size = 14),
    legend.title = element_text(
      size = 16,
      face = "bold",
      margin = margin(r = 10)
    ),
  )


# save
ggsave(
  filename = file.path(project_root, "data/figures/02_3_1_cor_twi_vegh_map.png"),
  plot = p_cor,
  width = 24,
  height = 11.5,
  dpi = 300,
  units = "in"
)


# --------- plot P value ----------------------------

p_pval <- ggplot() +
  tidyterra::geom_spatraster(data = cor_r[[2]], maxcell = Inf) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1) +
  scale_fill_stepsn(
    colours = c("#2166AC", "#67A9CF", "#D1E5F0", "#FDDBC7", "#EF8A62", "#B2182B"),
    values = scales::rescale(c(0, 0.01, 0.05, 0.1, 0.5, 1)),  # 分段值（rescaled to 0–1）
    breaks = c(0.01, 0.05, 0.1, 0.5, 1),
    limits = c(0, 1),
    oob = scales::squish,
    name = "p",
    na.value = NA,
    guide = guide_colorbar(
      title.position = "left",
      label.position = "bottom",
      barwidth = unit(12, "cm"),
      barheight = unit(0.5, "cm")
    )
  ) +
  labs(
    title = "VEGH–TWI Pearson Correlation: P-value Map"
  ) +
  scale_x_continuous(
    expand = c(0, 0),
    breaks = seq(-180, 180, by = 30)
  ) +
  scale_y_continuous(
    expand = c(0, 0),
    limits = c(-60, 85),
    breaks = seq(-60, 90, by = 30)
  ) +
  theme(
    plot.title = element_text(
      size = 24,
      face = "bold",
      hjust = 0,
      margin = margin(b = 5)
    ),
    plot.title.position = "panel",
    axis.title = element_text(size = 18),
    axis.text = element_text(size = 14),

    legend.position = "bottom",
    legend.box = "horizontal",

    legend.text = element_text(size = 14),
    legend.title = element_text(
      size = 16,
      face = "bold",
      margin = margin(r = 10)
    ),
  )


# save
ggsave(
  filename = file.path(project_root, "data/figures/02_3_1_cor_twi_vegh_p_val_map.png"),
  plot = p_pval,
  width = 24,
  height = 11.5,
  dpi = 300,
  units = "in"
)
