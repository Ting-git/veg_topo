# ---------- SetUp -------------------------------------------------------------
library(terra)
library(ggplot2)
library(tidyterra)
library(scico)
library(rnaturalearth)
library(sf)

source(here::here("R/config.R"))
source(here::here("R/plot_r_H_R.R"))
source(here::here("R/plot_cor_pval.R"))

# ---------- data pre ----------------------------------------------------------
# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# ------- Plot global correlation analysis of TWI and VEGH ---------------------
r_H_Rin <- rast(r_H_R_5km_path) |>
  aggregate(fact = c(2,2))

p_r <- plot_r_H_R(
  input = r_H_Rin,
  extent = ext_global,
  # title_text = bquote("5-km Pearson's " * r[.("H")*","*.("Rᵢₙ")]),
  title_text = "",
  text_size = 7,
  x_step = 30,
  y_step = 30
) +
  guides(fill = guide_colorbar(
    title.position = "left",
    barwidth = grid::unit(0.1, "in"),
    barheight = grid::unit(2.7, "in")
  )) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1) +
  ggplot2::theme(
    legend.margin = margin(0, 0, 0, 0),
    legend.box.margin = margin(0, 0, 0, -10),
    axis.title.x = ggplot2::element_blank(),
    axis.title.y = ggplot2::element_blank(),
    panel.spacing = unit(0, "pt"),
    plot.margin = margin(t = 0, r = 0, b = 0, l = 0, unit = "pt"),
    panel.background = element_rect(fill = "white", color = NA),  # panel background white
    plot.background  = element_blank(),                            # plot background transparent
    legend.background = element_blank(),
    legend.box.background = element_blank()
  )

# save
ggsave(
  filename = file.path(project_root, "data/figures/3_01_r_H_R_5km_map_0.1d.png"),
  plot = p_r,
  width = 7,
  height = 3,
  dpi = 600,
  units = "in",
  limitsize = FALSE)  # 允许超大文件

# --------- plot P value ----------------------------
p_H_Rin <- rast(pval_r_H_R_5km_path) |>
  aggregate(fact = c(2,2))

p_pval <-  plot_cor_pval(
  input = p_H_Rin,
  extent = ext_global,
  title_text = "",
  # title_text = "Pearson's p-value (H～Rᵢₙ)",
  text_size = 7,
  x_step = 30,
  y_step = 30
) +
  guides(fill = guide_colorbar(
    title.position = "left",
    barwidth = grid::unit(0.1, "in"),
    barheight = grid::unit(2.7, "in")
  )) +
  geom_sf(data = coast,
          colour = 'black',
          linewidth = 0.1) +
  ggplot2::theme(
    legend.margin = margin(0, 0, 0, 0),
    legend.box.margin = margin(0, 0, 0, -10),
    axis.title.x = ggplot2::element_blank(),
    axis.title.y = ggplot2::element_blank(),
    panel.spacing = unit(0, "pt"),
    plot.margin = margin(t = 0, r = 0, b = 0, l = 0, unit = "pt"),
    panel.background = element_rect(fill = "white", color = NA),  # panel background white
    plot.background  = element_blank(),                            # plot background transparent
    legend.background = element_blank(),
    legend.box.background = element_blank()
  )

# save
ggsave(
  filename = file.path(project_root, "data/figures/3_01_r_H_R_5km_pval_0.1d.png"),
  plot = p_pval,
  width = 7,
  height = 3,
  dpi = 600,
  units = "in"
)
