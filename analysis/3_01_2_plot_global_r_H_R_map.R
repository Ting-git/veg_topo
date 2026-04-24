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
# p_r <- plot_r_H_R(
#   input = r_H_R_5km_path,
#   extent = ext_global,
#   title_text = bquote("5-km Pearson's " * r[.("H")*","*.("Rᵢₙ")]),
#   text_size = 14,
#   x_step = 30,
#   y_step = 30,
#   land_color = "#EFECE4"
# ) +
#   guides(fill = guide_colorbar(
#     title.position = "left",
#     barwidth = grid::unit(0.2, "in"),
#     barheight = grid::unit(5.3, "in")
#   )) +
#   geom_sf(data = coast,
#           colour = 'black',
#           linewidth = 0.1) +
#   coord_sf(
#     xlim = c(terra::xmin(ext_global), terra::xmax(ext_global)),
#     ylim = c(terra::ymin(ext_global), terra::ymax(ext_global)),
#     expand = FALSE,
#     clip = "on"
#   ) +
#   ggplot2::theme(
#     legend.margin = margin(0, 0, 0, 0),
#     legend.box.margin = margin(0, 0, 0, -10),
#     axis.title.x = ggplot2::element_blank(),
#     axis.title.y = ggplot2::element_blank(),
#     panel.spacing = unit(0, "pt"),
#     plot.margin = margin(t = 0, r = 0, b = 0, l = 0, unit = "pt"),
#     panel.background = element_rect(fill = "white", color = NA),  # panel background white
#     plot.background  = element_blank(),                            # plot background transparent
#     legend.background = element_blank(),
#     legend.box.background = element_blank()
#   )


# 读取数据
r <- rast(r_H_R_5km_path)

# 直接绘制
p_r <- ggplot() +
  tidyterra::geom_spatraster(data = r, maxcell = Inf, interpolate = TRUE) +
  scale_fill_gradient2(
    low = "blue",
    mid = "white",
    high = "red",
    midpoint = 0,
    limits = c(-1, 1),
    na.value = "transparent",
    name = bquote("Pearson's " * r[.("H")*","*.("Rᵢₙ")])
  ) +
  geom_sf(data = coast, colour = 'black', linewidth = 0.1) +
  coord_sf(
    xlim = c(-180, 180),
    ylim = c(-60, 85),  # 限制在数据有效范围
    expand = FALSE
  ) +
  theme_minimal() +
  theme(
    legend.position = "right",
    panel.background = element_rect(fill = "white", color = NA),
    plot.background = element_blank()
  )


# save
ggsave(
  filename = file.path(project_root, "data/figures/3_01_r_H_R_5km_map.png"),
  plot = p_r,
  width = 14,
  height = 6,
  dpi = 600,
  units = "in",
  limitsize = FALSE)  # 允许超大文件

# --------- plot P value ----------------------------

p_pval <-  plot_cor_pval(
  input = pval_r_H_R_5km_path,
  extent = ext_global,
  title_text = "Pearson's p-value (H～Rᵢₙ)",
  text_size = 14,
  x_step = 30,
  y_step = 30
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
    panel.background = element_rect(fill = "white", color = NA),  # panel background white
    plot.background  = element_blank(),                            # plot background transparent
    legend.background = element_blank(),
    legend.box.background = element_blank()
  )

# save
ggsave(
  filename = file.path(project_root, "data/figures/3_01_r_H_R_5km_pval.png"),
  plot = p_pval,
  width = 14,
  height = 5.8,
  dpi = 600,
  units = "in"
)
