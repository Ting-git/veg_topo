
library(terra)
library(dplyr) # need in calculate_correlation_bywin()
library(sf) # need in get_lonlat_extent()

library(ggplot2)
library(patchwork)

source(here::here("R/config.R"))
source(here::here("R/get_lonlat_extent.R"))  # need in create_aligned_template()
source(here::here("R/create_aligned_template.R"))
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/calculate_correlation_bywin.R"))
source(here::here("R/raster_preprocess_save.R"))
source(here::here("R/aggregate_topography.R"))
source(here::here("R/extent_to_tile_ids.R"))

source(here::here("R/plot_hex_scatter.R"))
source(here::here("R/plot_cor_pval.R"))
source(here::here("R/plot_cor_twi_vegh.R"))

lidar <- rast(file.path(lidar_asl_dir, "32TMS.tif"))
lidar <- clamp(lidar, lower = 0, upper = 70, values = FALSE)

align_r <- create_aligned_template(lidar, res_out = 0.00025)
align_r

vegh_lidar <- project(lidar, align_r, method = "average")
vegh_lidar

reg_extent <- ext(align_r)
twi_rc <- terra::rast(twi_30m_path) |> terra::crop(reg_extent)/100
twi_rc

vegh_lang <- extent_to_tile_ids(reg_extent, tile_size = 3, return_raster = TRUE,
                              source = "lang_vegh_10m", tiles_dir = vegh_10m_tiles_dir)
# Set 0 as NA value (0m canopy height represents not vegetated or water according to Lang et al. (2019))
# Aggregate and resample using TWI data from Ho et al. (2025)
vegh_lang <- raster_preprocess_save(
  input = vegh_lang,
  target = vegh_lidar,
  na_value = 0,
  fun = mean,
  varname = "vegh",
  if_aggregate = TRUE,
  if_resample = TRUE,
  if_mask = TRUE,
  if_return_raster = TRUE
)

plot(vegh_lidar)
plot(vegh_lang)


stacked_lidar <- c(twi_rc, vegh_lidar)

stacked_lang <- c(twi_rc, vegh_lang, vegh_lidar)
df_win_lang <- create_spatial_windows(stacked_lang, value_vars = c("twi", "vegh_lang", "vegh_lidar"), dwin = 0.005)
df_cor_lang <- calculate_correlation_bywin(df_win_lang, x = "twi", y = "vegh_lang")
df_cor_lidar <- calculate_correlation_bywin(df_win_lidar, x = "twi", y = "vegh_lidar")

df_combined <- merge(df_cor_lang, df_cor_lidar,
                     by = c("lon_mid", "lat_mid"),
                     suffixes = c("_lang", "_lidar"))
colnames(df_combined)

summary_cor <- data.frame(
  Variable = c("correlation_lang", "correlation_lidar"),
  Mean = c(mean(df_combined$correlation_lang, na.rm = TRUE),
           mean(df_combined$correlation_lidar, na.rm = TRUE)),
  SD = c(sd(df_combined$correlation_lang, na.rm = TRUE),
         sd(df_combined$correlation_lidar, na.rm = TRUE)),
  Min = c(min(df_combined$correlation_lang, na.rm = TRUE),
          min(df_combined$correlation_lidar, na.rm = TRUE)),
  Max = c(max(df_combined$correlation_lang, na.rm = TRUE),
          max(df_combined$correlation_lidar, na.rm = TRUE))
)
print(summary_cor)

df_combined$cor_diff <- df_combined$correlation_lang - df_combined$correlation_lidar
df_combined$abs_diff <- abs(df_combined$cor_diff)

# 两个相关性的相关性
cor_between <- cor(df_combined$correlation_lang,
                   df_combined$correlation_lidar,
                   use = "complete.obs")
cat("两个相关性的相关系数:", cor_between, "\n")

# 散点图
plot(df_combined$correlation_lang, df_combined$correlation_lidar,
     xlab = "Correlation (Lang)",
     ylab = "Correlation (Lidar)",
     main = paste("Comparison of Correlations (r =", round(cor_between, 3), ")"),
     pch = 19, cex = 0.5, col = rgb(0, 0, 1, 0.3))
abline(0, 1, col = "red", lwd = 2)  # 1:1 线
abline(lm(correlation_lidar ~ correlation_lang, data = df_combined),
       col = "black", lwd = 2)
legend("topleft", legend = c("1:1 line", "Regression line"),
       col = c("red", "black"), lwd = 2)


# 差异直方图
hist(df_combined$cor_diff,
     breaks = 50,
     main = "Distribution of Correlation Differences (Lang - Lidar)",
     xlab = "Difference",
     col = "lightblue",
     border = "black")
abline(v = 0, col = "red", lwd = 2, lty = 2)

# 添加统计线
abline(v = mean(df_combined$cor_diff, na.rm = TRUE),
       col = "blue", lwd = 2)
legend("topright", legend = c("Zero line", "Mean difference"),
       col = c("red", "blue"), lwd = 2)

# 差异箱线图
boxplot(df_combined$correlation_lang, df_combined$correlation_lidar,
        names = c("Lang", "Lidar"),
        main = "Comparison of Correlations",
        ylab = "Correlation",
        col = c("lightgreen", "lightcoral"))

p1 <- ggplot(df_combined, aes(x = correlation_lang, y = correlation_lidar)) +
  geom_hex(bins = 30) +  # bins 控制六边形数量
  scale_fill_viridis_c(name = "Count", trans = "log") +  # 使用对数变换更好显示密度
  geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed", size = 1) +
  geom_smooth(method = "lm", se = TRUE, color = "darkblue", size = 1) +
  labs(title = "Correlation Comparison",
       x = "Lang Correlation",
       y = "Lidar Correlation") +
  theme_minimal() +
  theme(legend.position = "right")

p2 <- ggplot(df_combined, aes(x = cor_diff)) +
  geom_histogram(bins = 50, fill = "lightblue", color = "black", alpha = 0.7) +
  geom_vline(xintercept = 0, color = "red", linetype = "dashed", size = 1) +
  labs(title = "Difference Distribution", x = "Difference (Lang - Lidar)") +
  theme_minimal()

p3 <- ggplot(df_combined, aes(y = correlation_lang, x = "Lang")) +
  geom_boxplot(fill = "lightgreen") +
  geom_boxplot(aes(y = correlation_lidar, x = "Lidar"), fill = "lightcoral") +
  labs(title = "Boxplot Comparison", y = "Correlation") +
  theme_minimal()

# 合并图形

(p1) / (p2 | p3) + plot_layout(heights = c(2, 1))  +
  plot_annotation(title = "Figure: Correlation Analysis",
                  theme = theme(plot.title = element_text(hjust = 0.5)))

cor_lang <- terra::rast(df_combined[, c("lon_mid", "lat_mid", "correlation_lang")], type="xyz", crs="EPSG:4326")
cor_lidar <- terra::rast(df_combined[, c("lon_mid", "lat_mid", "correlation_lidar")], type="xyz", crs="EPSG:4326")

text_size = 14
x_step = 0.5
y_step = 0.5

p_rA_lang <- plot_cor_twi_vegh(cor_lang, extent = reg_extent,  title_text <- bquote("   500-m Pearson's " * r[.("H")*","*.("TWI")]), text_size = text_size, x_step = x_step, y_step = y_step) +
  labs(tag = "f)") +
  theme(
    plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
    plot.tag.position = c(0.06, 1)
  )
p_rA_lang

p_rA_lidar <- plot_cor_twi_vegh(cor_lidar, extent = reg_extent,  title_text <- bquote("   500-m Pearson's " * r[.("H")*","*.("TWI")]), text_size = text_size, x_step = x_step, y_step = y_step) +
  labs(tag = "f)") +
  theme(
    plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
    plot.tag.position = c(0.06, 1)
  )
p_rA_lidar


p_H_TWI_30 <- plot_hex_scatter(df_win_lidar, x_var="twi",y_var = "vegh", x_text = "Topographic wetness index", y_text = "Vegetation height (m)", text_size = text_size, title_text="TWI vs H at 30 m")
p_H_TWI_30

p_google <- plot_google_img(extent = reg_extent, title_text = "   Google Satellite Map", text_size = text_size, x_step = x_step, y_step = y_step) +
  labs(tag = "b)") +
  theme(
    plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
    plot.tag.position = c(0.06, 1)) + re_theme0 +  ggplot2::theme(plot.margin = margin(t = 0, r = 0, b = 4, l = 0))

p_dem <- plot_dem(file.path(output_dir, paste0("reg_", reg_id, "_dem_30m.nc")), extent = reg_extent, title_text = "   30-m Elevation", text_size = text_size, x_step = x_step, y_step = y_step) +
  labs(tag = "c)") +
  theme(
    plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
    plot.tag.position = c(0.12, 1)
  ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 0, r = 20, b = 4, l = 0))

p_vegh <- plot_vegh(file.path(output_dir, paste0("reg_", reg_id, "_vegh_30m.nc")), extent = reg_extent, title_text = expression("   30-m " * italic(H)), text_size = text_size, x_step = x_step, y_step = y_step) +
  labs(tag = "d)") +
  theme(
    plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
    plot.tag.position = c(0.06, 1)
  ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 0, r = 0, b = 4, l = 0))

p_twi <- plot_twi(file.path(output_dir, paste0("reg_", reg_id, "_twi_30m.nc")), extent = reg_extent, title_text = "   30-m TWI", text_size = text_size, x_step = x_step, y_step = y_step) +
  labs(tag = "e)") +
  theme(
    plot.tag = element_text(size = 14, face = "bold", hjust = 0, vjust = 1),
    plot.tag.position = c(0.13, 1)
  ) + re_theme +  ggplot2::theme(plot.margin = margin(t = 4, r = 20, b = 4, l = 0))



