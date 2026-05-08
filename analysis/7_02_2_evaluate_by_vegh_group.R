# ============================================================================
# EVALUATE MODELS BY ECOLOGICAL VEGETATION HEIGHT GROUPS
# ============================================================================

library(dplyr)
library(ggplot2)
library(data.table)

source(here::here("R/config.R"))

# ============================================================================
# 1. LOAD MODELS AND EXTRACT PREDICTIONS
# ============================================================================

cat("\nLoading models...\n")

full_model <- readRDS(file.path(rf_models_dir, "rf_full_model_cv.rds"))
climate_model <- readRDS(file.path(rf_models_dir, "rf_climate_model_cv.rds"))
topo_model <- readRDS(file.path(rf_models_dir, "rf_topo_model_cv.rds"))

# 提取预测
full_pred <- as.data.table(full_model$pred)[, .(pred, obs, Resample)][, Model := "Full"]
climate_pred <- as.data.table(climate_model$pred)[, .(pred, obs, Resample)][, Model := "Climate"]
topo_pred <- as.data.table(topo_model$pred)[, .(pred, obs, Resample)][, Model := "Topo"]

all_pred <- rbindlist(list(full_pred, climate_pred, topo_pred))

rm(full_model, climate_model, topo_model)
gc()

# ============================================================================
# 2. ADD ECOLOGICAL GROUPS
# ============================================================================

# 基于森林生态学的植被高度分组
all_pred[, group := fcase(
  obs < 2, "0-2m (Shrub/Grass)",
  obs >= 2 & obs < 5, "2-5m (Low forest)",
  obs >= 5 & obs < 10, "5-10m (Medium forest)",
  obs >= 10 & obs < 15, "10-15m (Tall forest)",
  obs >= 15 & obs < 20, "15-20m (Very tall forest)",
  obs >= 20 & obs < 30, "20-30m (Emergent)",
  obs >= 30, ">30m (Canopy)",
  default = NA_character_
)]

# 设置因子顺序
group_order <- c("0-2m (Shrub/Grass)", "2-5m (Low forest)", "5-10m (Medium forest)",
                 "10-15m (Tall forest)", "15-20m (Very tall forest)",
                 "20-30m (Emergent)", ">30m (Canopy)")
all_pred[, group := factor(group, levels = group_order)]

# 显示各组样本量
cat("\nSample size by group:\n")
all_pred[, .(N = .N), by = .(Model, group)][order(Model, group)] %>% print()

# ============================================================================
# 3. CALCULATE METRICS
# ============================================================================

metrics <- all_pred[!is.na(group), .(
  n = .N,
  RMSE = sqrt(mean((pred - obs)^2)),
  R2 = cor(pred, obs)^2,
  Bias = mean(pred - obs),
  MAE = mean(abs(pred - obs))
), by = .(Model, group)]

# ============================================================================
# 4. OUTPUT RESULTS
# ============================================================================

cat("\n", paste(rep("=", 80), collapse = ""))
cat("\nPERFORMANCE BY ECOLOGICAL VEGETATION HEIGHT GROUPS")
cat("\n", paste(rep("=", 80), collapse = ""), "\n\n")

print(metrics[, .(Model, group, n, RMSE = round(RMSE, 2),
                  R2 = round(R2, 3), Bias = round(Bias, 2), MAE = round(MAE, 2))],
      row.names = FALSE)

# ============================================================================
# 5. VISUALIZATION
# ============================================================================

# RMSE
p1 <- ggplot(metrics, aes(x = group, y = RMSE, color = Model, group = Model)) +
  geom_line(size = 1.2) +
  geom_point(size = 3) +
  labs(title = "RMSE by Vegetation Height Class",
       x = NULL, y = "RMSE (m)") +
  theme_bw(base_size = 12) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "bottom")

# R²
p2 <- ggplot(metrics, aes(x = group, y = R2, color = Model, group = Model)) +
  geom_line(size = 1.2) +
  geom_point(size = 3) +
  labs(title = expression(R^2 ~ "by Vegetation Height Class"),
       x = NULL, y = expression(R^2)) +
  ylim(0, 1) +
  theme_bw(base_size = 12) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "bottom")

# Bias
p3 <- ggplot(metrics, aes(x = group, y = Bias, fill = Model)) +
  geom_bar(stat = "identity", position = position_dodge(0.8), width = 0.7) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "red", size = 1) +
  labs(title = "Prediction Bias by Height Class",
       x = NULL, y = "Bias (m)") +
  theme_bw(base_size = 12) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "bottom")

ggplot(metrics, aes(x = group, y = Bias, color = Model, group = Model)) +
  geom_line(size = 1.2) +
  geom_point(size = 3) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  labs(title = "Systematic Bias Across Height Classes",
       x = NULL, y = "Bias (m)") +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# 保存
ggsave(here::here("data/figures/7_03_rmse_by_ecological_group.png"),
       p1, width = 10, height = 6, dpi = 300)
ggsave(here::here("data/figures/7_03_r2_by_ecological_group.png"),
       p2, width = 10, height = 6, dpi = 300)
ggsave(here::here("data/figures/7_03_bias_by_ecological_group.png"),
       p3, width = 10, height = 6, dpi = 300)

# ============================================================================
# 6. SAVE RESULTS
# ============================================================================

fwrite(metrics, here::here("data/performance_by_ecological_group.csv"))

# ============================================================================
# 7. SUMMARY
# ============================================================================

cat("\n", paste(rep("=", 80), collapse = ""))
cat("\nKEY FINDINGS")
cat("\n", paste(rep("=", 80), collapse = ""), "\n\n")

for(m in c("Full", "Climate", "Topo")) {
  temp <- metrics[Model == m]
  best <- temp[which.min(RMSE)]
  worst <- temp[which.max(RMSE)]

  cat(sprintf("\n%s Model:", m))
  cat(sprintf("\n  Best:  %-20s (RMSE=%.2fm, R²=%.3f, n=%d)",
              best$group, best$RMSE, best$R2, best$n))
  cat(sprintf("\n  Worst: %-20s (RMSE=%.2fm, R²=%.3f, n=%d)",
              worst$group, worst$RMSE, worst$R2, worst$n))
  cat(sprintf("\n  Worst/Best RMSE ratio: %.2f", worst$RMSE / best$RMSE))
}

cat("\n\n✅ Complete!\n")
