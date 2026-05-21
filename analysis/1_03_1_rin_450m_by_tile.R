# ============================================================================
# 1. LIBRARIES AND SOURCES
# ============================================================================
library(terra)
library(dplyr)
library(tidyr)
library(fs)
library(stringr)
library(parallel)
library(future)
library(furrr)
library(meteoland)
source(here::here("R/config.R"))
source(here::here("R/raster_preprocess_save.R"))
source(here::here("R/aggregate_topography.R"))
source(here::here("R/df_to_raster.R"))
source(here::here("R/create_aligned_template.R"))
source(here::here("R/cacl_meteoland_sw_in.R"))


# ============================================================================
# 2. PARALLEL CONFIGURATION (by hostname)
# ============================================================================
if (hostname == "dash") workers <- 10 else workers <- 100
message("→ using ", workers, " workers")


# ============================================================================
# 3. TILE PROCESSING CONFIGURATION
# ============================================================================
# Total tiles: 19426

# Test mode (recommended for first run)
start_idx <- 11
end_idx   <- 20

# Full run (uncomment when ready)
# start_idx <- 1
# end_idx   <- 19426

message(sprintf("Processing tiles %d to %d out of 19426 total", start_idx, end_idx))


# ============================================================================
# 4. DIRECTORY CONFIGURATION
# ============================================================================
if (!dir.exists(COP30_dir)) stop(paste("Input directory does not exist:", COP30_dir))

if (!dir.exists(rin_450m_tiles_dir)) dir.create(rin_450m_tiles_dir, recursive = TRUE)
if (!dir.exists(dem_450m_tiles_dir)) dir.create(dem_450m_tiles_dir, recursive = TRUE)


# ============================================================================
# 5. HELPER FUNCTIONS
# ============================================================================

# 5.1 Coordinate conversion
convert_lat <- function(x) {
  if (is.numeric(x)) {
    return(paste0(ifelse(x < 0, "S", "N"), sprintf("%02d", abs(x))))
  } else {
    return(ifelse(substr(x, 1, 1) == "S", -1, 1) * as.numeric(substring(x, 2)))
  }
}

convert_lon <- function(x) {
  if (is.numeric(x)) {
    return(paste0(ifelse(x < 0, "W", "E"), sprintf("%03d", abs(x))))
  } else {
    return(ifelse(substr(x, 1, 1) == "W", -1, 1) * as.numeric(substring(x, 2)))
  }
}

# 5.2 DEM file path
get_dem_path <- function(lat, lon, file_dir = "") {
  file.path(COP30_dir, sprintf("Copernicus_DSM_10_%s_00_%s_00_DEM.tif",
                               convert_lat(lat), convert_lon(lon)))
}

# 5.3 Merge 3x3 neighboring DEM tiles
merge_dem_neighbors <- function(lat, lon, file_dir = "") {
  dem_file <- get_dem_path(lat, lon, file_dir)

  if(!file.exists(dem_file)) return(NULL)
  dem <- rast(dem_file)

  n_rows <- nrow(dem)
  n_cols <- ncol(dem)

  # Latitudes where neighbor tiles have different longitude resolution!!!!!!
  no_south <- c(-50, -60, -70, -75, -80, -85, 50, 60, 70, 75, 80, 85)
  no_north <- c(-51, -61, -71, -76, -81, -86, 49, 59, 69, 74, 79, 84)

  get_neighbor <- function(dlat, dlon, rows, cols, nrow, ncol) {
    if((dlat == -1 && lat %in% no_south) || (dlat == 1 && lat %in% no_north)) {
      return(matrix(NA_real_, nrow, ncol))
    }
    dem_file <- get_dem_path(lat + dlat, lon + dlon, file_dir)
    if(file.exists(dem_file)) {
      subset <- rast(dem_file)[rows, cols]
      subset <- as.matrix(subset)
      return(matrix(as.numeric(subset), nrow, ncol, byrow=TRUE))
    } else {
      return(matrix(NA_real_, nrow, ncol))
    }
  }

  # Center DEM
  c_matrix <- as.matrix(dem, wide=TRUE)

  # Read all 8 neighbors
  nw <- get_neighbor(1, -1, (n_rows-2):n_rows, (n_cols-2):n_cols, 3, 3)
  n  <- get_neighbor(1, 0,  (n_rows-2):n_rows, 1:n_cols, 3, n_cols)
  ne <- get_neighbor(1, 1,  (n_rows-2):n_rows, 1:3, 3, 3)
  w  <- get_neighbor(0, -1, 1:n_rows, (n_cols-2):n_cols, n_rows, 3)
  e  <- get_neighbor(0, 1,  1:n_rows, 1:3, n_rows, 3)
  sw <- get_neighbor(-1, -1, 1:3, (n_cols-2):n_cols, 3, 3)
  s  <- get_neighbor(-1, 0,  1:3, 1:n_cols, 3, n_cols)
  se <- get_neighbor(-1, 1,  1:3, 1:3, 3, 3)

  # Combine into 3x3 block
  full_matrix <- rbind(
    cbind(nw, n, ne),
    cbind(w,  c_matrix, e),
    cbind(sw, s, se)
  )

  full_raster <- rast(full_matrix)
  ext(full_raster) <- c(
    ext(dem)$xmin - 3 * xres(dem),
    ext(dem)$xmax + 3 * xres(dem),
    ext(dem)$ymin - 3 * yres(dem),
    ext(dem)$ymax + 3 * yres(dem)
  )
  crs(full_raster) <- crs(dem)

  return(full_raster)
}


# ============================================================================
# 6. MAIN PROCESSING FUNCTION (per tile)
# ============================================================================
cal_rin_by_tile <- function(lat, lon) {

  # Output file paths
  rin_out_file <- file.path(rin_450m_tiles_dir,
                            paste0("radiation_index_", convert_lat(lat), "_", convert_lon(lon), "_15_arcscd.tif"))
  dem_out_file <- file.path(dem_450m_tiles_dir,
                            paste0("dem_", convert_lat(lat), "_", convert_lon(lon), "_15_arcscd.tif"))

  # Skip if already processed
  if (file.exists(rin_out_file) && file.exists(dem_out_file)) return(TRUE)
  tryCatch({

    # 6.1 Create alignment grid (30m resolution)
    align_30m <- create_aligned_template(ext(lon, lon + 1, lat, lat + 1), res_out = 1/3600)

    # 6.2 Load DEM with neighbors
    dem_ex <- merge_dem_neighbors(lat, lon, file_dir = COP30_dir)
    if (is.null(dem_ex)) stop("Failed to load DEM data for this tile")

    # 6.3 Compute slope (450m resolution)
    slope_450m <- terrain(dem_ex, v = "slope", unit = "degrees") |>
      resample(align_30m, method = "bilinear") |>
      aggregate(c(15, 15))

    # 6.4 Compute aspect (450m resolution, circular mean)
    aspect <- terrain(dem_ex, v = "aspect", unit = "radians")
    aspect_cos <- cos(aspect) |>
      resample(align_30m, method = "bilinear") |>
      aggregate(c(15, 15))
    aspect_sin <- sin(aspect) |>
      resample(align_30m, method = "bilinear") |>
      aggregate(c(15, 15))
    aspect_450m <- (atan2(aspect_sin, aspect_cos) * 180 / pi) %% 360

    # 6.5 Save DEM
    dem_450m <- suppressMessages(
      raster_preprocess_save(dem_ex, output = dem_out_file, target = slope_450m,
                             if_aggregate = FALSE, if_resample = TRUE)
    )

    # 6.6 Extract topography to dataframe
    df_topo <- as.data.frame(slope_450m, xy = TRUE) |>
      left_join(as.data.frame(aspect_450m, xy = TRUE), by = c("x", "y")) |>
      tidyr::drop_na()
    colnames(df_topo) <- c("lon", "lat", "slope", "aspect")

    # 6.7 Radiation calculation
    # Slope surface radiation
    sw_meteoland_surf_vec <- cacl_meteoland_sw_in(df_topo$lat, df_topo$slope, df_topo$aspect, 2020)

    # Flat surface radiation (cached by latitude)
    sw_meteoland_flat_vec <- unlist(ave(df_topo$lat, df_topo$lat,
                                        FUN = function(x) cacl_meteoland_sw_in(x[1], 0, 0, 2020)))

    # Radiation index
    rin_meteoland_vec <- sw_meteoland_surf_vec / sw_meteoland_flat_vec

    # 6.8 Save result as raster
    df_calc <- df_topo[, c("lon", "lat")] |>
      mutate(rin = rin_meteoland_vec)

    rin_450m <- suppressMessages(
      df_to_raster(df_calc, "lon", "lat", "rin", slope_450m, output_file = rin_out_file)
    )

    return(TRUE)

  }, error = function(e) {
    message(sprintf("❌ Tile (%s, %s) failed: %s", convert_lat(lat), convert_lon(lon), e$message))
    return(FALSE)
  })
}


# ============================================================================
# 7. LOAD DEM FILE INDEX
# ============================================================================
dem_files_all <- fs::dir_ls(
  path = COP30_dir,
  glob = "*_DEM.tif",
  recurse = TRUE
)
message(sprintf("Found %d DEM tiles", length(dem_files_all)))

# Extract lat/lon from filenames
lat_str <- substring(str_extract(dem_files_all, "_[NS]\\d{2}"), 2)
lon_str <- substring(str_extract(dem_files_all, "_[EW]\\d{3}"), 2)
lats <- unname(sapply(lat_str, convert_lat))
lons <- unname(sapply(lon_str, convert_lon))

# Filter high-latitude tiles
keep_idx <- lats >= -56 & lats <= 86
dem_files_all <- dem_files_all[keep_idx]
lats <- lats[keep_idx]
lons <- lons[keep_idx]

message(sprintf("After filter, found %d DEM tiles", length(dem_files_all)))


# ============================================================================
# 8. PARALLEL EXECUTION
# ============================================================================
plan(cluster, workers = workers)

# Get tile indices
tile_indices <- start_idx:end_idx
n_tiles <- length(tile_indices)

message("========================================")
message(sprintf("HPC Mode (Silent):"))
message(sprintf("  Total tiles: %d", n_tiles))
message(sprintf("  Workers: %d", workers))
message("========================================")

tictoc::tic("🚀 HPC Parallel processing")

results <- future_map(tile_indices, function(idx) {
  cal_rin_by_tile(lats[idx], lons[idx])
}, .progress = FALSE, .options = furrr_options(seed = TRUE))

plan(sequential)
tictoc::toc()

# ============================================================================
# 9. RESULTS SUMMARY
# ============================================================================
all_results <- unlist(results)
success_count <- sum(all_results, na.rm = TRUE)
fail_count <- length(all_results) - success_count

message("========================================")
message(sprintf("✅ %d succeeded, ❌ %d failed (%.1f%%)",
                success_count, fail_count, success_count / n_tiles * 100))
message("========================================")


# # ---------- 选择一个测试瓦片 ----------------
# id_test <- 22
# lat <- lats[[id_test]]
# lon <- lons[[id_test]]
#
# cal_rin_by_tile(lat, lon)
#
# plot(rast("/data_2/scratch/ting/veg_topo_data/data/global_dem_slope_aspect_450m/dem_1_1_deg/dem_N00_E029_15_arcscd.tif"))
# plot(rast("/data_2/scratch/ting/veg_topo_data/data/global_rin_450m/1_1_deg/radiation_index_N00_E029_15_arcscd.tif"))
#
# # ---- 单片测试参数设置 ----
# # 选择一个测试瓦片
# id_test <- 11111
# lat <- lats[[id_test]]
# lon <- lons[[id_test]]
#
# # 输出文件路径
# rin_out_file <- file.path(rin_450m_tiles_dir,
#                           paste0("radiation_index_", convert_lat(lat), "_", convert_lon(lon), "_15_arcscd.tif"))
# dem_out_file <- file.path(dem_450m_tiles_dir,
#                           paste0("dem_", convert_lat(lat), "_", convert_lon(lon), "_15_arcscd.tif"))
#
# # 创建对齐网格
# align_30m <- create_aligned_template(ext(lon, lon + 1, lat, lat + 1), res_out = 1/3600)
#
# # ---- 处理 DEM ----
# # 合并相邻 DEM 瓦片
# dem_ex <- merge_dem_neighbors(lat, lon, file_dir = COP30_dir)
#
# # 检查 DEM 是否有效
# if (is.null(dem_ex)) {
#   stop("Failed to load DEM data for this tile")
# }
# dem_ex
# # 计算坡度和坡向
# slope_450m <- terrain(dem_ex, v = "slope", unit = "degrees") |> resample(align_30m, method = "bilinear") |> aggregate(c(15, 15))
#
# aspect <- terrain(dem_ex, v = "aspect", unit = "radians")
# aspect_cos <- cos(aspect) |> resample(align_30m, method = "bilinear") |> aggregate(c(15, 15))
# aspect_sin <- sin(aspect) |> resample(align_30m, method = "bilinear") |> aggregate(c(15, 15))
# aspect_450m <- (atan2(aspect_sin, aspect_cos) * 180 / pi) %% 360
#
# # 保存 DEM
# dem_450m <- raster_preprocess_save(dem_ex, output = dem_out_file, target = slope_450m,
#                                     if_aggregate = FALSE, if_resample = TRUE)
# # ---- 准备数据框 ----
# df_topo <- as.data.frame(slope_450m, xy = TRUE) |>
#   left_join(as.data.frame(aspect_450m, xy = TRUE), by = c("x", "y")) |>
#   tidyr::drop_na()
# colnames(df_topo) <- c("lon", "lat", "slope", "aspect")
#
# message(sprintf("Number of valid pixels: %d", nrow(df_topo)))
#
# # ---- 计算辐射 ----
# tictoc::tic("Surface radiation calculation: FOR")
# sw_meteoland_surf_vec <- cacl_meteoland_sw_in(df_topo$lat, df_topo$slope, df_topo$aspect, 2020)
# sw_meteoland_flat_vec <- unlist(ave(df_topo$lat, df_topo$lat,
#                                     FUN = function(x) cacl_meteoland_sw_in(x[1], 0, 0, 2020)))
# rin_meteoland_vec <- sw_meteoland_surf_vec / sw_meteoland_flat_vec
# tictoc::toc()
#
# # 检查是否有异常值
# message(sprintf("Radiation index range: [%.3f, %.3f]",
#                 min(rin_meteoland_vec, na.rm = TRUE),
#                 max(rin_meteoland_vec, na.rm = TRUE)))
#
# # ---- 保存结果 ----
# df_calc <- df_topo[, c("lon", "lat")] |>
#   mutate(rin = rin_meteoland_vec)
#
# rin_450m <- df_to_raster(df_calc, "lon", "lat", "rin", slope_450m, output_file = rin_out_file)
#
# # ------PLOT AND CHECK ---------------------------------------------------------
# # DEM, Slope and Aspect range
# dem_range <- round(range(values(dem_450m), na.rm = TRUE), 2)
# slope_range <- round(range(values(slope_450m), na.rm = TRUE), 2)
# aspect_range <- c(0, 360)  # 方面范围固定
#
# rin_range <- round(range(values(rin_450m), na.rm = TRUE), 4)
# max_abs <- max(abs(rin_range - 1), na.rm = TRUE)
# rin_sym_range <- round(c(1 - max_abs, 1 + max_abs), 4)
#
# cat("\n=== Print Data Ranges ===\n")
# cat("DEM (Elevation):", dem_range[1], "to", dem_range[2], "\n")
# cat("Slope (degrees):", slope_range[1], "to", slope_range[2], "\n")
# cat("Aspect (degrees):", aspect_range[1], "to", aspect_range[2], "\n\n")
# cat("Rin:", rin_range[1], "to", rin_range[2], "\n\n")
#
# # Set up plotting layout and palette
# my_palette_func <- colorRampPalette(rev(brewer.pal(11, "RdBu")))
# par(mfrow = c(2, 2), mar = c(3, 3, 2, 2))
#
# # Plot topography
# image.plot(dem_450m, main = paste0("DEM (m)"), col = terrain.colors(256), zlim = dem_range, smallplot = c(0.80, 0.83, 0.15, 0.85))
# mtext("(a)", side = 3, line = 0.5, adj = 0, cex = 1, font = 2)  # 左上角添加(a)
#
# image.plot(slope_450m, main = paste0("Slope (°)"), col = terrain.colors(256), zlim = slope_range, smallplot = c(0.80, 0.83, 0.15, 0.85))
# mtext("(b)", side = 3, line = 0.5, adj = 0, cex = 1, font = 2)
#
# image.plot(aspect_450m, main = "Aspect(°)", col = my_palette_func(256), zlim = aspect_range, smallplot = c(0.80, 0.83, 0.15, 0.85))
# mtext("(c)", side = 3, line = 0.5, adj = 0, cex = 1, font = 2)
#
# image.plot(rin_450m, main = "Rin", col = my_palette_func(256), zlim = rin_sym_range, smallplot = c(0.80, 0.83, 0.15, 0.85))
# mtext("(d)", side = 3, line = 0.5, adj = 0, cex = 1, font = 2)
#
# par(mfrow = c(1, 1))
