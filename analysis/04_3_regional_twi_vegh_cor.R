# ~30 min

# ------Load required libraries-------------------------------------------------------------
library(terra)     # For handling raster data
library(dplyr)
library(purrr)     # For functional programming tools like pmap_dfr

source(here::here("config.R"))
source(here::here("R/generate_tile_grid.R"))
source(here::here("R/extent_to_tile_ids.R"))
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/calculate_correlation_bywin.R"))
# source(here::here("R/filter_land_tiles.R"))

# load region infos
regA_info <- readRDS(here::here("data/df_samples_A.rds"))[,8:12]

# set region info
reg_id <- regA_info$strata_A_label[1]

xmin <- regA_info$xmin[1]
xmax <- regA_info$xmax[1]
ymin <- regA_info$ymin[1]
ymax <- regA_info$ymax[1]

ext <- terra::ext(regA_info$xmin[1], regA_info$xmax[1], regA_info$ymin[1], regA_info$ymax[1])

ext
# ---- main processing -------

# Get TWI raster
twi_r <- terra::rast(twi_30m_path)
twi_rc <- terra::crop(twi_r, ext)

# Get VegH raster
tile_ids <- extent_to_tile_ids(ext)
vegh_filepaths <- file.path(vegh_10m_tiles_dir, paste0("ETH_GlobalCanopyHeight_10m_2020_", tile_ids, "_Map.tif"))
vegh_rs <- lapply(vegh_filepaths, terra::rast)
vegh_rm <- do.call(terra::mosaic, c(vegh_rs, fun = mean))

vegh_rc <- terra:: crop (vegh_rm, ext)
vegh_rr <- resample(vegh_rm, twi_rc, method = "bilinear")

stacked_r <- c(twi_rc, vegh_rr)
names(stacked_r) <- c("twi", "vegh")

plot(stacked_r)

vegh_rm

plot(vegh_rm, axes = TRUE, asp = 1)
plot(vegh_rc, axes = TRUE, asp = 1)


# df_win <- create_spatial_windows(stacked_r, dwin = 0.0025) # 10X10 window
df_win <- create_spatial_windows(stacked_r, dwin = 0.005) # 20X20 window
df_cor <- calculate_correlation_bywin(df_win)
# ----------------------------





# --------- Parallel Processing for Each Regions -------------------------------

gc()
plan(multisession, workers = 8)

t00 <- Sys.time()
message(paste0("Start Regional Correlation Analysis:", format(t00, "%Y-%m-%d %H:%M:%S")))

results <- future_pmap(
  regA_info,
  function(...) {
    args <- list(...)
    tryCatch({

      # set region info
      reg_id <- args$strata_A_label
      ext <- terra::ext(args$xmin, args$xmax, args$ymin, args$ymax)

      # ---- main processing -------


      # ----------------------------

      message(sprintf("region %s done [%.1f mins]", tile_id, difftime(Sys.time(), t0, units = "mins")))

    }, error = function(e) {
      msg <- sprintf("Tile %s failed: %s", args$tile_id %||% "unknown", conditionMessage(e))
      message("❌ ", msg)
      return(list(success = FALSE, error = msg))
    })
  },
  .options = furrr_options(seed = TRUE)
)

plan(sequential)
gc()

elapsed <- as.numeric(difftime(Sys.time(), t00, units = "mins"))
message(sprintf("All regions done [%.1f mins]", elapsed))
