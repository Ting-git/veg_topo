
# ---------------- SetUp -------------------------------------------------------
library(terra)
library(ggplot2)
library(tidyterra)
library(scico)
library(rnaturalearth)
library(sf)

# source(here::here("config.R"))

# ---------- File configuration ------------------------------------------------
valid_tiles_info_path <- here::here("data/valid_tiles_info.rds")

sw_in_uneven_450m_path <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_450m.nc")
sw_in_flat_450m_path <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_flat_450m.nc")
sw_in_terrain_effect_450m_path <- file.path("/storage/scratch/giub_geco/tting/global_sw_in_450m/sw_in_terrain_effect_450m.nc")

ext_global <- ext(-180, 180, -60, 85)

# ---------- data pre ----------------------------------------------------------
# load coast outline, vector data
coast <- rnaturalearth::ne_coastline(scale = 110, returnclass = "sf")

# ------- Tiles Details --------------------------------------------------------

valid_tiles_info <- readRDS(valid_tiles_info_path)

gc()
plan(multisession, workers = 49)

t00 <- Sys.time()
message(paste0("Start plotting:", format(t00, "%Y-%m-%d %H:%M:%S")))

results <- future_pmap(
  tiles_info,
  function(...) {
    args <- list(...)
    tryCatch({

      tile_id <- args$regA_info

      # set the input
      ext <- terra::ext(args$xmin, args$xmax, args$ymin, args$ymax)

      sw_in_uneven_450m <- rast(sw_in_uneven_450m_path )
      sw_in_flat_450m <- rast(sw_in_flat_450m_path)


      message(sprintf("tile %s done [%.1f mins]", tile_id, difftime(Sys.time(), t0, units = "mins")))

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
message(sprintf("All tiles done [%.1f mins]", elapsed))
