# ------Set up------------------------------------------------------------------------

library(terra)
library(tidyr)
library(dplyr)
library(purrr)
library(furrr)

# ------Load configuration and helper functions---------------------------------------------

source(here::here("config.R"))
source(here::here("R/split_window_analysis.R"))
source(here::here("R/build_global_tiles.R"))

tiles_info <- readRDS(tiles_info_path)

# ------calculate fraction of land use---------------------------------------------

# Start paralell processing
gc()
plan(multisession, workers = 8)
t00 <- Sys.time()

results <- future_pmap(
  tiles_info,
  function(...) {
    args <- list(...)
    tryCatch({

      tile_id <- args$tile_id

      # set the input
      ext <- terra::ext(args$xmin, args$xmax, args$ymin, args$ymax)
      lc_r <- terra::rast(cci_landcover_path, lyrs = "lccs_class")
      rc <- terra::crop(lc_r, ext)

      # set start time
      t0 <- Sys.time()
      print(t0)

      # create window bins
      df_win <- create_spatial_windows(rc, value_vars = "lccs_class", dwin = 0.05)

      # plot the density of land use classes
      # df_win |> count(lccs_class) |>
      #   mutate(prop = n / sum(n)) |>
      #   ggplot(aes(x = factor(lccs_class), y = prop)) +
      #   geom_col(fill = "grey70", color = "black") +
      #   labs(title = "Land Cover Class Frequency",
      #        x = "LCCS Class",
      #        y = "Density") +
      #   theme_classic()

      # calculate the fractino of used, bared, water areas and save output
      output_file <- file.path(win_flc_5km_tiles_dir, paste0("win_flc_5km_", tile_id, ".nc"))
      df_flc <- calculate_fraction_land_use(
        df_win,
        output_file = output_file)

      # ---------- ploting the fraction of used land ----------
      # plot_his <- ggplot(
      #   data = df_flc,
      #   aes(x = f_used, y = after_stat(density))) +
      #   geom_histogram(fill = "grey70", color = "black") +
      #   geom_density(color = 'red')+
      #   labs(title = 'Histogram, density and boxplot',
      #        x = expression(paste("fraction of used land"))) +
      #   theme_classic()
      #
      # plot_box <- ggplot(
      #   data = df_flc,
      #   aes(x = "", y = f_used)) +
      #   geom_boxplot(fill = "grey70", color = "black") +
      #   coord_flip() +
      #   theme_classic() +
      #   theme(axis.text.y=element_blank(),
      #         axis.ticks.y=element_blank()) +
      #   labs(y = expression(paste("fraction of used land")))
      #
      # cowplot::plot_grid(plot_his, plot_box,
      #                    ncol = 2, rel_heights = c(2,1),
      #                    align = 'v', axis = 'lr')


      # --------- plot spatial fraction ------------
      # library(patchwork)  # install.packages("patchwork") if needed
      #
      # df_long <- df_flc |>
      #   pivot_longer(cols = starts_with("f_"), names_to = "fraction_type", values_to = "value")
      #
      # ggplot(df_long, aes(x = lon_mid, y = lat_mid, fill = value)) +
      #   geom_tile() +
      #   facet_wrap(~fraction_type) +
      #   scale_fill_viridis_c(option = "C") +
      #   coord_equal() +
      #   labs(title = "Spatial Fractions", fill = "Value") +
      #   theme_minimal()
      #

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

message(sprintf("all tiles done [%.1fmins]", difftime(Sys.time(), t00, units = "mins")))
