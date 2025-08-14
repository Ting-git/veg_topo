# ~0.4 min

# ------Load required libraries-------------------------------------------------
library(terra)     # For handling raster data
library(dplyr)
library(ggplot2)
library(tidyterra)
library(scico)
library(khroma)
library(patchwork)

library(furrr)     # For functional programming tools like pmap_dfr
library(future)

source(here::here("config.R"))
source(here::here("R/plot_hex_vegh_twi.R"))
source(here::here("R/plot_cor_twi_vegh.R"))
source(here::here("R/plot_twi.R"))
source(here::here("R/plot_vegh.R"))


# --- Load Region Info ---
regA_info <- readRDS(here::here("data/df_samples_A.rds")) |>
  select(ends_with("label"), ends_with("min"), ends_with("max"))

regA_info

# --------------- none paralell testing ----------------------------------------

# --- Set Region Extent ---

reg_id <- regA_info$strata_A_label[1]

xmin <- regA_info$xmin[1]
xmax <- regA_info$xmax[1]
ymin <- regA_info$ymin[1]
ymax <- regA_info$ymax[1]

ext <- terra::ext(xmin, xmax, ymin, ymax)

# ---main processing ---
# copy to here


# --------- Parallel Processing for Each Regions -------------------------------

gc()
plan(multisession, workers = 8)

t00 <- Sys.time()
message(paste0("Plot for each region start:", format(t00, "%Y-%m-%d %H:%M:%S")))

results <- future_pmap(
  regA_info,
  function(...) {
    args <- list(...)
    tryCatch({

      t0 <- Sys.time()

      # set region info
      reg_id <- args$strata_A_label
      ext <- terra::ext(args$xmin, args$xmax, args$ymin, args$ymax)

      # ---- main processing ---------------------------------------------------
      # Load raster data for correlation, TWI, and vegetation height
      reg_cor_nc_path <- file.path(regA_cor_twi_vegh_dir, paste0("regA_", reg_id, "_cor_twi_vegh_500m.nc"))
      reg_twi_nc_path <- file.path(regA_cor_twi_vegh_dir, paste0("regA_", reg_id, "_twi_30m.nc"))
      reg_vegh_nc_path <- file.path(regA_cor_twi_vegh_dir, paste0("regA_", reg_id, "_vegh_30m.nc"))

      reg_cor_r <- terra::rast(reg_cor_nc_path)
      reg_twi_r <- terra::rast(reg_twi_nc_path)
      reg_vegh_r <- terra::rast(reg_vegh_nc_path)

      text_size <- 6

      # Convert correlation raster to data frame for violin and boxplot
      df_cor <- as.data.frame(reg_cor_r, xy = FALSE, na.rm = TRUE)
      names(df_cor) <- "value"

      # Plot distribution of correlation values (r(H ~ TWI))
      p_ds <- ggplot(df_cor, aes(x = factor(reg_id), y = value)) +
        geom_violin(fill = "skyblue", color = "blue", alpha = 0.6) +
        geom_boxplot(width = 0.1, outlier.color = "red", alpha = 0.8) +
        labs(title = "Distribution of r (H ~ TWI)", y = "r (H~TWI)", x = NULL) +
        theme_bw(base_size = text_size) +
        theme(
          axis.title = element_text(size = text_size),
          axis.text = element_text(size = text_size * 0.9),
          plot.title = element_text(size = text_size * 1.2, face = "bold"),
          plot.title.position = "panel"
        )

      # Stack TWI and vegetation height rasters and convert to dataframe
      stacked <- c(reg_twi_r, reg_vegh_r)
      names(stacked) <- c("twi", "vegh")
      df_st <- as.data.frame(stacked, xy = FALSE, na.rm = TRUE)

      # plot vegetation height with TWI scatterplot
      p_dt <- plot_hex_vegh_twi(df_st)

      # plot correlation raster map
      p_cor <- plot_cor_twi_vegh(
        input = reg_cor_r,
        extent = ext,
        x_breaks = 0.1,
        y_breaks = 0.1,
        text_size = text_size
      )

      # plot TWI raster map
      p_twi <- plot_twi(
        input = reg_twi_r,
        extent = ext,
        text_size = text_size,
        x_breaks = 0.1,
        y_breaks = 0.1
      )

      # plot vegetation height raster map
      p_vegh <- plot_vegh(
        input = reg_vegh_r,
        extent = ext,
        text_size = text_size,
        x_breaks = 0.1,
        y_breaks = 0.1
      )

      # Combine all plots into a 2-row, 3-column layout with titles and tags
      final_plot <- wrap_plots(
        list(p_twi, p_vegh, p_cor, p_dt, p_ds),
        ncol = 3,
        nrow = 2,
        heights = c(2, 1.5),
        align = "hv"
      ) +
        plot_annotation(
          title = paste0(reg_id, " Analysis"),
          tag_levels = "A",
          theme = theme(
            plot.title = element_text(size = 10, face = "bold", hjust = 0.5)
          )
        )

      # Save combined plot as high-res PNG
      ggsave(
        filename = here::here(paste0("data/figures/04_", reg_id, "_combined_plot.png")),
        plot = final_plot,
        width = 12,
        height = 8,
        dpi = 300
      )

      # ------------------------------------------------------------------------

      message(sprintf("Plot region %s done [%.1f mins]", reg_id, difftime(Sys.time(), t0, units = "mins")))

    }, error = function(e) {
      msg <- sprintf("Region %s failed: %s", args$strata_A_label %||% "unknown", conditionMessage(e))
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

# ------ Cleanup ---------------------------------------------------------------

rm(list = ls())
gc

