# ~4.5 min for 18 sample regions

# ------Load required libraries-------------------------------------------------------------
library(terra)     # For handling raster data
library(dplyr)
library(furrr)     # For functional programming tools like pmap_dfr
library(future)

source(here::here("config.R"))
source(here::here("R/generate_tile_grid.R"))
source(here::here("R/extent_to_tile_ids.R"))
source(here::here("R/create_spatial_windows.R"))
source(here::here("R/calculate_correlation_bywin.R"))
# source(here::here("R/filter_land_tiles.R"))


# --- Load Region Info ---
regA_info <- readRDS(here::here("data/df_samples_A.rds")) |>
  select(ends_with("label"), ends_with("min"), ends_with("max"), sample_id)

# --------------- Main Processing Function -------------------------------------

#' Process a single region for TWI and vegetation height correlation analysis
#'
#' @param reg_id Region identifier
#' @param xmin,xmax,ymin,ymax Extent coordinates
#' @param sample_id Sample identifier
#' @param output_dir Directory to save output NetCDF files
#' @param dwin Window size for spatial analysis (in degrees)
#'
#' @return Returns TRUE if successful, FALSE otherwise
process_region <- function(reg_id, xmin, xmax, ymin, ymax, sample_id,
                           output_dir = regA_cor_twi_vegh_dir,
                           dwin = 0.005) {

  tryCatch({
    t0 <- Sys.time()

    # Create full region ID
    full_reg_id <- paste0(reg_id, "_", sample_id)
    ext <- terra::ext(xmin, xmax, ymin, ymax)

    # --- Load TWI Raster ---
    twi_r <- terra::rast(twi_30m_path)
    names(twi_r) <- "twi"
    twi_rc <- terra::crop(twi_r, ext)

    # --- Load and Prepare Vegetation Height Raster ---
    vegh_rc <- extent_to_tile_ids(ext, tile_size = 3, return_raster = TRUE,
                                  source = "lang_vegh_10m", tiles_dir = vegh_10m_tiles_dir)
    names(vegh_rc) <- "vegh"

    # Resample vegH to TWI resolution
    vegh_rr <- terra::resample(vegh_rc, twi_rc, method = "bilinear")

    # --- Stack Rasters ---
    stacked_r <- c(twi_rc, vegh_rr)
    names(stacked_r) <- c("twi", "vegh")

    # --- Create Spatial Windows and Compute Correlation ---
    df_win <- create_spatial_windows(stacked_r, dwin = dwin)
    df_cor <- calculate_correlation_bywin(df_win)

    # --- Convert Correlation to Raster and Save as NetCDF ---
    cor_r <- terra::rast(
      df_cor[, c("lon_mid", "lat_mid", "correlation")],
      type = "xyz",
      crs = "EPSG:4326"
    )
    names(cor_r) <- "correlation"

    cor_nc_path <- file.path(output_dir, paste0("regA_", full_reg_id, "_cor_twi_vegh_500m.nc"))
    terra::writeCDF(cor_r, cor_nc_path, overwrite = TRUE)
    message("Saved: ", cor_nc_path)

    # --- Save Vegetation Height as NetCDF ---
    vegh_nc_path <- file.path(output_dir, paste0("regA_", full_reg_id, "_vegh_30m.nc"))
    terra::writeCDF(vegh_rr, vegh_nc_path, overwrite = TRUE)
    message("Saved: ", vegh_nc_path)

    # --- Save TWI as NetCDF ---
    twi_nc_path <- file.path(output_dir, paste0("regA_", full_reg_id, "_twi_30m.nc"))
    terra::writeCDF(twi_rc, twi_nc_path, overwrite = TRUE)
    message("Saved: ", twi_nc_path)

    elapsed_mins <- difftime(Sys.time(), t0, units = "mins")
    message(sprintf("Region %s completed [%.1f mins]", full_reg_id, elapsed_mins))

    rm(twi_r, vegh_rc, stacked_r, df_win, df_cor); gc()
    return(TRUE)

  }, error = function(e) {
    msg <- sprintf("Region %s_%s failed: %s", reg_id, sample_id, conditionMessage(e))
    message("❌ ", msg)
    return(FALSE)
  })
}

# --------- Parallel Processing for Each Region -------------------------------

gc()
plan(multisession, workers = 8)

t00 <- Sys.time()
message(paste0("Regional Correlation Analysis Start: ", format(t00, "%Y-%m-%d %H:%M:%S")))

# Process all regions in parallel
results <- future_pmap(
  list(
    reg_id = regA_info$strata_A_label,
    xmin = regA_info$xmin,
    xmax = regA_info$xmax,
    ymin = regA_info$ymin,
    ymax = regA_info$ymax,
    sample_id = regA_info$sample_id
  ),
  process_region,
  .options = furrr_options(seed = TRUE)
)

plan(sequential)
gc()

# Summary of processing results
success_count <- sum(unlist(results))
total_count <- length(results)
failure_count <- total_count - success_count

elapsed_mins <- difftime(Sys.time(), t00, units = "mins")
message(sprintf("All regions processed [%.1f mins]", elapsed_mins))
message(sprintf("Success: %d, Failed: %d", success_count, failure_count))
