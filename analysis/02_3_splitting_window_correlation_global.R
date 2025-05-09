# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

library(terra)
library(future)
library(tidyverse)
library(furrr)
library(scico)
library(fs)
library(progressr)
handlers(handler_txtprogressbar(style = 3))  # style = 3 is like `utils::txtProgressBar`

source(here::here("R/split_window_correlation.R"))

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

file_vegh_450m_mosaic <- file.path("/data_2/scratch/ting/data/vegh_450m/vegh_450m_2020_mosaic.nc")
file_ga2 <- file.path("/data/archive/gti_marthews_2015/data/ga2.nc")  # Target raster file path
cor_mosaic_file <- file.path("/data_2/scratch/ting/data/corr_map/corr_twi_vegh_5km_mosaic.nc")  # Output file
temp_dir <- file.path("/data_2/scratch/ting/data_temp")
cor_tiles_dir <- file.path("/data_2/scratch/ting/data/corr_map/30_30_deg")
# ------------------------------------------------------------------------------
# Preprocess --> data preparing
# ------------------------------------------------------------------------------

# Divide the globe into multipal tiles and
# name the west_south_cor(i.e. lat_min, lon_min)
generate_global_extents <- function(
    lon_step = 30,
    lat_step = 30
) {
  ext_valids <- list()

  # Define longitude and latitude edges
  lon_edges <- seq(-180, 180, by = lon_step)
  lat_edges <- seq(-90, 90, by = lat_step)

  # Label formatting functions for longitude and latitude
  make_lon_label <- function(lon) {
    if (lon < 0) paste0("W", abs(lon)) else paste0("E", lon)
  }

  make_lat_label <- function(lat) {
    if (lat < 0) paste0("S", abs(lat)) else paste0("N", lat)
  }

  # Iterate from south to north, and west to east
  for (lat_i in 1:(length(lat_edges) - 1)) {
    for (lon_j in 1:(length(lon_edges) - 1)) {
      lat_min <- lat_edges[lat_i]
      lat_max <- lat_edges[lat_i + 1]
      lon_min <- lon_edges[lon_j]
      lon_max <- lon_edges[lon_j + 1]

      # Use northwest (upper-left) corner for naming
      name <- paste0(
        make_lat_label(lat_min), "_",
        make_lon_label(lon_min)
      )

      ext_valids[[name]] <- ext(lon_min, lon_max, lat_min, lat_max)
    }
  }

  return(ext_valids)
}

# Internal processing function
pre_merge_tile_rasters <- function(tile_extents, twi_file, vegh_file, processed_dir) {
  # Load rasters
  twi_raster_full <- terra::rast(twi_file)
  vegh_raster_full <- terra::rast(vegh_file)
  mosaic_extent <- terra::ext(twi_raster_full)

  # Progress bar
  p <- progressr::progressor(along = tile_extents)

  # Process each tile
  processed_tile_files <- purrr::imap(tile_extents, function(tile_extent, tile_name) {
    p(message = paste("Processing", tile_name))
    output_file <- file.path(processed_dir, paste0(tile_name, "_twi_vegh_rm.nc"))

    # Skip if there is no overlap
    if (is.null(terra::intersect(mosaic_extent, tile_extent))) {
      message(glue::glue("⚠ Skipping {tile_name}: no intersection"))
      return(NA)
    }

    # Try processing the tile
    tryCatch({
      # Crop the TWI and vegetation rasters
      twi_crop <- terra::crop(twi_raster_full, tile_extent)
      vegh_crop <- terra::crop(vegh_raster_full, tile_extent)

      # Check if either of the cropped rasters is entirely NA
      if (terra::ncell(twi_crop) == 0 || all(is.na(twi_crop[])) || terra::ncell(vegh_crop) == 0 || all(is.na(vegh_crop[]))) {
        message(glue::glue("⚠ Skipping {tile_name}: no valid data in TWI or vegetation"))
        return(NA)
      }

      # Resample vegetation raster to match TWI raster's resolution
      vegh_resampled <- terra::resample(vegh_crop, twi_crop, method = "bilinear")

      # Mask the resampled vegetation raster with the TWI raster
      vegh_masked <- terra::mask(vegh_resampled, twi_crop)

      # Merge the TWI and masked vegetation rasters
      merged <- c(twi_crop, vegh_masked)
      names(merged) <- c("twi", "vegh")

      # Write the merged raster to the output file
      terra::writeCDF(merged, output_file, overwrite = TRUE)
      message(glue::glue("✓ Saved: {output_file}"))
      output_file
    }, error = function(e) {
      message(glue::glue("❌ Failed {tile_name}: {e$message}"))
      NA
    })
  })

  # Clean up
  rm(twi_raster_full, vegh_raster_full); gc()
  return(processed_tile_files)
}

# Main function to process all tiles and merge results
generate_prep_tiles_info <- function(twi_file, vegh_file, temp_dir) {
  # Create output subfolder
  processed_dir <- file.path(temp_dir, "pre_merged_rasters")
  dir.create(processed_dir, showWarnings = FALSE, recursive = TRUE)

  # Generate extents for 72 tiles
  tile_extents <- generate_global_extents()

  # Run processing
  prep_tile_files <- with_progress({
    pre_merge_tile_rasters(
      tile_extents = tile_extents,
      twi_file = twi_file,
      vegh_file = vegh_file,
      processed_dir = processed_dir
    )
  })

  # Merge name, extent, and file path
  all_tile_names <- names(tile_extents)

  prep_tiles_info <- map2_dfr(
    all_tile_names, seq_along(all_tile_names),
    function(name, idx) {
      ext <- tile_extents[[idx]]
      file <- prep_tile_files[[idx]]

      if (!is.na(file)) {
        tibble(
          name = name,
          xmin = terra::xmin(ext),
          xmax = terra::xmax(ext),
          ymin = terra::ymin(ext),
          ymax = terra::ymax(ext),
          file = file
        )
      } else {
        NULL
      }
    }
  )

  # Save as CSV
  csv_file <- file.path("./data", "preprocessed_tiles_info.csv")
  readr::write_csv(prep_tiles_info, csv_file)

  return(prep_tiles_info)
}

# Usage
# this process need 30 mins
# prep_tiles_info <- generate_prep_tiles_info(
#   file_ga2, file_vegh_450m_mosaic, temp_dir)
str(prep_tiles_info, max.level = 2)
# ------------------------------------------------------------------------------
# Implementation
# ------------------------------------------------------------------------------

prep_tiles_info <- readr::read_csv(here::here("data/preprocessed_tiles_info.csv"))

gc()
plan(multisession, workers = 6)

# Create a list to collect failed tiles
error_log <- list()

with_progress({
  pb <- progressor(along = prep_tiles_info)

  results <- future_pmap(
    prep_tiles_info,
    function(name, xmin, xmax, ymin, ymax, file) {
      tryCatch({
        pb(sprintf("Processing tile: %s", name))

        # Check if file exists
        if (!file.exists(file)) stop("Raster file does not exist.")

        # Load raster
        merged_raster <- terra::rast(file)

        # Your custom processing
        windowed_data <- create_spatial_windows(merged_raster, 12)
        correlation_df <- calculate_window_correlations(windowed_data)

        # Convert to raster
        correlation_raster <- terra::rast(correlation_df[, c("lon_cen", "lat_cen", "correlation", "cor_pval")],
                                          type = "xyz", crs = "EPSG:4326")
        names(correlation_raster) <- c("correlation", "cor_pval")

        # Save
        output_file <- file.path(output_dir, paste0(name,"_corr_twi_vegh_5km_.nc")) # changed here after running
        terra::writeCDF(correlation_raster, cor_tiles_dir, overwrite = TRUE)

        # Optional plot
        plot_corr(correlation_df)

        return(output_file)
      }, error = function(e) {
        msg <- sprintf("Tile %s failed: %s", name, e$message)
        message("❌ ", msg)
        error_log[[length(error_log) + 1]] <<- list(
          name = name,
          xmin = xmin,
          xmax = xmax,
          ymin = ymin,
          ymax = ymax,
          file = file,
          error = e$message
        )
        return(NA)
      })
    }
  )

  # Update progress bar in the main thread after all tasks are completed
  future_walk(results, ~pb())  # This ensures the progress bar is updated as tasks complete
})

# Restore single-threaded processing
plan(sequential)
gc()

# Summary
cat("✅ Completed tiles:", sum(!is.na(results)), "\n")
cat("❌ Failed tiles:", sum(is.na(results)), "\n")

# Save failed tiles log to CSV
if (length(error_log) > 0) {
  error_df <- bind_rows(error_log)
  log_file <- file.path(output_dir, "failed_tiles_log.csv")
  write_csv(error_df, log_file)
  message("📄 Error log saved to: ", log_file)
}





# ------------------------------------------------------------------------------
# Combination and visualisation
# ------------------------------------------------------------------------------

library(terra)
library(fs)
mosaic_and_save_rasters <- function(input_dir, output_file, pattern = "*.nc", verbose = FALSE) {
  if (verbose) message("Reading raster files...")
  files <- dir_ls(path = input_dir, glob = pattern)
  if (length(files) == 0) stop("No matching raster files found.")

  rasters <- lapply(files, rast)
  rasters <- unname(rasters)

  mosaic <- do.call(merge, rasters)

  rm(rasters)
  gc()

  writeCDF(mosaic, filename = output_file, overwrite = TRUE)

  rm(mosaic)
  gc()

  if (verbose) message("Done: Mosaic saved to ", output_file)
}


mosaic_and_save_rasters(cor_tiles_dir, cor_mosaic_file, verbose = TRUE)

# read the mosacic correlation maps
correlation_raster <- rast(cor_mosaic_file)
names(correlation_raster) <- c("correlation", "cor_pval")
plot(correlation_raster)

correlation_df <- as.data.frame(rm, xy = TRUE, na.rm = TRUE) |>
  rename(lon_cen = x, lat_cen = y,
         correlation = corr_twi_vegh_5km_mosaic_1,
         cor_pval = corr_twi_vegh_5km_mosaic_2)
summary(correlation_df)
plot_corr(correlation_df)
# ------------------------------------------------------------------------------
# Implementation --> single test
# ------------------------------------------------------------------------------

# Clear memory and set up parallel processing
prep_tiles_info <- readr::read_csv(here::here("data/preprocessed_tiles_info.csv"))
prep_tiles_info_sub <- prep_tiles_info[1:49, ]

file <- as.character(prep_tiles_info_sub[1, 6])
name <- as.character(prep_tiles_info_sub[1, 1])

# Load the merged raster
merged_raster <- terra::rast(file)

# Generate spatial windows (e.g., 5km or 12-cell window)
windowed_data <- create_spatial_windows(merged_raster, 12)

# Calculate correlation and p-values
correlation_df <- calculate_window_correlations(windowed_data)

# Convert to raster
correlation_raster <- terra::rast(correlation_df[, c("lon_cen", "lat_cen", "correlation", "cor_pval")],
                                  type = "xyz", crs = "EPSG:4326")
names(correlation_raster) <- c("correlation", "cor_pval")

# Define output path
output_file <- file.path(output_dir, paste0("corr_twi_vegh_5km_", name, ".nc"))

# Save the raster
terra::writeCDF(correlation_raster, output_file, overwrite = TRUE)

# Optionally plot (or save plots)
plot_corr(correlation_df)


# ------------------------------------------------------------------------------
# Implementation --> single test 2
# ------------------------------------------------------------------------------

# Clear memory and set up parallel processing
prep_tiles_info <- readr::read_csv(here::here("data/preprocessed_tiles_info.csv"))
prep_tiles_info_sub <- prep_tiles_info[1:49, ]

file <- as.character(prep_tiles_info_sub[1, 6])
name <- as.character(prep_tiles_info_sub[1, 1])


# Load processed raster
merged_raster <- terra::rast(file)
suppressWarnings({windowed_data <- as.data.frame(merged_raster, xy = TRUE, na.rm = TRUE)})
colnames(windowed_data) <- c("lon", "lat", "twi", "vegh")

nc_path <- file.path(cor_tiles_dir, paste0("corr_twi_vegh_5km_",name,".nc"))
corr_raster <- terra::rast(nc_path)
suppressWarnings({correlation_df <- as.data.frame(corr_raster, xy = TRUE, na.rm = TRUE)})
colnames(correlation_df) <- c("lon_cen", "lat_cen", "correlation", "pval")

# Generate plots
plot1 <- plot_twi(windowed_data)
plot2 <- plot_vegh(windowed_data)
plot3 <- plot_corr(correlation_df)
# plot4 <- plot_correlation_vs_pixel_count(correlation_df)
plot5 <- plot_overview(windowed_data)

plot1
plot2
plot3
plot5

source(here::here("R/split_window_correlation.R"))
png_path <- save_combined_plot(
  plots = list(plot1, plot2, plot3, plot5),
  region_name = name,
  title_text = "VEGH and TWI Correlation Analysis",
  ncol = 2,
  width = 18, height = 16,
  file_index = "02_3"
)

# Clean memory
rm(merged_raster, windowed_data, correlation_df, correlation_raster)
gc()
