#' Calculate Topographic Radiation Index (TRI) from DEM
#'
#' This function processes a DEM to compute slope, aspect, and topographic
#' radiation index (TRI) for a given region. It handles resampling to a target
#' grid, saves intermediate rasters, and performs parallel computation of solar
#' radiation.
#'
#' @param input_dem A SpatRaster or file path to DEM data
#' @param reg_id Region identifier string used for output file naming
#' @param aligned_raster Optional target raster template for resampling (default: NULL)
#' @param output_dir Directory path for saving output files
#' @param chunk_size Number of rows per chunk for parallel processing (default: 1000)
#' @param workers Number of CPU cores for parallel computation (default: 16)
#'
#' @return Saves DEM, slope, aspect, and TRI rasters to output_dir
#' @export
#'
#' @examples
#' \dontrun{
#' calc_tri(input_dem, "Swiss", aligned_30m, "/data/output", 1000, 16)
#' }
calc_tri <- function(input_dem, reg_id, aligned_raster = NULL, output_dir,
                     chunk_size = 1000, workers = 16) {

  # ---- Create output directory if it doesn't exist ----
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    message("Directory created: ", output_dir)
  }

  # ---- Define output file paths ----
  dem_file <- file.path(output_dir, paste0(reg_id, "_dem_30m.tif"))
  slope_file <- file.path(output_dir, paste0(reg_id, "_slope_degrees_30m.tif"))
  aspect_file <- file.path(output_dir, paste0(reg_id, "_aspect_degrees_30m.tif"))
  rin_file <- file.path(output_dir, paste0(reg_id, "_tri_30m.tif"))

  # ---- Load DEM raster ----
  if (is.character(input_dem)) input_dem <- terra::rast(input_dem)
  if (!inherits(input_dem, "SpatRaster")) stop("Input must be a SpatRaster or valid file path.")

  # ---- Compute slope and aspect with or without Resample ----
  if (is.null(aligned_raster)) {
    message("Use original DEM resolution!")
    dem <- input_dem
    slope <- terrain(input_dem, v = "slope", unit = "degrees")
    aspect <- terrain(input_dem, v = "aspect", unit = "degrees")
  } else {
    message("Resample DEM to aligned raster grid!")
    dem <- resample(input_dem, aligned_raster, method = "bilinear")

    # Compute slope and resample
    slope <- terrain(input_dem, v = "slope", unit = "degrees") |>
      resample(aligned_raster, method = "bilinear")

    # Compute aspect using trigonometric resampling to handle circular data
    aspect <- terrain(input_dem, v = "aspect", unit = "radians")
    aspect_cos <- cos(aspect) |> resample(aligned_raster, method = "bilinear")
    aspect_sin <- sin(aspect) |> resample(aligned_raster, method = "bilinear")
    aspect <- (atan2(aspect_sin, aspect_cos) * 180 / pi) %% 360
  }

  # ---- Save intermediate rasters ----
  save_raster(dem, dem_file)
  save_raster(slope, slope_file)
  save_raster(aspect, aspect_file)

  # ---- Extract topography (slope and aspect) to data frame ----
  # !!!!!!!!!!
  # IMPORTANT: Solar radiation calculation requires latitude in geographic
  # coordinates (-90 to 90). If slope/aspect rasters use a projected CRS,
  # compute latitude from the CRS projection first.
  df_topo <- as.data.frame(slope, xy = TRUE) |>
    left_join(as.data.frame(aspect, xy = TRUE), by = c("x", "y")) |>
    tidyr::drop_na()
  colnames(df_topo) <- c("lon", "lat", "slope", "aspect")
  message(sprintf("Number of valid pixels: %d", nrow(df_topo)))

  # ---- Compute solar radiation and TRI ----
  tictoc::tic()

  # Calculate flat-surface (horizontal) radiation for each latitude
  sw_meteoland_flat_vec <- unlist(ave(df_topo$lat, df_topo$lat,
                                      FUN = function(x) cacl_meteoland_sw_in(x[1], 0, 0, 2020)))

  # Parallel computation of slope-adjusted radiation (split into chunks)
  df_topo_split <- split(df_topo, ceiling(1:nrow(df_topo) / chunk_size))

  sw_meteoland_surf_vec <- unlist(mclapply(df_topo_split, function(chunk) {
    cacl_meteoland_sw_in(chunk$lat, chunk$slope, chunk$aspect, 2020)
  }, mc.cores = workers))

  # Calculate Topographic Radiation Index (TRI = actual / flat radiation)
  rin <- df_topo |>
    mutate(rin = sw_meteoland_surf_vec / sw_meteoland_flat_vec) |>
    df_to_raster("lon", "lat", "rin", slope, output_file = rin_file)

  tictoc::toc()

  return(list(
    rin = rin,                    # Topographic Radiation Index
    dem = dem,                    # Digital Elevation Model (resampled)
    slope = slope,                # Slope in degrees
    aspect = aspect               # aspect in degrees
    ))
}

# # ================= Example usage ==============================================
# library(terra)
# library(dplyr)
# library(parallel)
# source(here::here("R/config.R"))
# source(here::here("R/save_raster.R"))
# source(here::here("R/extent_to_tile_ids.R"))
# source(here::here("R/create_aligned_template.R"))
# source(here::here("R/cacl_meteoland_sw_in.R"))
# source(here::here("R/df_to_raster.R"))
#
# # Set worker configuration based on system
# if (hostname == "dash") {
#   chunk_size <- 1000
#   workers <- 16
# } else {
#   chunk_size <- 10000
#   workers <- 100
# }
# message("→ Using ", workers, " workers and chunk_size = ", chunk_size, " for parallel processing!\n")
#
# # Define region of interest
# reg_extent <- ext(7.1, 7.53, 46.9, 47.19)
# reg_id <- "swiss"
# output_dir <- "/data_2/scratch/ting/veg_topo_data/data/TRI_rubens"
#
# # Create aligned raster template (30m resolution)
# aligned_30m <- create_aligned_template(reg_extent, res_out = 0.00025)
#
# # Load DEM data (without cropping to avoid edge effects for slope/aspect calculation)
# input_dem <- extent_to_tile_ids(reg_extent, tile_size = 1, return_raster = TRUE,
#                                 source = "COP30", tiles_dir = COP30_dir, if_crop = FALSE)
#
# # Run TRI calculation
# topo_vars <- calc_tri(input_dem, reg_id, aligned_30m, output_dir, chunk_size, workers)
#
# # Plot results for quick inspection
# plot(input_dem)
# plot(topo_vars$dem)
# plot(topo_vars$rin)
# topo_vars$rin

