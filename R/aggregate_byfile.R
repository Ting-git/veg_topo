# Declare necessary libraries
library(terra)      # For raster manipulation functions like rast, xres, yres, aggregate, writeCDF
library(stringr)    # For string manipulation functions like str_remove

aggregate_byfile <- function(filename_vegheight, raster_target, outdir){

  # Load the two raster files
  r1 <- rast(filename_vegheight)  # Load the raster file (obtained from a specified source)

  # Aggregate r1 to match r2's resolution
  # The factor is determined by the ratio of resolutions
  fact_x <- (xres(raster_target) / xres(r1))  # Calculate aggregation factor along x-axis
  fact_y <- (yres(raster_target) / yres(r1))  # Calculate aggregation factor along y-axis

  # Aggregate using mean (can be changed to other functions like max, min, sum)
  r1_aggregated <- aggregate(r1, fact = c(fact_x, fact_y), fun = mean, na.rm = TRUE)

  # Create output file name and write to file
  outfilnam <- paste0(outdir, str_remove(basename(filename_vegheight), ".tif"), "_15arcsec.nc")

  # Write the aggregated raster to NetCDF format
  message(paste("Writing to file", outfilnam, "..."))
  writeCDF(r1_aggregated, outfilnam, overwrite = TRUE)
}
