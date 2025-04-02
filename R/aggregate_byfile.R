# Declare necessary libraries
library(terra)      # For raster manipulation functions like rast, xres, yres, aggregate, writeCDF
library(stringr)    # For string manipulation functions like str_remove

aggregate_byfile <- function(filename, rast_tar, outdir){

  # Load the two raster files
  rast_ob <- rast(filename)  # Load the raster file (obtained from a specified source)

  # Aggregate r1 to match r2's resolution
  # The factor is determined by the ratio of resolutions
  fact_x <- (xres(rast_tar) / xres(rast_ob))  # Calculate aggregation factor along x-axis
  fact_y <- (yres(rast_tar) / yres(rast_ob))  # Calculate aggregation factor along y-axis

  # Aggregate using mean (can be changed to other functions like max, min, sum)
  rast_agg <- aggregate(rast_ob, fact = c(fact_x, fact_y), fun = mean, na.rm = TRUE)

  # Create output file name and write to file
  outfilename<- paste0(outdir, str_remove(basename(filename), ".tif"), "_to450m.nc")

  # Write the aggregated raster to NetCDF format
  message(paste("Writing to file", outfilename, "..."))
  writeCDF(rast_agg, outfilename, overwrite = TRUE)

  # Clean up memory by removing large objects after writing the file
  rm(rast_ob, rast_agg)  # Remove the raster objects to free memory
  gc()  # Trigger garbage collection to release memory
}


