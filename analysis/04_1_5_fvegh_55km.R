# ---- Setup -------------------------------------------------------------------

# load library
library(terra)

# Load configuration and functions
source(here::here("config.R"))
source(here::here("R/aggregate_byfile.R"))


# ---- Aggregate to 55km -------------------------------------------------------------

# Aggregation
aggregate_byfile(
  input_path = vegh_450m_mosaic_path,
  output_path = fvegh_55km_path,
  target_path = ai_55km_file,
  varname = "fvegh",
  if_resample = TRUE,
  fun = function(x, na.rm) {
    total <- length(x)  # includes NA and 0
    if (total == 0) {
      return(NA)
    } else {
      return(sum(!is.na(x) & x != 0) / total)
    }
  }
)
#
# # check the data
# r_in <- terra::rast(vegh_450m_mosaic_path)
# r_out <- terra::rast(fvegh_55km_path)
#
# r_in
# r_out
#
# plot(r_out)
