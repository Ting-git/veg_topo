# ------Set up------------------------------------------------------------------------

library(terra)
library(dplyr)

# ------Load configuration and helper functions---------------------------------------------

source(here::here("config.R"))
source(here::here("R/split_window_analysis.R"))


# ------calculate fraction of land use---------------------------------------------
lc_r <- terra::rast(cci_landcover_path, lyrs = "lccs_class")

# plot(lc_r)

d_win <- create_spatial_windows(lc_r,
                                coord_vars = c("lon", "lat"),
                                value_vars = "lccs_class",
                                dwin = 0.05)

head(d_win)
