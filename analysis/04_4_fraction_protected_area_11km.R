# 📦 Load required packages
library(terra)
library(sf)
library(rmapshaper)   # for robust geometry union


# 🔧 Disable S2 geometry engine (for better tolerance of invalid shapes)
sf_use_s2(FALSE)

# 📁 Load configuration (defines paths like pa_shp0, pa_11km_path, etc.)
source(here::here("config.R"))

# 🗺️ Read shapefiles, keeping only geometry column
sf0 <- st_read(pa_shp0, quiet = TRUE)[, "geometry"]
sf1 <- st_read(pa_shp1, quiet = TRUE)[, "geometry"]
sf2 <- st_read(pa_shp2, quiet = TRUE)[, "geometry"]

# ✅ Fix invalid geometries
sf0 <- st_make_valid(sf0)
sf1 <- st_make_valid(sf1)
sf2 <- st_make_valid(sf2)

# ❌ Drop geometries that are still invalid
sf0 <- sf0[st_is_valid(sf0), ]
sf1 <- sf1[st_is_valid(sf1), ]
sf2 <- sf2[st_is_valid(sf2), ]

# 🧹 (Optional) Simplify geometries to reduce topological errors
sf0 <- st_simplify(sf0, dTolerance = 0.0001, preserveTopology = TRUE)
sf1 <- st_simplify(sf1, dTolerance = 0.0001, preserveTopology = TRUE)
sf2 <- st_simplify(sf2, dTolerance = 0.0001, preserveTopology = TRUE)

# 🌐 Reproject to an equal-area CRS for union operations
ea_crs <- "ESRI:54017"
sf0 <- st_transform(sf0, ea_crs)
sf1 <- st_transform(sf1, ea_crs)
sf2 <- st_transform(sf2, ea_crs)

# 🔀 Robust union using Mapshaper (handles complex topologies well)
sf0_u <- ms_union(sf0)
sf1_u <- ms_union(sf1)
sf2_u <- ms_union(sf2)

# 🧩 Merge all unioned geometries
sf_all_u <- st_union(c(sf0_u, sf1_u, sf2_u))

# 🌍 Transform back to WGS84 (lat/lon)
sf_all_u <- st_transform(sf_all_u, crs = "EPSG:4326")

# 🔄 Convert to terra vector
pa_all <- vect(sf_all_u)

# 🧱 Define raster extent and resolution (0.1° grid)
r_template <- rast(ext(-180, 180, -60, 90), resolution = 0.1, crs = "EPSG:4326")
values(r_template) <- NA

# 🧮 Extract protected area coverage per pixel (weighted)
coverage_df <- extract(r_template, pa_all, weights = TRUE, normalize = TRUE)

# 🧾 Aggregate weights by pixel ID
coverage_sum <- aggregate(coverage_df$weight, by = list(coverage_df$ID), FUN = sum)

# 🖼️ Create final output raster
pa_ratio_raster <- r_template
values(pa_ratio_raster) <- 0
pa_ratio_raster[coverage_sum$Group.1] <- coverage_sum$x

# 💾 Save the raster to file
writeRaster(pa_ratio_raster, pa_11km_path, overwrite = TRUE)

cat("✅ Output saved: protected_area_ratio_0\n")

# 🧹 Clean up
rm(list = ls())
gc()
