# ------calculate_land_use_fraction---------------
calculate_fraction_land_use <- function(df_win, output_file = NULL){

  # set reference land cover classes
  ref_classes <- c(10, 11, 12, 20, 30, 40, 190, 200, 201, 202, 210)

  # calculation
  df_flc <- df_win  |>

    mutate(lccs_class = factor(lccs_class, levels = ref_classes)) |>
    count(lon_mid, lat_mid, lccs_class, .drop = FALSE) |>

    group_by(lon_mid, lat_mid) |>
    mutate(prop = n / sum(n)) |>

    summarise(
      fused = sum(prop[lccs_class %in% c(10, 11, 12, 20, 190)], na.rm = TRUE) +
        0.75 * sum(prop[lccs_class == 30], na.rm = TRUE) +
        0.25 * sum(prop[lccs_class == 40], na.rm = TRUE),
      fbare   = sum(prop[lccs_class %in% c(200, 201, 202)], na.rm = TRUE),
      fwi = sum(prop[lccs_class %in% c(210, 220)], na.rm = TRUE),
      .groups = "drop"
    )

  # ------ save 5km flc output -------
  if (!is.null(output_file)) {
    flc_r <- terra::rast(
      df_flc[, c("lon_mid", "lat_mid", "fused", "fbare", "fwi")],
      type = "xyz",
      crs = "EPSG:4326"
    )

    names(flc_r) <- c("fused", "fbare", "fwi")

    terra::writeCDF(flc_r, output_file, overwrite = TRUE)
  }

  return(df_flc)
}
