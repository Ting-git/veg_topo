cacl_meteoland_sw_in <- function(lat_deg, slope_deg, aspect_deg, year = 2020) {

  deg2rad <- function(d) d * pi / 180

  # Convert all inputs to radians once (vectorized)
  lat_rad <- deg2rad(lat_deg)
  slope_rad <- deg2rad(slope_deg)
  aspect_rad <- deg2rad(aspect_deg)

  # Pre-calculate daily solar declination for the entire year (once only)
  dates <- seq(as.Date(paste0(year, "-01-01")),
               as.Date(paste0(year, "-12-31")),
               by = "day")
  J <- as.numeric(format(dates, "%j"))

  # Pre-compute declination for all days
  delta_values <- sapply(J, meteoland::radiation_solarDeclination)

  # Solar constant (MJ/m²/day)
  Sc <- 1.361

  # Vectorized calculation for all points (returns raw slope radiation)
  calc_annual_raw <- function(lr, sr, ar) {
    daily_rad <- sapply(delta_values, function(delta) {
      meteoland::radiation_potentialRadiation(
        solarConstant = Sc,
        latrad = lr,
        slorad = sr,
        asprad = ar,
        delta = delta
      )
    })

    return(sum(daily_rad))
  }

  # Apply to all input points (per unit slope-surface area)
  potentialRad <- mapply(calc_annual_raw,
                        lr = lat_rad,
                        sr = slope_rad,
                        ar = aspect_rad,
                        SIMPLIFY = TRUE)

  # Project slope-surface irradiance to horizontal-equivalent
  potentialRad_proj <- potentialRad / cos(slope_rad)

  return(potentialRad_proj)
}
