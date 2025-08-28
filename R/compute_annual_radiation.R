#' Compute annual potential shortwave radiation for a single point
#'
#' @param lat_deg Latitude in degrees
#' @param lon_deg Longitude in degrees (optional, for completeness)
#' @param slope_deg Slope in degrees
#' @param aspect_deg Aspect in degrees
#' @param year Numeric year (default 2020)
#'
#' @return Annual potential radiation in J/m²
compute_annual_radiation <- function(lat_deg, lon_deg=NULL, slope_deg, aspect_deg, year=2020) {
  library(meteoland)

  # Degrees → radians helper inside the function
  deg2rad <- function(d) d * pi / 180

  # Convert to radians
  lat_rad <- deg2rad(lat_deg)
  slope_rad <- deg2rad(slope_deg)
  aspect_rad <- deg2rad(aspect_deg)

  # Daily dates
  dates <- seq(as.Date(paste0(year, "-01-01")),
               as.Date(paste0(year, "-12-31")), by="day")
  DOY <- as.numeric(format(dates, "%j"))

  # Base solar constant
  S0 <- 1361
  daily_Rpot <- numeric(length(DOY))

  # Loop through days
  for (i in seq_along(DOY)) {
    Sc <- S0 * (1 + 0.033 * cos(2 * pi * DOY[i] / 365))  # orbital correction
    delta <- radiation_solarDeclination(DOY[i])
    daily_Rpot[i] <- radiation_potentialRadiation(Sc, lat_rad, slope_rad, aspect_rad, delta)
  }

  # Annual radiation in J/m²
  return(sum(daily_Rpot, na.rm = TRUE) * 1e6)
}


# Example usage:
# compute_annual_radiation(lat_deg = 45, slope_deg = 10, aspect_deg = 180)
