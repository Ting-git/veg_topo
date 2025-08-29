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

  # Degrees → radians helper
  deg2rad <- function(d) d * pi / 180

  # Convert to radians
  lat_rad <- deg2rad(lat_deg)
  slope_rad <- deg2rad(slope_deg)
  aspect_rad <- deg2rad(aspect_deg)

  # Daily Julian days using meteoland function
  dates <- seq(as.Date(paste0(year, "-01-01")),
               as.Date(paste0(year, "-12-31")), by="day")

  J <- radiation_dateStringToJulianDays(format(dates, "%Y-%m-%d"))

  # Base solar constant in kW/m²
  S0 <- 1.361
  daily_Rpot <- numeric(length(J))

  # Loop through days
  for (i in seq_along(J)) {
    Sc <- S0 * (1 + 0.033 * cos(2 * pi * (i) / 365))  # orbital correction
    delta <- radiation_solarDeclination(J[i])
    daily_Rpot[i] <- radiation_potentialRadiation(Sc, lat_rad, slope_rad, aspect_rad, delta)
  }

  # return(sum(daily_Rpot, na.rm = TRUE) * 1e6)  # Annual radiation in J/m²
  return(sum(daily_Rpot, na.rm = TRUE))  # Annual radiation in MJ/m²
}

# Example usage:
# compute_annual_radiation(lat_deg = 45, slope_deg = 10, aspect_deg = 180)
