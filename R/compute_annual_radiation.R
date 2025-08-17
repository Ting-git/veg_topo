#' Compute annual shortwave radiation for a single point
#'
#' @param lat_deg Latitude in degrees
#' @param lon_deg Longitude in degrees (optional, only for completeness)
#' @param slope_deg Slope in degrees
#' @param aspect_deg Aspect in degrees
#' @param year Numeric year (default 2020)
#'
#' @return annual radiation in J/m2
compute_annual_radiation <- function(lat_deg, lon_deg=NULL, slope_deg, aspect_deg, year=2020) {
  library(meteoland)

  deg2rad <- function(d) d * pi / 180
  lat_rad <- deg2rad(lat_deg)
  slorad <- deg2rad(slope_deg)
  asprad <- deg2rad(aspect_deg)

  # Daily dates for the year
  dates <- seq(as.Date(paste0(year, "-01-01")),
               as.Date(paste0(year, "-12-31")), by="day")

  # Julian day
  J <- radiation_dateStringToJulianDays(format(dates, "%Y-%m-%d"))

  # --- Daily loop ---
  daily_Rpot <- numeric(length(J))
  for (i in seq_along(J)) {
    delta <- radiation_solarDeclination(J[i])
    Sc    <- radiation_solarConstant(J[i])
    daily_Rpot[i] <- radiation_potentialRadiation(Sc, lat_rad, slorad, asprad, delta)
  }

  # Summarize annual radiation (only J/m2)
  annual_J_m2 <- sum(daily_Rpot, na.rm=TRUE) * 1e6  # MJ/m2 -> J/m2

  return(annual_J_m2)
}


# Example usage:
# compute_annual_radiation(lat_deg = 45, slope_deg = 10, aspect_deg = 180)
