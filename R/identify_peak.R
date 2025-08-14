# Function to identify a peak (breakpoint) in non-monotonic relationship
identify_peak <- function(df) {
  # Fit a linear model: vegh as a function of twi
  linmod <- lm(vegh ~ twi, data = df)

  # Safely try to fit a segmented (piecewise) regression model
  segmod <- tryCatch(
    segmented::segmented(linmod, seg.Z = ~ twi, npsi = 1, silent = TRUE),
    error = function(e) return(NULL)  # Return NULL if model fitting fails
  )

  # If the segmented model fitting fails, return NA
  if (is.null(segmod)) return(NA)

  # Extract coefficients from the segmented model
  coefs <- coef(segmod)

  # Ensure the necessary coefficients exist
  if (!all(c("twi", "U1.twi") %in% names(coefs))) return(NA)

  # Calculate slope before and after the breakpoint
  slope1 <- coefs[["twi"]]                     # Slope before breakpoint
  slope2 <- coefs[["twi"]] + coefs[["U1.twi"]] # Slope after breakpoint

  # Return TRUE if peak exists (slope changes from positive to negative)
  return(slope1 > 0 && slope2 < 0)
}

normalize_string <- function(x) {
  x <- tolower(x)
  x <- gsub(" ", "_", x)
  return(x)
}
