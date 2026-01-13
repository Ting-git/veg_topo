# ------------------ Parallel Wrapper Function ------------------
#' Compute Windowed Correlation Statistics in Parallel
#'
#' This function computes correlation statistics (Pearson correlation, p-value,
#' observation count, and optional peak calculation) for nested spatial windows
#' using parallel processing via `multidplyr`.
#'
#' @param df_win A data frame containing spatial windows, typically produced by `create_spatial_windows()`.
#' @param x Character. Name of the first variable. Default is "twi".
#' @param y Character. Name of the second variable. Default is "vegh".
#' @param total_obs Numeric. Expected total observations per window. Default is 144.
#' @param if_nobs Logical. Include the number of observations? Default is TRUE.
#' @param if_pval Logical. Include p-values? Default is TRUE.
#' @param if_peak Logical. Calculate peak relationships? Default is FALSE.
#' @param n_workers Numeric. Number of parallel workers. Default is 4.
#' @return A data frame with correlation statistics for each window.
parallel_window_correlation <- function(df_win,
                                        x = "twi",
                                        y = "vegh",
                                        # total_obs = 144,
                                        if_nobs = TRUE,
                                        if_pval = TRUE,
                                        if_peak = FALSE,
                                        n_workers = 4) {

  # Load required libraries
  # library(multidplyr)
  # library(dplyr)
  # library(purrr)

  # Create a cluster and copy necessary functions
  cl <- new_cluster(n_workers)
  cluster_library(cl, c("dplyr", "purrr", "tidyr", "segmented"))
  cluster_copy(cl, c("win_correlation_stats", "identify_peak"))

  # Partition the data by window groups
  df_part <- df_win |>
    group_by(lon_mid, lat_mid) |>
    nest(data = c(all_of(x), all_of(y))) |>
    partition(cluster = cl)

  # Compute correlation statistics in parallel
  df_cor <- df_part |>
    mutate(stats = map(
      data,
      ~ win_correlation_stats(
        .x,
        x = x,
        y = y,
        # total_obs = total_obs,
        if_nobs = if_nobs,
        if_pval = if_pval,
        if_peak = if_peak
      )
    )) |>
    dplyr::collect() |>
    mutate(
      correlation = map_dbl(stats, "correlation"),
      n_obs = if (if_nobs) map_int(stats, "n_obs") else NA_integer_,
      cor_pval = if (if_pval) map_dbl(stats, "cor_pval") else NA_real_,
      peak = if (if_peak) map_dbl(stats, "peak") else NA_real_
    )

  message(colnames(df_cor))

  df_cor <- df_cor |>
    select(-any_of(c("stats", "data")))  |>
    ungroup()

  return(df_cor)
}


# ------------------ Base Function ------------------
#' Compute Correlation Statistics for a Single Window
#'
#' This helper function computes correlation statistics for a single spatial window.
#' It returns a list containing the correlation coefficient, p-value, number of
#' observations, and optionally a peak value.
#'
#' @param df A data frame for a single spatial window.
#' @param x Character. Name of the first variable. Default is "twi".
#' @param y Character. Name of the second variable. Default is "vegh".
#' @param total_obs Numeric. Expected total observations per window. Default is 144.
#' @param if_nobs Logical. Include the number of observations? Default is TRUE.
#' @param if_pval Logical. Include p-value? Default is TRUE.
#' @param if_peak Logical. Calculate peak relationship? Default is FALSE.
#' @return A list containing correlation statistics.
win_correlation_stats <- function(df,
                                  x = "twi",
                                  y = "vegh",
                                  # total_obs = 144,
                                  if_nobs = TRUE,
                                  if_pval = TRUE,
                                  if_peak = FALSE) {

  n_obs <- nrow(df)
  result <- list(
    correlation = NA_real_,
    n_obs = if (if_nobs) n_obs else NULL,
    cor_pval = if (if_pval) NA_real_ else NULL,
    peak = if (if_peak) NA_real_ else NULL
  )

  # Only compute correlation if there is sufficient data and non-zero variance
  if (n_obs >= 30 && sd(df[[x]], na.rm = TRUE) > 0 && sd(df[[y]], na.rm = TRUE) > 0) {
    test <- cor.test(df[[x]], df[[y]])
    result$correlation <- test$estimate
    if (if_pval) result$cor_pval <- test$p.value
    if (if_peak) result$peak <- identify_peak(df)
  }

  return(result[!sapply(result, is.null)])
}



# ------------------ Helper Function ------------------
# Function to identify a peak (breakpoint) in non-monotonic relationship
identify_peak <- function(df, y_var = "vegh", x_var = "twi", min_obs = 30, p_threshold = 0.05) {
  # Fit linear model
  linmod <- lm(as.formula(paste(y_var, "~", x_var)), data = df)

  # Try segmented model
  segmod <- tryCatch(
    segmented::segmented(linmod, seg.Z = as.formula(paste("~", x_var)), npsi = 1, silent = TRUE),
    error = function(e) return(NULL)
  )

  # If segmentation fails, return NA
  if (is.null(segmod)) return(NA_real_)

  # Extract breakpoint
  bp <- segmod$psi[2]

  # Split data by breakpoint
  df1 <- df[df[[x_var]] <= bp, ]
  df2 <- df[df[[x_var]] > bp, ]

  # Compute slopes to check for peak
  coefs <- coef(segmod)
  slope1 <- coefs[[x_var]]
  slope2 <- slope1 + coefs[[paste0("U1.", x_var)]]

  # Peak exists only if slope changes from positive to negative
  if (!(slope1 > 0 && slope2 < 0)) return(NA_real_)

  # Compute Pearson correlations and p-values for segments if enough data
  safe_cor <- function(x, y) {
    if (length(x) >= min_obs) {
      test <- cor.test(x, y)
      return(list(r = as.numeric(test$estimate), p = test$p.value))
    } else {
      return(list(r = NA_real_, p = NA_real_))
    }
  }

  cor1 <- safe_cor(df1[[x_var]], df1[[y_var]])
  cor2 <- safe_cor(df2[[x_var]], df2[[y_var]])

  # Keep only correlations that are statistically significant
  valid <- c(
    if (!is.na(cor1$p) && cor1$p < p_threshold) cor1$r else NA_real_,
    if (!is.na(cor2$p) && cor2$p < p_threshold) cor2$r else NA_real_
  )

  # Return the correlation from the segment with more observations (most reliable)
  n_seg <- c(nrow(df1), nrow(df2))
  if (all(is.na(valid))) return(NA_real_)

  idx <- which(!is.na(valid))
  if (length(idx) == 1) return(valid[idx])

  # If both segments are significant, pick the one with more observations
  return(valid[idx[which.max(n_seg[idx])]])
}

