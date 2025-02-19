# Setup: Load Required Packages with Explicit References
library(httr)        # For HTTP requests
library(stringr)     # For string manipulation
library(jsonlite)    # For JSON parsing
library(purrr)       # For functional programming
library(fs)          # For file system operations
library(furrr)       # For parallel processing
library(future)      # For parallel backend
library(progress)    # For progress bars
library(dplyr)       # For data manipulation
library(readr)       # For error logging

# -------------------
# Configuration Section
# -------------------
SAVE_DIR <- fs::path_expand("~/data_2/vegheight_lang_2023")  # Save location
MAX_WORKERS <- 8                     # Parallel workers
BATCH_SIZE <- 200                    # Files per batch
MAX_RETRIES <- 5                     # Retry attempts
INITIAL_DELAY <- 0.5                 # Base delay in seconds
JITTER <- 1.5                        # Delay randomness factor
LOG_FILE <- "download_errors.log"    # Error log file

# -------------------
# Function Definitions (Fixed)
# -------------------

#' Sanitize and validate download URLs
validate_urls <- function(urls) {
  valid <- purrr::map_lgl(urls, function(u) {
    stringr::str_detect(u, "^https?://") &&
      !is.na(stringr::str_extract(u, "(?<=files=)[^&]+"))
  })
  if(sum(!valid) > 0) {
    msg <- sprintf("Found %d invalid URLs:\n%s",
                   sum(!valid),
                   paste(urls[!valid], collapse = "\n"))
    warning(msg)
  }
  urls[valid]
}

#' Enhanced download function with exponential backoff
safe_download <- function(url, attempt = 1) {
  tryCatch({
    # Generate safe filename
    fname <- stringr::str_extract(url, "(?<=files=)[^&]+") |>
      fs::path_sanitize()

    dest <- fs::path(SAVE_DIR, fname)

    # Skip existing files
    if(fs::file_exists(dest)) {
      return(list(success = TRUE, skipped = TRUE, file = fname))
    }

    # Add randomized delay
    Sys.sleep(INITIAL_DELAY * (attempt^JITTER) * runif(1, 0.8, 1.2))

    resp <- httr::GET(
      url = url,
      httr::write_disk(dest, overwrite = FALSE),
      httr::progress(type = "down"),
      httr::timeout(600)  # 10-minute timeout
    )

    if(httr::status_code(resp) == 200) {
      return(list(success = TRUE, skipped = FALSE, file = fname))
    } else {
      fs::file_delete(dest)  # Remove partial download
      return(list(success = FALSE, error = sprintf("HTTP %d", httr::status_code(resp))))
    }
  }, error = function(e) {
    return(list(success = FALSE, error = conditionMessage(e)))
  })
}

# -------------------
# URLs Extraction
# -------------------
# Download and parse data
url <- "https://libdrive.ethz.ch/index.php/s/cO8or7iOe5dT2Rt/download?path=%2F&files=tile_index.html"
response <- httr::GET(url)
html_content <- httr::content(response, "text", encoding = "UTF-8")

# Extract JSON
pattern <- "geo_json_[a-f0-9]+_add\\(\\s*(\\{.*?\\})\\s*\\)"
matches <- stringr::str_match(html_content, stringr::regex(pattern, dotall = TRUE))
json_str <- matches[1, 2]

# Parse JSON without simplification
geo_data <- jsonlite::fromJSON(json_str, simplifyVector = FALSE)

# Extract URLs
urls <- purrr::map_chr(geo_data$features, ~.x$properties$href_canopy_height)

# Take first n URLs for testing
urls <- urls[1:10]  # Test with first 10 URLs

# -------------------
# Main Execution (Fixed)
# -------------------
# 1. Prepare Environment
fs::dir_create(SAVE_DIR)
start_time <- Sys.time()

# 2. Validate URLs
valid_urls <- validate_urls(urls)

# 3. Disk Space Check
required_space <- 651 * 1.1  # 10% buffer (GB)
available_space <- fs::disk_free(SAVE_DIR) / (1024^3)
if(available_space < required_space) {
  stop(sprintf("Insufficient disk space. Need: %.1fGB, Available: %.1fGB",
               required_space, available_space))
}

# 4. Batch Processing Setup
batches <- split(valid_urls, ceiling(seq_along(valid_urls)/BATCH_SIZE))
pb <- progress::progress_bar$new(
  format = "[:bar] :percent | Batch :current/:total | ETA: :eta",
  total = length(valid_urls),
  clear = FALSE
)

# 5. Parallel Download Execution
future::plan(future::multisession, workers = MAX_WORKERS)

results <- purrr::map(batches, function(batch) {
  batch_results <- furrr::future_map(batch, function(url) {
    result <- NULL
    for(attempt in 1:MAX_RETRIES) {
      result <- safe_download(url, attempt)
      if(result$success) break
    }
    pb$tick()
    result
  }, .options = furrr::furrr_options(seed = TRUE))

  # Error logging
  errors <- purrr::keep(batch_results, ~!.$success)
  if(length(errors) > 0) {
    error_log <- purrr::map_chr(errors, ~sprintf("[%s] %s", .$file, .$error))
    readr::write_lines(error_log, fs::path(SAVE_DIR, LOG_FILE), append = TRUE)
  }

  batch_results
})

# 6. Generate Final Report
total_time <- difftime(Sys.time(), start_time, units = "hours")
success_count <- sum(purrr::map_lgl(unlist(results), "success"))

dir_summary <- fs::dir_info(SAVE_DIR) |>
  dplyr::filter(type == "file") |>
  dplyr::summarise(
    total_files = dplyr::n(),
    total_size = sum(size, na.rm = TRUE) / (1024^3)
  )

cat("\n================ Download Report ================\n")
cat(sprintf("Total URLs processed:    %d\n", length(valid_urls)))
cat(sprintf("Successfully downloaded: %d (%.1f%%)\n",
            success_count, (success_count/length(valid_urls))*100))
cat(sprintf("Total data downloaded:   %.1f GB\n", dir_summary$total_size))
cat(sprintf("Time elapsed:            %.1f hours\n", as.numeric(total_time)))
cat(sprintf("Average speed:           %.1f MB/s\n",
            (dir_summary$total_size * 1024) / as.numeric(total_time)))
cat(sprintf("Error log saved to:      %s/%s\n", SAVE_DIR, LOG_FILE))
cat("================================================\n")

# 7. Cleanup
future::plan(future::sequential)
rm(list = ls())
gc()
