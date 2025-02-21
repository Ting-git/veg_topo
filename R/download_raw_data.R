# -------------------
# Required Packages
# -------------------
library(httr)        # HTTP request handling
library(stringr)     # String manipulation utilities
library(jsonlite)    # JSON parsing and generation
library(purrr)       # Functional programming tools
library(fs)          # Filesystem path operations
library(progress)    # Progress bar implementation
library(dplyr)       # Data manipulation verbs
library(readr)       # File reading/writing utilities

# -------------------
# Configuration
# -------------------
SAVE_DIR <- fs::path_expand("/data_2/vegheight_lang_2023")  # Target directory for downloads
MAX_RETRIES <- 5                     # Maximum download attempts per file
INITIAL_DELAY <- 0.5                 # Base delay between retries (seconds)
JITTER <- 1.5                        # Exponential backoff factor
LOG_FILE <- "download_errors.log"    # Error log filename

# -------------------
# URL Extraction
# -------------------
get_download_urls <- function() {
  # Retrieve HTML content containing file index
  response <- httr::GET("https://libdrive.ethz.ch/index.php/s/cO8or7iOe5dT2Rt/download?path=%2F&files=tile_index.html")
  html_content <- httr::content(response, "text", encoding = "UTF-8")

  # Extract JSON string from HTML using regex pattern matching
  json_str <- stringr::str_match(
    html_content,
    stringr::regex("geo_json_[a-f0-9]+_add\\(\\s*(\\{.*?\\})\\s*\\)", dotall = TRUE)
  )[1, 2]

  # Parse JSON structure to extract download URLs
  geo_data <- jsonlite::fromJSON(json_str, simplifyVector = FALSE)

  # Extract canopy height URLs and select specific range for processing
  purrr::map_chr(geo_data$features, ~.x$properties$href_canopy_height)[2001:2651]  # Process 651 URLs
}

# -------------------
# Core Functions
# -------------------

#' Validate URL structure
#' @param url Character string containing URL to validate
#' @return Logical indicating valid URL format
validate_url <- function(url) {
  stringr::str_detect(url, "^https?://") &&                # Check protocol prefix
    !is.na(stringr::str_extract(url, "(?<=files=)[^&]+"))  # Verify filename parameter exists
}

#' File download handler with retry logic
#' @param url Download URL
#' @param pb Progress bar object reference
#' @return List containing download result metadata
download_file <- function(url, pb) {
  # Generate safe filename from URL parameters
  fname <- stringr::str_extract(url, "(?<=files=)[^&]+") |> fs::path_sanitize()
  dest <- fs::path(SAVE_DIR, fname)

  # Skip existing files and update progress
  if(fs::file_exists(dest)) {
    pb$tick()
    return(list(success = TRUE, skipped = TRUE, file = fname))
  }

  # Retry loop with exponential backoff
  for(attempt in 1:MAX_RETRIES) {
    tryCatch({
      # Calculate dynamic delay with random jitter
      Sys.sleep(INITIAL_DELAY * (attempt^JITTER) * stats::runif(1, 0.8, 1.2))

      # Execute download with progress tracking
      resp <- httr::GET(
        url = url,
        httr::write_disk(dest, overwrite = FALSE),  # Stream to filesystem
        httr::progress(type = "down"),              # Display transfer progress
        httr::timeout(600)                          # 10-minute timeout
      )

      # Validate HTTP response status
      if(httr::status_code(resp) == 200) {
        pb$tick()
        return(list(success = TRUE, file = fname))
      }

      # Cleanup failed download
      fs::file_delete(dest)
    }, error = function(e) {
      # Ensure cleanup on error
      if(fs::file_exists(dest)) fs::file_delete(dest)
    })
  }
  # Return failure after exhausting retries
  list(success = FALSE, error = "Max retries exceeded", file = fname)
}

# -------------------
# Main Execution
# -------------------

# Ensure output directory exists
fs::dir_create(SAVE_DIR)

# Retrieve and validate URLs
urls <- get_download_urls()
valid_urls <- purrr::keep(urls, validate_url)

# Initialize progress bar with custom format
pb <- progress::progress_bar$new(
  format = "[:bar] :percent | ETA: :eta | Current: :current",  # Progress display template
  total = length(valid_urls),   # Total files to process
  clear = FALSE                 # Preserve progress display after completion
)

# Process downloads and handle errors
results <- purrr::map(valid_urls, ~{
  result <- download_file(.x, pb)
  # Log failures with timestamp
  if(!result$success) {
    readr::write_lines(
      sprintf("[%s] %s - %s", Sys.time(), result::file, result$error),
      fs::path(SAVE_DIR, LOG_FILE),
      append = TRUE
    )
  }
  result
})

# Generate summary statistics
success_count <- sum(purrr::map_lgl(results, "success"))
dir_summary <- fs::dir_info(SAVE_DIR) |>
  dplyr::filter(type == "file") |>  # Exclude directories
  dplyr::summarise(
    total_files = dplyr::n(),       # Count downloaded files
    total_size = sum(size)/1024^3  # Convert bytes to GB
  )

# Print formatted report
cat("\nDownload Report:
    Total URLs processed:", length(valid_urls), "
    Successfully downloaded:", success_count, "
    Total data size:", round(dir_summary$total_size, 1), "GB
    Error log:", fs::path(SAVE_DIR, LOG_FILE), "\n")
