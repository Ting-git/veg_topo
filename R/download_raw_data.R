# setup --> Load necessary libraries
library(httr)
library(stringr)
library(jsonlite)
library(purrr)
library(fs)
library(furrr)
library(future)

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
urls <- base::sapply(geo_data$features, function(f) {
  f$properties$href_canopy_height
})

# Take first n URLs for testing
urls <- urls[c(1:10)]

# Create save directory
save_dir <- fs::path.expand("~/data_2/vegheight_lang_2023")
fs::dir_create(save_dir)

# Enhanced download function with retry mechanism 增强版下载函数（带重试机制）
safe_download <- function(url, max_retries = 3) {
  try_num <- 0
  file_name <- stringr::str_extract(url, "(?<=files=)[^&]+")
  dest_path <- base::file.path(save_dir, file_name)

  # Skip existing files
  if(fs::file_exists(dest_path)) {
    base::message(base::sprintf("File already exists: %s", file_name))
    return(TRUE)
  }

  while(try_num < max_retries) {
    base::tryCatch({
      resp <- httr::GET(
        url,
        httr::write_disk(dest_path, overwrite = TRUE),
        httr::progress(type = "down"),
        httr::timeout(300)
      )
      if(httr::status_code(resp) == 200) {
        base::message(base::sprintf("Successfully downloaded: %s (%.2f MB)",
                                    file_name,
                                    fs::file_size(dest_path)/1024/1024))
        return(TRUE)
      }
    }, error = function(e) {
      base::message(base::sprintf("Attempt %d failed: %s", try_num+1, e$message))
    })
    try_num <- try_num + 1
    base::Sys.sleep(2^try_num)
  }
  base::warning(base::sprintf("Download failed: %s", url))
  return(FALSE)
}

# Batch download with parallel processing 批量下载（使用future加速）
future::plan(future::multisession)
results <- urls |>
  purrr::set_names() |>
  furrr::future_map(safe_download, .progress = TRUE)

# Generate download report
success_rate <- base::mean(base::unlist(results))
base::message(base::sprintf("\nDownload completed! Success rate: %.1f%%", success_rate*100))

# Count files in save directory
existing_files <- fs::dir_info(save_dir, type = "file")
base::message(sprintf("Current number of files in save directory: %d", nrow(existing_files)))

# Show failed downloads
failed <- urls[!base::unlist(results)]
if(base::length(failed) > 0) {
  base::message("\nThe following files failed to download:")
  purrr::walk(failed, base::message)
}

