# Load the necessary library
library(fs)

# Function to list all .tif files in a folder
get_all_files <- function(directory) {
  # List all files in the directory with the .tif extension
  all_files <- fs::dir_ls(path = directory)

  # Convert to character vector and return
  return(as.character(all_files))
}


