save_combined_plot_byregion <- function(
    plots,
    region_name,
    title_text,
    ncol = 3,
    output_dir = here::here("data", "figures"),
    width = 20,
    height = 13,
    dpi = 300,
    file_index = ""
) {

  valid_plots <- keep(plots, ~ inherits(.x, "ggplot"))

  # Create the full title by combining region name and title text
  title_text_full <- paste0(region_name, " ", title_text)

  # Combine the plots with the title on top
  combined_plot <- cowplot::plot_grid(
    cowplot::ggdraw() + cowplot::draw_label(title_text_full, fontface = "bold", size = 20, x = 0, hjust = 0),
    cowplot::plot_grid(plotlist = valid_plots, ncol = ncol, align = "hv"),
    ncol = 1,
    rel_heights = c(0.05, 1)
  ) +
    theme(plot.background = element_rect(fill = "white", color = "white"))

  # Construct the output file path
  output_file <- file.path(
    output_dir,
    paste0(
      file_index,
      "_",
      normalize_string(region_name),
      "_",
      normalize_string(title_text),
      ".png"
    )
  )

  # Save the combined plot to a file
  ggplot2::ggsave(
    filename = output_file,
    plot = combined_plot,
    width = width,
    height = height,
    dpi = dpi,
    bg = "white"
  )

  message("✅ Plot saved to: ", output_file)
  return(output_file)
}
