# library(reticulate)
#
# use_python("/usr/bin/python3")
# py_install("cdsapi")
# py_install("attrs")
# use_virtualenv("~/.virtualenvs/r-reticulate")
# py_run_string("import attrs; print(attrs.__version__)")
# py_config()

# Setup
library(reticulate)
use_virtualenv("~/.virtualenvs/r-reticulate")
py_config()
py_run_string("import attrs; print(attrs.__version__)")

# downloading
py_run_file("/home/ting/veg_topo/src/download_cci_landcover_2020.py")
