# In the same directory as this file, remove
#    1. *.nc files, except for "pr_12_months.nc"
#    2. *.pkl files
#    3. directories
find "$(dirname "$0")"/* -type f -name "*.nc" ! -name "pr_12_months.nc" -delete
find "$(dirname "$0")"/* -type f -name "*.pkl" -delete
find "$(dirname "$0")"/* -type d -exec rm -rf {} +