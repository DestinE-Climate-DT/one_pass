#!/bin/bash
# Download test data to the repository's data directory

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DATA_DIR="$(dirname "$SCRIPT_DIR")/data"

echo "Data directory: $DATA_DIR"

if [ ! -d "$DATA_DIR" ]; then
    mkdir -p "$DATA_DIR"
fi

if [ ! -f "${DATA_DIR}/pr_12_months.nc" ]; then
    echo "Downloading pr_12_months.nc..."
    curl -s -L -o "${DATA_DIR}/pr_12_months.nc" "https://zenodo.org/record/8337510/files/pr_12_months.nc?download=1"
    echo "Download complete!"
else
    echo "Test data already exists."
fi
