#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DATA_DIR="$(dirname "$SCRIPT_DIR")/data/"
echo $SCRIPT_DIR
echo $DATA_DIR
if [ ! -d $DATA_DIR ]; then
    mkdir -p $DATA_DIR
fi;
if [ ! -f "${DATA_DIR}/pr_12_months.nc" ]; then
   curl -s -L -o "${DATA_DIR}/pr_12_months.nc" "https://zenodo.org/record/8337510/files/pr_12_months.nc?download=1"
fi
