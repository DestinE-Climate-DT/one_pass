#!/bin/bash

if [ ! -f tests/pr_12_months.nc ]; then
   curl -s -L -o tests/pr_12_months.nc "https://zenodo.org/record/8337510/files/pr_12_months.nc?download=1"
fi
