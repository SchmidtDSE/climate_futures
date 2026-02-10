#!/bin/bash

# Crop CMIP6 files to Southern US and delete global files
# Usage: ./preprocess_download.sh <scenario> <variable>

scenario=$1
variable=$2

# Southern US bounding box (CA, AZ, NV, CO, NM, TX, UT)
lonmin=-125.0
lonmax=-93.0
latmin=25.0
latmax=42.0

mkdir -p data/interim

# Process each downloaded file
# Pattern: variable_day_model_scenario_ensemble_grid_daterange.nc
for filepath in data/raw/${variable}_day_*_${scenario}_*.nc; do
    [ ! -f "${filepath}" ] && continue
    
    filename=$(basename "${filepath}")
    # Replace 'global' or 'gn' grid identifier with 'southus'
    # Handle both possible grid identifiers (gn, gr, etc.)
    interim_file="data/interim/${filename/_gn_/_southus_}"
    interim_file="${interim_file/_gr_/_southus_}"
    interim_file="${interim_file/_global_/_southus_}"
    
    # Crop to Southern US and delete global file
    cdo sellonlatbox,${lonmin},${lonmax},${latmin},${latmax} "${filepath}" "${interim_file}" && \
    rm "${filepath}"
done
