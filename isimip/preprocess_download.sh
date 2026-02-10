#!/bin/bash

# Download, crop to Southern US, and delete global files
# Usage: ./preprocess_download.sh <scenario> <variable> <filelist>

scenario=$1
variable=$2
filelist=$3

# Southern US bounding box (CA, AZ, NV, CO, NM, TX, UT)
lonmin=-125.0
lonmax=-93.0
latmin=25.0
latmax=42.0

mkdir -p data/interim

# Create filtered filelist
filtered_list="data/interim/filtered_${scenario}_${variable}.txt"
grep "${scenario}" "${filelist}" | grep "${variable}" > "${filtered_list}"

# Download all files at once
wget -ci "${filtered_list}" -P data/raw/

# Process each downloaded file
for filepath in data/raw/*_w5e5_${scenario}_${variable}_global_daily_*.nc; do
    [ ! -f "${filepath}" ] && continue
    
    filename=$(basename "${filepath}")
    interim_file="data/interim/${filename/_global_/_southus_}"
    
    # Crop to Southern US and delete global file
    cdo sellonlatbox,${lonmin},${lonmax},${latmin},${latmax} "${filepath}" "${interim_file}" && \
    rm "${filepath}"
done

rm "${filtered_list}"
