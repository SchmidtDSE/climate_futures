#!/bin/bash

variable=$1
scenario=$2
model=$3
region=$4
lonmin=$5
lonmax=$6
latmin=$7
latmax=$8

if [ "$scenario" == "historical" ]; then
    decades="1850_1850 1851_1860 1861_1870 1871_1880 1881_1890 1891_1900 1901_1910 1911_1920 1921_1930 1931_1940 1941_1950 1951_1960 1961_1970 1971_1980 1981_1990 1991_2000 2001_2010 2011_2014"
else
    decades="2015_2020 2021_2030 2031_2040 2041_2050 2051_2060 2061_2070 2071_2080 2081_2090 2091_2100"
fi

for decade in $decades
do
    cdo sellonlatbox,${lonmin},${lonmax},${latmin},${latmax} \
        -monmean \
        data/interim/${model}_w5e5_${scenario}_${variable}_southus_daily_${decade}.nc \
        ${model}_w5e5_${scenario}_${variable}_${region}_monthly_${decade}.nc
done
 
cdo mergetime ${model}_w5e5_${scenario}_${variable}_${region}_monthly_*.nc \
    data/processed/${model}_w5e5_${scenario}_${variable}_${region}_monthly.nc

# Clean up intermediate files
rm ${model}_w5e5_${scenario}_${variable}_${region}_monthly_[0-9]*.nc