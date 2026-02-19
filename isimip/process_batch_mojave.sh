#!/bin/bash

#wget -ci pr_historic.txt

region=mojave
lonmin=-116.665  # west
lonmax=-114.445  # east
latmin=34.717    # south (increased to exclude southern row)
latmax=36.091    # north

for variable in tas; do
for scenario in historical ssp126 ssp585; do
for model in gfdl-esm4_r1i1p1f1 ipsl-cm6a-lr_r1i1p1f1 mri-esm2-0_r1i1p1f1 mpi-esm1-2-hr_r1i1p1f1 ukesm1-0-ll_r1i1p1f2; do 
	./process_unit.sh "${variable}" "${scenario}" "${model}" "${region}" "${lonmin}" "${lonmax}" "${latmin}" "${latmax}" &
done
done
wait
done
