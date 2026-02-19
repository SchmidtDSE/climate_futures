#!/bin/bash

#wget -ci pr_historic.txt

region=jotr
lonmin=-116.555  # west
lonmax=-115.445  # east
latmin=33.655    # south
latmax=34.345    # north

for variable in tas; do
for scenario in historical ssp126 ssp585; do
for model in  ukesm1-0-ll_r1i1p1f2 gfdl-esm4_r1i1p1f1 ipsl-cm6a-lr_r1i1p1f1 mri-esm2-0_r1i1p1f1 mpi-esm1-2-hr_r1i1p1f1; do 
	./process_unit.sh "${variable}" "${scenario}" "${model}" "${region}" "${lonmin}" "${lonmax}" "${latmin}" "${latmax}" &
done
done
wait
done