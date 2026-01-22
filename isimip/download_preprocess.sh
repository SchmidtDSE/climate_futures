#!/bin/bash

#wget -ci pr_historic.txt

region=jotr
lonmin=-116.555  # west
lonmax=-115.445  # east
latmin=33.655    # south
latmax=34.345    # north

for variable in pr; do
for scenario in historical; do
for model in gfdl-esm4; do
	./process_unit.sh "${variable}" "${scenario}" "${model}" "${region}" "${lonmin}" "${lonmax}" "${latmin}" "${latmax}" &
done
done
wait
done