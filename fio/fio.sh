#!/bin/bash
set -x

#rm -f *.png *.report *.log

if [[ ${#} -lt 2 ]];
then
    echo "supply region file and run name!"
    exit 1
fi

REGION_FILE="${1}"
NAME="${2}"

./generate_crucible_fio.py "${REGION_FILE}" "${NAME}"
sudo fio --output="fio.report" --output-format=normal crucible.fio

./hist.py crucible_"${NAME}"_bw.1.log crucible_"${NAME}"_lat.1.log

./cumulative_points_over_time.py crucible_"${NAME}"_bw.1.log

gnuplot plt.plt

