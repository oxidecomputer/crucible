#!/bin/bash
set -x

rm -f *.png *.report *.log

# TODO: crucible.fio is hard coded with 1000 blocks of size 512
sudo fio --output="fio.report" --output-format=normal crucible.fio

./hist.py crucible_bw.1.log crucible_lat.1.log

gnuplot plt.plt

