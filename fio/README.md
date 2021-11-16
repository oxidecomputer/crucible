This directory contains some scripts for running fio against crucible through
NBD.

To get crucible-nbd-server running, please follow [this README](../nbd_server/src/README.md).

Run fio.sh with a path to a region.json and a name for the run:

    ./fio.sh ../disks/d1/region.json "before"

gnuplot will run and probably complain unless you have "before" and "after"
run results.

