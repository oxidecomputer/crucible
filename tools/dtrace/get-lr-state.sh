#!/bin/bash
#
# This script will log into every propolis zone it finds and get the
# DTrace live repair counters from propolis-server in each zone.
for zzz in $(zoneadm list | grep propolis); do
    echo -n "$zzz "
    ppid=$(zlogin "$zzz" pgrep propolis-server)
    dtrace -xstrsize=1k -q -n 'crucible_upstairs*:::up-status /pid==$1/ { printf("%6d %s %s %s %s %s %s", pid, json(copyinstr(arg1), "ok.ds_live_repair_completed[0]"), json(copyinstr(arg1), "ok.ds_live_repair_completed[1]"), json(copyinstr(arg1), "ok.ds_live_repair_completed[2]"), json(copyinstr(arg1), "ok.ds_live_repair_aborted[0]"), json(copyinstr(arg1), "ok.ds_live_repair_aborted[1]"), json(copyinstr(arg1), "ok.ds_live_repair_aborted[2]")); exit(0); }' $ppid
done
