#!/bin/bash
#
# This script will display the downstairs states for any propolis zones it
# finds running on a system.
for zzz in $(zoneadm list | grep propolis); do
    echo -n "$zzz "
    ppid=$(zlogin "$zzz" pgrep propolis-server)
    dtrace -xstrsize=1k -q -n 'crucible_upstairs*:::up-status /pid==$1/ { printf("%6d %17s %17s %17s", pid, json(copyinstr(arg1), "ok.ds_state[0]"), json(copyinstr(arg1), "ok.ds_state[1]"), json(copyinstr(arg1), "ok.ds_state[2]")); exit(0); }' $ppid
done
