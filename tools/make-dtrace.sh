#!/bin/bash
#
# Build a tar archive with selected DTrace scripts.
# This archive is used to install DTrace scripts on the global zone of each
# sled.  As such, the scripts here should match the probes that exist for
# any consumer of the upstairs, like propolis, the pantry, or crucible agent.
set -eux

rm -f out/crucible-dtrace.tar 2> /dev/null

mkdir -p out

echo "$(date) Create DTrace archive on $(hostname)" > /tmp/dtrace-info.txt
echo "git log -1:" >> dtrace-info.txt
git log -1 >> dtrace-info.txt
echo "git status:" >> dtrace-info.txt
git status >> dtrace-info.txt
mv dtrace-info.txt tools/dtrace

pushd tools/dtrace
tar cvf ../../out/crucible-dtrace.tar \
    dtrace-info.txt \
    README.md \
    all_downstairs.d \
    downstairs_count.d \
    get-ds-state.d \
    get-ds-state.sh \
    get-lr-state.d \
    get-lr-state.sh \
    perf-downstairs-os.d \
    perf-downstairs-three.d \
    perf-downstairs-tick.d \
    perf-downstairs.d \
    perf-ds-client.d \
    perf-ds-net.d \
    perf-online-repair.d \
    perf-reqwest.d \
    perf-upstairs-wf.d \
    perf-vol.d \
    perfgw.d \
    single_up_info.d \
    sled_upstairs_info.d \
    trace-vol.d \
    tracegw.d \
    upstairs_action.d \
    upstairs_count.d \
    upstairs_info.d \
    upstairs_raw.d \
    upstairs_repair.d

rm dtrace-info.txt
popd
ls -l out/crucible-dtrace.tar
