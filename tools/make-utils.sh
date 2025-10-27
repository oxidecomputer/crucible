#!/bin/bash
#
# Build a tar archive with selected DTrace scripts and other utilities
# deemed useful to contain on the sled.
# This archive is used to install DTrace scripts on the global zone of each
# sled.  As such, the scripts here should match the probes that exist for
# any consumer of the upstairs, like propolis, the pantry, or crucible agent.
set -eux

rm -f out/crucible-utils.tar 2> /dev/null

mkdir -p out


echo "$(date) Create tools archive on $(hostname)" > utils-info.txt
echo "git log -1:" >> utils-info.txt
git log -1 >> utils-info.txt
echo "git status:" >> utils-info.txt
git status >> utils-info.txt
mv utils-info.txt tools/dtrace

# Add all the DTrace scripts and tools
pushd tools/dtrace
tar cvf ../../out/crucible-utils.tar \
    utils-info.txt \
    README.md \
    all_downstairs.d \
    downstairs_count.d \
    get-ds-state.d \
    get-ds-state.sh \
    get-lr-state.d \
    get-lr-state.sh \
    get-up-state.d \
    get-up-state.sh \
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
    up-info.d \
    upstairs_action.d \
    upstairs_count.d \
    upstairs_info.d \
    upstairs_raw.d \
    upstairs_repair.d

rm utils-info.txt
popd

# Add crucible-verify-raw
pushd target/release
tar -rf ../../out/crucible-utils.tar \
    crucible-verify-raw
popd

ls -l out/crucible-utils.tar
