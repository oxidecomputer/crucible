#!/bin/bash
#:
#: name = "test-perf-fio"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "1.66"
#: output_rules = [
#:  "/tmp/test_perf_fio.txt",
#:  "%/tmp/debug/*.txt",
#: ]
#: skip_clone = true

# it would be nice to re-use the binaries from rbuild. to do that we need a way
# to tell the fio rig to use those binaries instead of building its own.

set -o errexit
set -o pipefail
set -o xtrace

banner setup
pfexec plimit -n 9123456 $$

echo "Setup debug logging"
mkdir /tmp/debug
psrinfo -v > /tmp/debug/psrinfo.txt
df -h > /tmp/debug/df.txt
prstat -d d -mLc 1 > /tmp/debug/prstat.txt 2>&1 &
iostat -T d -xn 1 > /tmp/debug/iostat.txt 2>&1 &
mpstat -T d 1 > /tmp/debug/mpstat.txt 2>&1 &
vmstat -T d -p 1 < /dev/null > /tmp/debug/paging.txt 2>&1 &

echo "Clone fio rig"
# maybe this should clone a specific hash?
git clone https://github.com/oxidecomputer/crucible-fio-rig.git
cd crucible-fio-rig

echo "Build fio rig"
cargo build --release

echo "Add fio job"

printf '%s' '
[global]
iodepth=25
ioengine=aio
time_based
runtime=120
numjobs=1
direct=1
stonewall=1

[randread-4K]
bs=4K
rw=randread

[randread-16K]
bs=16K
rw=randread

[randread-4M]
bs=4M
rw=randread

[randwrite-4K]
bs=4K
rw=randwrite

[randwrite-16K]
bs=16K
rw=randwrite

[randwrite-4M]
bs=4M
rw=randwrite

[randwr-4K]
bs=4K
rw=randrw

[randrw-16K]
bs=16K
rw=randrw

[randrw-4M]
bs=4M
rw=randrw
' > iops.fio

banner start
pfexec ./target/release/run_crucible_fio_test \
    --crucible-commit HEAD \
    --propolis-commit 28be85642e85afd1f47be861f920458beb3514f0 \
    --output-file /tmp/test_perf_fio.txt \
    ./iops.fio

echo "$? was our result"
echo "Test finished"
sleep 5
ps -ef
