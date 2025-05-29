#!/bin/bash
#:
#: name = "test-memory"
#: variety = "basic"
#: target = "helios-2.0"
#: output_rules = [
#:  "/tmp/*.txt",
#:  "/tmp/*.log",
#:  "%/tmp/debug/*.txt",
#:  "%/tmp/dsc/*.txt",
#:  "/tmp/core.*",
#: ]
#: skip_clone = true
#:
#: [dependencies.rbuild]
#: job = "rbuild"

input="/input/rbuild/work"

set -o errexit
set -o pipefail
set -o xtrace

banner cores
pfexec coreadm -i /tmp/core.%f.%p \
 -g /tmp/core.%f.%p \
 -e global \
 -e log \
 -e proc-setid \
 -e global-setid

banner unpack
mkdir -p /var/tmp/bins
for t in "$input/rbins/"*.gz; do
	b=$(basename "$t")
	b=${b%.gz}
	gunzip < "$t" > "/var/tmp/bins/$b"
	chmod +x "/var/tmp/bins/$b"
done

export BINDIR=/var/tmp/bins
export RUST_BACKTRACE=1

banner setup
pfexec plimit -n 9123456 $$

echo "Setup self timeout"
# one hour should be enough
jobpid=$$; (sleep 3600; banner fail-timeout; ps -ef; zfs list;kill $jobpid) &

echo "Setup debug logging"
mkdir /tmp/debug
psrinfo -v > /tmp/debug/psrinfo.txt
df -h > /tmp/debug/df.txt || true
prstat -d d -mLc 1 > /tmp/debug/prstat.txt 2>&1 &
iostat -T d -xn 1 > /tmp/debug/iostat.txt 2>&1 &
mpstat -T d 1 > /tmp/debug/mpstat.txt 2>&1 &
vmstat -T d -p 1 < /dev/null > /tmp/debug/paging.txt 2>&1 &
pfexec dtrace -Z -s $input/scripts/perf-downstairs-tick.d > /tmp/debug/perf.txt 2>&1 &
pfexec dtrace -Z -s $input/scripts/upstairs_info.d > /tmp/debug/upinfo.txt 2>&1 &

banner 512-memtest
ptime -m bash $input/scripts/test_mem.sh -b 512 -e 131072 -c 160
banner 4k-memtest
ptime -m bash $input/scripts/test_mem.sh -b 4096 -e 16384 -c 160
