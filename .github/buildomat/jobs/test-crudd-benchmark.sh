#!/bin/bash
#:
#: name = "test-crudd-benchmark"
#: variety = "basic"
#: target = "helios-2.0"
#: output_rules = [
#:  "/tmp/crudd-speed-battery-results.json",
#:  "%/tmp/debug/*.txt",
#:  "/tmp/dsc/*.txt",
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

echo "input rbins dir contains:"
ls -ltr "$input"/rbins || true
echo "input scripts dir contains:"
ls -ltr "$input"/scripts || true

banner unpack
mkdir -p /var/tmp/bins
for t in "$input/rbins/"*.gz; do
	b=$(basename "$t")
	b=${b%.gz}
	gunzip < "$t" > "/var/tmp/bins/$b"
	chmod +x "/var/tmp/bins/$b"
done

export BINDIR=/var/tmp/bins

banner setup
pfexec plimit -n 9123456 $$

echo "Setup self timeout"
jobpid=$$; (sleep $(( 2 * 60 * 60 )); ps -ef; zfs list;kill $jobpid) &

echo "Setup debug logging"
mkdir /tmp/debug
psrinfo -v > /tmp/debug/psrinfo.txt
df -h > /tmp/debug/df.txt
prstat -d d -mLc 1 > /tmp/debug/prstat.txt 2>&1 &
iostat -T d -xn 1 > /tmp/debug/iostat.txt 2>&1 &
mpstat -T d 1 > /tmp/debug/mpstat.txt 2>&1 &
vmstat -T d -p 1 < /dev/null > /tmp/debug/paging.txt 2>&1 &
pfexec dtrace -Z -s $input/scripts/perf-downstairs-tick.d > /tmp/debug/dtrace.txt 2>&1 &

banner "crudd bench"
pfexec plimit -n 9123456 $$

ptime -m bash "$input/scripts/crudd-speed-battery.sh"
