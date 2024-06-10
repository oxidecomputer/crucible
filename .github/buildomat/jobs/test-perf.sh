#!/bin/bash
#:
#: name = "test-perf"
#: variety = "basic"
#: target = "helios-2.0"
#: output_rules = [
#:  "=/tmp/perf*.csv",
#:  "/tmp/perfout.txt",
#:  "/tmp/upstairs-info.txt",
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
jobpid=$$; (sleep $(( 40 * 60 )); banner fail-timeout; ps -ef; zfs list;kill $jobpid) &

echo "Setup debug logging"
mkdir /tmp/debug
psrinfo -v > /tmp/debug/psrinfo.txt
df -h > /tmp/debug/df.txt || true
prstat -d d -mLc 1 > /tmp/debug/prstat.txt 2>&1 &
iostat -T d -xn 1 > /tmp/debug/iostat.txt 2>&1 &
mpstat -T d 1 > /tmp/debug/mpstat.txt 2>&1 &
vmstat -T d -p 1 < /dev/null > /tmp/debug/paging.txt 2>&1 &
pfexec dtrace -Z -s $input/scripts/perf-downstairs-tick.d > /tmp/debug/dtrace.txt 2>&1 &
pfexec dtrace -Z -s $input/scripts/upstairs_info.d > /tmp/debug/upstairs-info.txt 2>&1 &

banner start
bash $input/scripts/test_perf.sh > /tmp/debug/test_perf.txt 2>&1
echo "$? was our result"
echo "Test finished"
