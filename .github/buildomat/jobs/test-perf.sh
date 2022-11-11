#!/bin/bash
#:
#: name = "test-perf"
#: variety = "basic"
#: target = "helios"
#: output_rules = [
#:  "=/tmp/perf*.csv",
#:  "/tmp/perfout.txt",
#:  "%/tmp/debug/*.txt",
#:  "/tmp/dsc/*.txt",
#: ]
#: skip_clone = true
#:
#: [dependencies.rbuild]
#: job = "rbuild"

input="/input/rbuild/work"

set -o errexit
set -o pipefail
set -o xtrace

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
jobpid=$$; (sleep $(( 40 * 60 )); ps -ef; zfs list;kill $jobpid) &

echo "Setup debug logging"
mkdir /tmp/debug
psrinfo -v > /tmp/debug/psrinfo.txt
df -h > /tmp/debug/df.txt
prstat -d d -mLc 1 > /tmp/debug/prstat.txt 2>&1 &
iostat -T d -xn 1 > /tmp/debug/iostat.txt 2>&1 &
mpstat -T d 1 > /tmp/debug/mpstat.txt 2>&1 &
vmstat -T d -p 1 < /dev/null > /tmp/debug/paging.txt 2>&1 &
dtrace -Z -s $input/scripts/perf-downstairs-tick.d > /tmp/debug/dtrace.txt 2>&1 &

banner start
bash $input/scripts/test_perf.sh > /tmp/debug/test_perf.txt 2>&1
echo "$? was our result"
echo "Test finished"
sleep 5
ps -ef
