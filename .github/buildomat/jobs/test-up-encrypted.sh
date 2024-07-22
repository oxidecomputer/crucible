#!/bin/bash
#:
#: name = "test-up-encrypted"
#: variety = "basic"
#: target = "helios-2.0"
#: output_rules = [
#:	"%/tmp/test_up/*.txt",
#:	"%/tmp/test_up/dsc/*.txt",
#:	"%/tmp/debug/*",
#:	"/tmp/core.*",
#: ]
#: skip_clone = true
#:
#: [dependencies.build]
#: job = "build"

input="/input/build/work"

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

echo "input bins dir contains:"
ls -ltr "$input"/bins || true

banner unpack
mkdir -p /var/tmp/bins
for t in "$input/bins/"*.gz; do
	b=$(basename "$t")
	b=${b%.gz}
	gunzip < "$t" > "/var/tmp/bins/$b"
	chmod +x "/var/tmp/bins/$b"
done

export BINDIR=/var/tmp/bins

# Give this test two hours to finish
jobpid=$$; (sleep $(( 120 * 60 )); banner fail-timeout; ps -ef; zfs list;kill $jobpid) &

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

banner test_up_encrypted
ptime -m bash "$input/scripts/test_up.sh" -N encrypted

echo "test-up-encrypted ends"
