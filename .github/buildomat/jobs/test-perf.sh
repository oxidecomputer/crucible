#!/bin/bash
#:
#: name = "test-perf"
#: variety = "basic"
#: target = "helios"
#: output_rules = [
#:  "/tmp/perf*.csv",
#:  "/tmp/perfout.txt",
#:  "/tmp/debug/*.txt",
#: ]
#: skip_clone = true
#:
#: [dependencies.rbuild]
#: job = "rbuild"

input="/input/rbuild/work"

set -o errexit
set -o pipefail
set -o xtrace

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

banner perf
pfexec plimit -n 9123456 $$

echo "Setup self timeout"
jobpid=$$; (sleep $(( 2 * 60 )); ps -ef; kill $jobpid) &

echo "Setup debug logging"
mkdir /tmp/debug
prstat -d d -mLc 1 </dev/null > /tmp/debug/prstat.txt 2>&1 & disown
# iostat -T d -xn 1 > /tmp/debug/iostat.txt &
# mpstat -T d 1 > /tmp/debug/mpstat.txt &
# vmstat -T d -p 1 >/tmp/debug/paging.txt &

disown -a
echo "Now try with bash prefix"
bash $input/scripts/test_perf.sh > /tmp/debug/test_perf.txt 2>&1
echo "$? was our 2nd result"
echo "Test finished"
ps -ef
exit 0
