#!/bin/bash
#:
#: name = "test-perf"
#: variety = "basic"
#: target = "helios"
#: output_rules = [
#:  "/tmp/perf*.csv",
#:  "/tmp/perfout.txt",
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

ptime -m bash "$input/scripts/test_perf.sh"
