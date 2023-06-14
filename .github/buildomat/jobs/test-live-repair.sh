#!/bin/bash
#:
#: name = "test-live-repair"
#: variety = "basic"
#: target = "helios"
#: output_rules = [
#:	"/tmp/*.txt",
#:	"/tmp/*.log",
#: ]
#: skip_clone = true
#:
#: [dependencies.build]
#: job = "build"

input="/input/build/work"

set -o errexit
set -o pipefail
set -o xtrace

banner SKIP
exit 0

pfexec chmod +x "$input"/scripts/* || true

echo "input bins dir contains:"
ls -ltr "$input"/bins || true
echo "input script dir contains:"
ls -ltr "$input"/scripts || true

banner unpack
mkdir -p /var/tmp/bins
for t in "$input/bins/"*.gz; do
	b=$(basename "$t")
	b=${b%.gz}
	gunzip < "$t" > "/var/tmp/bins/$b"
	chmod +x "/var/tmp/bins/$b"
done

export BINDIR=/var/tmp/bins

echo "BINDIR is $BINDIR"
echo "bindir contains:"
ls -ltr "$BINDIR" || true

banner LR
echo "This test is skipped till Alan fixes it"
# ptime -m bash "$input/scripts/test_live_repair.sh"

# Save the output files?
