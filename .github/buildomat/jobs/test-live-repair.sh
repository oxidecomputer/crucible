#!/bin/bash
#:
#: name = "test-live-repair"
#: variety = "basic"
#: target = "helios"
#: output_rules = [
#:	"/tmp/*.txt",
#: ]
#: skip_clone = true
#:
#: [dependencies.build]
#: job = "build"

input="/input/build/work"

set -o errexit
set -o pipefail
set -o xtrace

pfexec chmod +x "$input"/scripts/* || true

banner unpack
mkdir -p /var/tmp/bins
for t in "$input/bins/"*.gz; do
	b=$(basename "$t")
	b=${b%.gz}
	gunzip < "$t" > "/var/tmp/bins/$b"
	chmod +x "/var/tmp/bins/$b"
done

export BINDIR=/var/tmp/bins

banner repair
ptime -m bash "$input/scripts/test_live_repair.sh"

# Save the output files?
