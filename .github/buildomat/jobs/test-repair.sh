#!/bin/bash
#:
#: name = "test-up-repair"
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

echo "input bins dir contains:"
ls -ltr "$input"/bins || true
echo "input script dir contains:"
ls -ltr "$input"/scripts || true
pfexec chmod +x "$input"/scripts/* || true
echo " chmod input script dir contains:"
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
export SCRIPTDIR="$input"/scripts

banner repair
ptime -m bash "$input/scripts/test_repair.sh"

# Save the output files?
