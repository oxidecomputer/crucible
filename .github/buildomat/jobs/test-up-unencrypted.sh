#!/bin/bash
#:
#: name = "test-up-unencrypted"
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

printenv

date
echo first
ls -ltr /
echo input
ls -ltr /input/
echo build
ls -ltr /input/build/
echo work
ls -ltr /input/build/work
set -o errexit
set -o pipefail
set -o xtrace

echo first bins
ls -ltr /input/build/work/bins || true

banner unpack
mkdir -p /var/tmp/bins
for t in "$input/bins/"*.gz; do
	b=$(basename "$t")
	b=${b%.gz}
	gunzip < "$t" > "/var/tmp/bins/$b"
	chmod +x "/var/tmp/bins/$b"
done

export BINDIR=/var/tmp/bins

banner test_up
ptime -m bash "$input/scripts/test_up.sh" unencrypted

# Save the output files?
