#!/bin/bash
#:
#: name = "test-replay"
#: variety = "basic"
#: target = "helios-2.0"
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

banner replay
ptime -m bash "$input/scripts/test_replay.sh" "-l 3"
