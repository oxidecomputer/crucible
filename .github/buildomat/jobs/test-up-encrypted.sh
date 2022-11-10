#!/bin/bash
#:
#: name = "test-up-encrypted"
#: variety = "basic"
#: target = "helios"
#: output_rules = [
#:	"/tmp/test_up/*.txt",
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

banner unpack
mkdir -p /var/tmp/bins
for t in "$input/bins/"*.gz; do
	b=$(basename "$t")
	b=${b%.gz}
	gunzip < "$t" > "/var/tmp/bins/$b"
	chmod +x "/var/tmp/bins/$b"
done

export BINDIR=/var/tmp/bins

banner test_up_encrypted
ptime -m bash "$input/scripts/test_up.sh" -N encrypted

# Save the output files?
