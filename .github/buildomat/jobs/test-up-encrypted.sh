#!/bin/bash
#:
#: name = "test-up-encrypted"
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

banner unpack
echo input
ls -ltr "/input"
echo input/build
ls -ltr "/input/build/"
echo dollar_input
ls -ltr "$input"
echo bins
ls -ltr "$input/bins"
mkdir -p /var/tmp/bins
for t in "$input/bins/"*.gz; do
	b=$(basename "$t")
	b=${b%.gz}
	gunzip < "$t" > "/var/tmp/bins/$b"
	chmod +x "/var/tmp/bins/$b"
done

export BINDIR=/var/tmp/bins

banner test_up
ptime -m bash "$input/scripts/test_up.sh" encrypted
