#!/bin/bash
#:
#: name = "test-memory"
#: variety = "basic"
#: target = "helios-2.0"
#: output_rules = [
#:  "/tmp/*.txt",
#:  "%/tmp/dtrace/*",
#:  "/tmp/dsc/*.txt",
#:  "/tmp/core.*",
#: ]
#: skip_clone = true
#:
#: [dependencies.rbuild]
#: job = "rbuild"

input="/input/rbuild/work"

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

banner unpack
mkdir -p /var/tmp/bins
for t in "$input/rbins/"*.gz; do
	b=$(basename "$t")
	b=${b%.gz}
	gunzip < "$t" > "/var/tmp/bins/$b"
	chmod +x "/var/tmp/bins/$b"
done

export BINDIR=/var/tmp/bins

banner setup
pfexec plimit -n 9123456 $$

banner dtrace
ls -l $input/scripts
pfexec dtrace -Z -s $input/scripts/upstairs_info.d > /tmp/dtrace/upstairs-info.txt 2>&1 &

banner start
ptime -m bash $input/scripts/test_mem.sh
