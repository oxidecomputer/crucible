#!/bin/bash
#:
#: name = "build"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "1.66"
#: output_rules = [
#:	"/work/bins/*",
#:	"/work/scripts/*",
#:	"/tmp/core.*",
#:	"/tmp/*.log",
#: ]
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

banner cores
pfexec coreadm -i /tmp/core.%f.%p
pfexec coreadm -g /tmp/core.%f.%p
pfexec coreadm -e global
pfexec coreadm -e log
pfexec coreadm -e proc-setid
pfexec coreadm -e global-setid

banner build
ptime -m cargo build --verbose > /tmp/buildout.txt 2>&1

banner output

mkdir -p /work/bins
for t in crucible-downstairs crucible-hammer crutest dsc; do
	gzip < "target/debug/$t" > "/work/bins/$t.gz"
done

mkdir -p /work/scripts
for s in tools/test_live_repair.sh tools/test_repair.sh tools/test_up.sh tools/test_ds.sh; do
	cp "$s" /work/scripts/
done

echo in_work_scripts
ls -l /work/scripts
echo in_work_bins
ls -l /work/bins

banner test
ptime -m cargo test --verbose -- --nocapture > /tmp/fulltest-out.log 2>&1
