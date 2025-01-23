#!/bin/bash
#:
#: name = "build"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = "1.84.0"
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
pfexec coreadm -i /tmp/core.%f.%p \
 -g /tmp/core.%f.%p \
 -e global \
 -e log \
 -e proc-setid \
 -e global-setid

banner build
ptime -m cargo build --verbose --all-features

banner output

mkdir -p /work/bins
for t in crucible-downstairs crucible-hammer crutest dsc; do
	gzip < "target/debug/$t" > "/work/bins/$t.gz"
done

mkdir -p /work/scripts
for s in tools/test_live_repair.sh tools/test_repair.sh tools/test_up.sh \
  tools/test_ds.sh tools/test_replay.sh tools/dtrace/upstairs_info.d \
  tools/dtrace/perf-downstairs-tick.d; do
	cp "$s" /work/scripts/
done

echo in_work_scripts
ls -l /work/scripts
echo in_work_bins
ls -l /work/bins

banner test
ptime -m cargo test --verbose --features=omicron-build -- --nocapture > /tmp/cargo-test-out.log 2>&1
