#!/bin/bash
#:
#: name = "build"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "1.66"
#: output_rules = [
#:	"/work/bins/*",
#:	"/work/scripts/*",
#: ]
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

banner build
ptime -m cargo build --verbose > /tmp/buildout.txt

banner LRtest
ptime -m cargo test -p crucible --lib test_successful_live_repair --verbose -- --nocapture

banner 2LRtest
ptime -m cargo test -p crucible --lib test_successful_live_repair --verbose -- --nocapture

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
ptime -m cargo test --verbose

