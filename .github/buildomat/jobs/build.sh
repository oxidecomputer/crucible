#!/bin/bash
#:
#: name = "build"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "nightly-2021-11-24"
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
ptime -m cargo build --verbose

banner output
mkdir -p /work/bins
for t in crucible-downstairs crucible-client crucible-hammer dsc; do
	gzip < "target/debug/$t" > "/work/bins/$t.gz"
done

mkdir -p /work/scripts
for s in tools/test_up.sh tools/test_ds.sh; do
	cp "$s" /work/scripts/
done

echo in_work_scripts
ls -l /work/scripts
echo in_work_bins
ls -l /work/bins

banner test
ptime -m cargo test --lib --verbose
