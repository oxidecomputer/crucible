#!/bin/bash
#:
#: name = "rbuild"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "nightly-2021-11-24"
#: output_rules = [
#:	"/work/rbins/*",
#:	"/work/scripts/*",
#: ]
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

banner rbuild
ptime -m cargo build --verbose --release

banner rtest
ptime -m cargo test --verbose

banner output
mkdir -p /work/rbins
for t in crucible-downstairs crucible-client crucible-hammer dsc; do
	gzip < "target/release/$t" > "/work/rbins/$t.gz"
done

mkdir -p /work/scripts
for s in tools/test_up.sh tools/test_ds.sh; do
	cp "$s" /work/scripts/
done

echo in_work_scripts
ls -l /work/scripts
echo in_work_rbins
ls -l /work/rbins

