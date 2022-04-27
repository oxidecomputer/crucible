#!/bin/bash
#:
#: name = "build"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "nightly"
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
for t in downstairs client hammer; do
	f="crucible-$t"
	gzip < "target/debug/$f" > "/work/bins/$f.gz"
done

mkdir -p /work/scripts
for s in tools/test_up.sh; do
	cp "$s" /work/scripts/
done

echo in_work_scripts
ls -l /work/scripts
echo in_work_bins
ls -l /work/bins

exit 0

#banner test
#ptime -m cargo test --lib --verbose

#banner test_up.sh
#ptime -m ./tools/test_up.sh unencrypted
#ptime -m ./tools/test_up.sh encrypted
#
#banner test_ds.sh
#ptime -m ./tools/test_ds.sh
