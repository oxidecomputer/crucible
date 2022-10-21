#!/bin/bash
#:
#: name = "rbuild"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "nightly-2021-11-24"
#: output_rules = [
#:	"/out/*",
#:	"/work/rbins/*",
#:	"/work/scripts/*",
#: ]
#:
#: [[publish]]
#: series = "image"
#: name = "crucible.tar.gz"
#: from_output = "/out/crucible.tar.gz"
#:
#: [[publish]]
#: series = "image"
#: name = "crucible.sha256.txt"
#: from_output = "/out/crucible.sha256.txt"
#:
#: [[publish]]
#: series = "image"
#: name = "crucible-pantry.tar.gz"
#: from_output = "/out/crucible-pantry.tar.gz"
#:
#: [[publish]]
#: series = "image"
#: name = "crucible-pantry.sha256.txt"
#: from_output = "/out/crucible-pantry.sha256.txt"
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

banner rbuild
ptime -m cargo build --verbose --release --all-features

banner rtest
ptime -m cargo test --verbose

banner output
mkdir -p /work/rbins
for t in crucible-downstairs crucible-hammer crutest dsc; do
	gzip < "target/release/$t" > "/work/rbins/$t.gz"
done

mkdir -p /work/scripts
for s in tools/test_perf.sh; do
	cp "$s" /work/scripts/
done

banner image
ptime -m cargo run --bin crucible-package

banner contents
tar tvfz out/crucible.tar.gz
tar tvfz out/crucible-pantry.tar.gz

banner copy
pfexec mkdir -p /out
pfexec chown "$UID" /out
mv out/crucible.tar.gz out/crucible-pantry.tar.gz /out/

cd /out
digest -a sha256 crucible.tar.gz > crucible.sha256.txt
digest -a sha256 crucible-pantry.tar.gz > crucible-pantry.sha256.txt
