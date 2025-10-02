#!/bin/bash
#:
#: name = "rbuild"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = "1.84.0"
#: output_rules = [
#:	"/out/*",
#:	"/work/rbins/*",
#:	"/work/scripts/*",
#:	"/tmp/core.*",
#:	"/tmp/*.log",
#: ]
#:
#: [[publish]]
#: series = "nightly-image"
#: name = "crucible-nightly.tar.gz"
#: from_output = "/out/crucible-nightly.tar.gz"
#:
#: [[publish]]
#: series = "nightly-image"
#: name = "crucible-nightly.sha256.txt"
#: from_output = "/out/crucible-nightly.sha256.txt"
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
#: [[publish]]
#: series = "image"
#: name = "crucible-dtrace.tar"
#: from_output = "/out/crucible-dtrace.tar"
#:
#: [[publish]]
#: series = "image"
#: name = "crucible-dtrace.sha256.txt"
#: from_output = "/out/crucible-dtrace.sha256.txt"
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
 
banner prerequisites
ptime -m ./tools/install_builder_prerequisites.sh -y

banner rbuild
ptime -m cargo build --verbose --release --all-features

banner rtest
ptime -m cargo test --verbose --features=omicron-build -- --nocapture > /tmp/cargo-test-out.log 2>&1

banner output
mkdir -p /work/rbins
for t in crucible-downstairs crucible-hammer crutest dsc crudd; do
	gzip < "target/release/$t" > "/work/rbins/$t.gz"
done

mkdir -p /work/scripts
for s in tools/crudd-speed-battery.sh tools/dtrace/perf-downstairs-tick.d tools/dtrace/upstairs_info.d tools/test_mem.sh; do
	cp "$s" /work/scripts/
done

# Make the top level /out directory
pfexec mkdir -p /out
pfexec chown "$UID" /out

# Make the crucible package images
banner image
ptime -m cargo run --bin crucible-package

banner contents
tar tvfz out/crucible.tar.gz
tar tvfz out/crucible-pantry.tar.gz
mv out/crucible.tar.gz out/crucible-pantry.tar.gz /out/

# Build the nightly archive file which should include all the scripts
# and binaries needed to run the nightly test.
# This needs the ./out directory created above
banner nightly
./tools/make-nightly.sh

banner copy
mv out/crucible-nightly.tar.gz /out/crucible-nightly.tar.gz

# Build the dtrace archive file which should include all the dtrace scripts.
# This needs the ./out directory created above
banner dtrace
./tools/make-dtrace.sh

banner copy
mv out/crucible-dtrace.tar /out/crucible-dtrace.tar

banner checksum
cd /out
digest -a sha256 crucible.tar.gz > crucible.sha256.txt
digest -a sha256 crucible-pantry.tar.gz > crucible-pantry.sha256.txt
digest -a sha256 crucible-nightly.tar.gz > crucible-nightly.sha256.txt
digest -a sha256 crucible-dtrace.tar > crucible-dtrace.sha256.txt
