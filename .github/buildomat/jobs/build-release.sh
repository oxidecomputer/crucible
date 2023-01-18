#!/bin/bash
#:
#: name = "rbuild"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "1.66"
#: output_rules = [
#:	"/out/*",
#:	"/work/rbins/*",
#:	"/work/scripts/*",
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
for t in crucible-downstairs crucible-hammer crutest dsc crudd; do
	gzip < "target/release/$t" > "/work/rbins/$t.gz"
done

mkdir -p /work/scripts
for s in tools/test_perf.sh tools/crudd-speed-battery.sh tools/dtrace/perf-downstairs-tick.d; do
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

tar cavf out/crucible-nightly.tar.gz \
    target/release/crutest \
    target/release/crucible-downstairs \
    target/release/crucible-hammer \
    target/release/dsc \
    target/release/crudd \
    tools/downstairs_daemon.sh \
    tools/hammer_loop.sh \
    tools/test_reconnect.sh \
    tools/test_repair.sh \
    tools/test_restart_repair.sh \
    tools/test_nightly.sh \
    tools/crudd-speed-battery.sh

banner copy
mv out/crucible-nightly.tar.gz /out/crucible-nightly.tar.gz

banner checksum
cd /out
digest -a sha256 crucible.tar.gz > crucible.sha256.txt
digest -a sha256 crucible-pantry.tar.gz > crucible-pantry.sha256.txt
digest -a sha256 crucible-nightly.tar.gz > crucible-nightly.sha256.txt
