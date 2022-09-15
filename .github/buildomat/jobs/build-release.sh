#!/bin/bash
#:
#: name = "rbuild"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "nightly-2021-11-24"
#: output_rules = [
#:	"/work/rbins/*",
#:	"/work/scripts/*",
#:	"/out/*",
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
#: from_output = "/out/crucible.sha256.txt"
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
for t in crucible-downstairs crucible-hammer crutest dsc; do
	gzip < "target/release/$t" > "/work/rbins/$t.gz"
done

mkdir -p /work/scripts
for s in tools/test_perf.sh; do
	cp "$s" /work/scripts/
done

# Build the nightly archive file which should include all the scripts
# and binaries needed to run the nightly test.
banner nightly
mkdir -p out
tar cavf out/crucible-nightly.tar.gz \
    target/release/crutest \
    target/release/crucible-downstairs \
    target/release/crucible-hammer \
    target/release/dsc \
    tools/downstairs_daemon.sh \
    tools/hammer_loop.sh \
    tools/test_reconnect.sh \
    tools/test_repair.sh \
    tools/test_restart_repair.sh \
    tools/test_nightly.sh

banner copy
pfexec mkdir -p /out
pfexec chown "$UID" /out
mv out/crucible-nightly.tar.gz /out/crucible-nightly.tar.gz
cd /out
digest -a sha256 crucible-nightly.tar.gz > crucible-nightly.sha256.txt
