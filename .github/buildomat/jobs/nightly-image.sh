#!/bin/bash
#:
#: name = "nightly-image"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "nightly"
#: output_rules = [
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

banner build
ptime -m cargo build --release --verbose --all-features

banner n-image
mkdir out
tar cavf out/crucible-nightly.tar.gz \
    target/release/crucible-client \
    target/release/crucible-downstairs \
    target/release/crucible-hammer \
    target/release/dsc \
    tools/downstairs_daemon.sh \
    tools/hammer_loop.sh \
    tools/test_reconnect.sh \
    tools/test_repair.sh \
    tools/test_restart_repair.sh

banner copy
pfexec mkdir -p /out
pfexec chown "$UID" /out
mv out/crucible-nightly.tar.gz /out/crucible-nightly.tar.gz
cd /out
digest -a sha256 crucible-nightly.tar.gz > crucible-nightly.sha256.txt
