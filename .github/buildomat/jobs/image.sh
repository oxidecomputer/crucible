#!/bin/bash
#:
#: name = "image"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "nightly"
#: output_rules = [
#:	"/out/*",
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

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

banner build
ptime -m cargo build --release --verbose --all-features

banner image
ptime -m cargo run --bin crucible-package

banner contents
tar tvfz out/crucible.tar.gz

banner copy
pfexec mkdir -p /out
pfexec chown "$UID" /out
mv out/crucible.tar.gz /out/crucible.tar.gz
cd /out
digest -a sha256 crucible.tar.gz > crucible.sha256.txt
