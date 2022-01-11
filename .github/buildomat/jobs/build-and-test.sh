#!/bin/bash
#:
#: name = "build-and-test"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "nightly"
#: output_rules = []
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

banner build
ptime -m cargo build --verbose

banner test
ptime -m cargo test --lib --verbose

banner bench
ptime -m cargo bench -p crucible-downstairs

banner test_up.sh
ptime -m ./tools/test_up.sh

banner test_ds.sh
ptime -m ./tools/test_ds.sh
