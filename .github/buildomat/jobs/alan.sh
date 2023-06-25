#!/bin/bash
#:
#: name = "alan"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "1.66"
#: output_rules = [
#:	"/tmp/core.*",
#:	"/tmp/*.log",
#: ]
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

banner cores
pfexec coreadm -i /tmp/core.%f.%p
pfexec coreadm -g /tmp/core.%f.%p
pfexec coreadm -e global
pfexec coreadm -e log
pfexec coreadm -e proc-setid
pfexec coreadm -e global-setid

banner build
ptime -m cargo build --release --all-targets --verbose > /tmp/buildout.txt 2>&1

for i in {0..40}; do
    date
    banner "$i"
    ptime -m cargo test --verbose -- --nocapture > /tmp/testout.log 2>&1
    rm /tmp/test*log
done
banner finished
