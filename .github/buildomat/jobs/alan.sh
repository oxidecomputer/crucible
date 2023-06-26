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

#
# If we fail, try to collect some debugging information
#
_exit_trap() {
    local status=$?
    [[ $status -eq 0 ]] && exit 0

    set +o errexit
    set -o xtrace
    banner evidence

	CORES=$(ls /tmp/core*)
	for c in $CORES; do
	    echo "Stack for Core file $c"
        pstack "$c"
    done

    exit $status
}
trap _exit_trap EXIT

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
ptime -m cargo build --release --verbose > /tmp/buildout.txt 2>&1

for i in {0..30}; do
    date
    banner "$i"
    ptime -m cargo test --verbose -- --nocapture > /tmp/testout.log 2>&1
    rm /tmp/test*log
done
banner finished
