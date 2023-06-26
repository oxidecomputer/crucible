#!/bin/bash
#:
#: name = "build"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "1.66"
#: output_rules = [
#:	"/work/bins/*",
#:	"/work/scripts/*",
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
ptime -m cargo build --verbose > /tmp/buildout.txt 2>&1

banner output

mkdir -p /work/bins
for t in crucible-downstairs crucible-hammer crutest dsc; do
	gzip < "target/debug/$t" > "/work/bins/$t.gz"
done

mkdir -p /work/scripts
for s in tools/test_live_repair.sh tools/test_repair.sh tools/test_up.sh tools/test_ds.sh; do
	cp "$s" /work/scripts/
done

echo in_work_scripts
ls -l /work/scripts
echo in_work_bins
ls -l /work/bins

banner test
ptime -m cargo test --verbose -- --nocapture
