#!/bin/bash
#
# Test import and export functions of crucible-downstairs

set -o pipefail

ulimit -n 16384

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT" || (echo failed to cd "$ROOT"; exit 1)
BINDIR=${BINDIR:-$ROOT/target/debug}

if pgrep -fl crucible-downstairs; then
    echo 'Downstairs already running?' >&2
    exit 1
fi

cds="$BINDIR/crucible-downstairs"
if [[ ! -f ${cds} ]]; then
    echo "Can't find crucible binary at $cds"
    exit 1
fi

testdir="/tmp/ds_test"
if [[ -d ${testdir} ]]; then
    rm -rf ${testdir}
fi
mkdir "${testdir}"

set -o errexit
uuid="12345678-1234-1234-1234-000000000001"
region_dir="${testdir}/region"
exp="${testdir}/exported_file"
imp="${testdir}/import"
clone_dir="${testdir}/clone"
clone_exp="${testdir}/clone_export_file"
create_clone_dir="${testdir}/create_clone"
echo "Create file for import"
dd if=/dev/urandom of="$imp" bs=512 count=300

echo "Import region"
${cds} create -i "$imp" -u $uuid -d "$region_dir"
echo "Export region"
${cds} export -d "$region_dir" -e "$exp" --count 300

diff $imp $exp
echo "Import Export test passed"

# We can make use of the export function to test downstairs clone
echo "Test clone"
echo "Starting source downstairs"
${cds} run -d "$region_dir" -p 8810 --mode ro > ${testdir}/ds_out.txt &
ds_pid=$!

sleep 1
if ! ps -p $ds_pid; then
    echo "Failed to start downstairs"
    exit 1
else
    echo "Downstairs running"
fi

echo "Creating new downstairs"
${cds} create -u $(uuidgen) -d "$clone_dir" --extent-size 100 --extent-count 15 --block-size 512
echo "Cloning existing downstairs"
${cds} clone -d "$clone_dir" -s 127.0.0.1:12810

echo "Verify clone using export"
${cds} export -d "$clone_dir" -e "$clone_exp" --count 300

diff $imp $clone_exp

echo "Creating new downstairs from clone directly"
${cds} create -u $(uuidgen) -d "$create_clone_dir" --extent-size 100 --extent-count 15 --block-size 512 --clone-source 127.0.0.1:12810

echo "Verify second clone using export"
${cds} export -d "$create_clone_dir" -e "$clone_exp" --count 300
diff $imp $clone_exp

echo "Stopping downstairs"
kill "$ds_pid"
echo "Clone test passed"
rm -rf ${testdir}
