#!/bin/bash
#
# Simple script to start three downstairs, then run through all the tests
# that exist on the crucible client program.  This should eventually either
# move to some common test framework, or be thrown away.

set -o pipefail

ulimit -n 16384

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BINDIR=${BINDIR:-$ROOT/target/debug}

echo "$ROOT"
cd "$ROOT"

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
dir="${testdir}/export"
exp="${testdir}/exported_file"
imp="${testdir}/import"
echo "Create file for import"
dd if=/dev/urandom of="$imp" bs=512 count=300

echo "Import region"
${cds} create -i "$imp" -u $uuid -d "$dir"
echo "Export region"
${cds} export -d "$dir" -e "$exp" --count 300

diff $imp $exp

echo "Import Export test passed"
rm -rf ${testdir}
