#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

ROOT=$(cd "$(dirname "$0")/.." && pwd)

echo "$ROOT"
cd "$ROOT"

if pgrep -fl target/debug/crucible-downstairs; then
	echo 'already running?' >&2
	exit 1
fi

testdir=/tmp/ds_test
if [[ -d ${testdir} ]]; then
    rm -rf ${testdir}
fi

for (( i = 0; i < 3; i++ )); do
	(( port = 3801 + i ))
	dir="${testdir}/$port"
	cargo run -p crucible-downstairs -- create -u $(uuidgen) -d "$dir"
	cargo run -p crucible-downstairs -- run -p "$port" -d "$dir" &
	disown
done
