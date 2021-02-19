#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

ROOT=$(cd "$(dirname "$0")/.." && pwd)

cd "$ROOT"

if pgrep -fl target/debug/crucible-downstairs; then
	echo 'already running?' >&2
	exit 1
fi

for (( i = 0; i < 3; i++ )); do
	(( port = 3801 + i ))
	dir="$ROOT/var/$port"
	mkdir -p "$dir"
	cargo run -p crucible-downstairs -- -p "$port" -d "$dir" &
	disown
done
