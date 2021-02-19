#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

ROOT=$(cd "$(dirname "$0")/.." && pwd)

cd "$ROOT"

args=()
for (( i = 0; i < 3; i++ )); do
	(( port = 3801 + i ))
	args+=( -t "127.0.0.1:$port" )
done
cargo run -p crucible-upstairs -- "${args[@]}"
