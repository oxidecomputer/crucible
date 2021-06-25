#!/bin/bash

set -o errexit
set -o pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)

cd "$ROOT"

cargo run -p crucible-scope -- "$ROOT/.scope.upstairs.sock"
