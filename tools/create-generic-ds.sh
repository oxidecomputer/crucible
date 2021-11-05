#!/bin/bash
#
# A hack of downstairs create tool

ROOT=$(cd "$(dirname "$0")/.." && pwd)

cd "$ROOT" || (echo failed to cd "$ROOT"; exit 1)

if pgrep -fl crucible-downstairs; then
    echo 'Some downstairs already running?' >&2
    exit 1
fi

if ! cargo build; then
    echo "Initial Build failed, no tests ran"
    exit 1
fi

if [[ -d var/3801 ]] || [[ -d var/3802 ]] || [[ -d var/3803 ]]; then
    echo " var/380* directories are already present"
    exit 1
fi

cds="./target/debug/crucible-downstairs"
if [[ ! -f ${cds} ]]; then
    echo "Can't find crucible binary at $cds"
    exit 1
fi

res=0
for port in 3801 3802 3803; do
    if ! cargo run -q -p crucible-downstairs -- create -u 12345678-"$port"-"$port"-"$port"-00000000"$port" -d var/"$port" --extent-count 20 --extent-size 100; then
        echo "Failed to create downstairs $port"
        res=1
    fi
done
exit $res
