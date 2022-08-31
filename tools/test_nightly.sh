#!/bin/bash

# nightly

SECONDS=0
err=0
output_file="/tmp/nightly_results"
rm -f "$output_file"

ROOT=$(cd "$(dirname "$0")/.." && pwd)
export BINDIR=${BINDIR:-$ROOT/target/release}

echo "Nightly starts at $(date)" | tee "$output_file"
echo "$(date) hammer start" >> "$output_file"
banner hammer
./tools/hammer_loop.sh
res=$?
if [[ "$res" -eq 0 ]]; then
    echo "$(date) hammer pass" >> "$output_file"
else
    echo "$(date) hammer fail with: $res" >> "$output_file"
    (( err += 1 ))
fi

banner reconnect
echo "$(date) reconnect start" >> "$output_file"
./tools/test_reconnect.sh
res=$?
if [[ "$res" -eq 0 ]]; then
    echo "$(date) reconnect pass" >> "$output_file"
else
    echo "$(date) reconnect fail with: $res" >> "$output_file"
    (( err += 1 ))
fi

banner repair
echo "$(date) repair start" >> "$output_file"
./tools/test_repair.sh
res=$?
if [[ "$res" -eq 0 ]]; then
    echo "$(date) repair pass" >> "$output_file"
else
    echo "$(date) repair fail with: $res" >> "$output_file"
    (( err += 1 ))
fi

banner restart_repair
echo "$(date) restart_repair start" >> "$output_file"
./tools/test_restart_repair.sh
res=$?
if [[ "$res" -eq 0 ]]; then
    echo "$(date) restart_repair pass" >> "$output_file"
else
    echo "$(date) restart_repair fail with: $res" >> "$output_file"
    (( err += 1 ))
fi

duration=$SECONDS

banner results
cat "$output_file"
printf "Tests took %d:%02d  errors:%d\n" \
    $((duration / 60)) $((duration % 60)) "$err"

