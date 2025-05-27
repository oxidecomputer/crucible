#!/bin/bash

# A nightly test, which is just a collection of other tests.
# If you are adding a test, make sure the binaries/scripts it needs are
# also part of what buildomat puts in the nightly archive, currently
# generated in: .github/buildomat/jobs/build-release.sh

SECONDS=0
err=0
output_file="/tmp/nightly_results"
rm -f "$output_file"

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT" || (echo failed to cd "$ROOT"; exit 1)
export BINDIR=${BINDIR:-$ROOT/target/release}

echo "Nightly starts at $(date)" | tee "$output_file"
echo "Running on $(git log -1 --no-color | head -20)" | tee -a "$output_file"
echo "" >> "$output_file"
echo "Environment settings are (Some may be unset):" | tee -a "$output_file"
echo "BINDIR is: $BINDIR" | tee -a "$output_file"
echo "REGION_ROOT is: $REGION_ROOT" | tee -a "$output_file"
echo "WORK_ROOT is: $WORK_ROOT" | tee -a "$output_file"
echo "REGION_SETS is: $REGION_SETS" | tee -a "$output_file"
echo "$(date) hammer start" >> "$output_file"
banner hammer
banner loop
./tools/hammer_loop.sh -l 200
res=$?
if [[ "$res" -eq 0 ]]; then
    echo "$(date) hammer pass" >> "$output_file"
else
    echo "$(date) hammer fail with: $res" >> "$output_file"
    (( err += 1 ))
fi
echo ""

sleep 1
banner test
banner replay
echo "$(date) test_replay start" >> "$output_file"
./tools/test_replay.sh -l 200
res=$?
if [[ "$res" -eq 0 ]]; then
    echo "$(date) test_replay pass" >> "$output_file"
else
    echo "$(date) test_replay fail with: $res" >> "$output_file"
    (( err += 1 ))
fi
echo ""

sleep 1
banner "test"
banner repair
echo "$(date) test_repair start" >> "$output_file"
./tools/test_repair.sh -l 500
res=$?
if [[ "$res" -eq 0 ]]; then
    echo "$(date) test_repair pass" >> "$output_file"
else
    echo "$(date) test_repair fail with: $res" >> "$output_file"
    (( err += 1 ))
    exit 1
fi
echo ""

sleep 1
banner restart
banner repair
echo "$(date) test_restart_repair start" >> "$output_file"
./tools/test_restart_repair.sh -l 50
res=$?
if [[ "$res" -eq 0 ]]; then
    echo "$(date) test_restart_repair pass" >> "$output_file"
else
    echo "$(date) test_restart_repair fail with: $res" >> "$output_file"
    (( err += 1 ))
    exit 1
fi
echo ""

sleep 1
banner live
banner repair
echo "$(date) test_live_repair start" >> "$output_file"
./tools/test_live_repair.sh -l 20
res=$?
if [[ "$res" -eq 0 ]]; then
    echo "$(date) test_live_repair pass" >> "$output_file"
else
    echo "$(date) test_live_repair fail with: $res" >> "$output_file"
    (( err += 1 ))
    exit 1
fi
echo ""

sleep 1
banner replace
banner special
echo "$(date) test_replace_special start" >> "$output_file"
./tools/test_replace_special.sh -l 50
res=$?
if [[ "$res" -eq 0 ]]; then
    echo "$(date) test_replace_special pass" >> "$output_file"
else
    echo "$(date) test_replace_special fail with: $res" >> "$output_file"
    (( err += 1 ))
    exit 1
fi
duration=$SECONDS

banner results
cat "$output_file"
printf "Tests took %d:%02d  errors:%d\n" \
    $((duration / 60)) $((duration % 60)) "$err" | tee -a "$output_file"

