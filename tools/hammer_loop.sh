#!/bin/bash

# Run hammer command in a loop, sending output to a file.
# Report how long each loop takes and compute average times for
# successful runs.
# If we get an error, panic, or assertion failed, then stop.
err=0
total=0
pass_total=0
SECONDS=0

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT" || (echo failed to cd "$ROOT"; exit 1)

export BINDIR=${BINDIR:-$ROOT/target/debug}
hammer="$BINDIR/crucible-hammer"
cds="$BINDIR/crucible-downstairs"
dsc="$BINDIR/dsc"
for bin in $hammer $cds $dsc; do
    if [[ ! -f "$bin" ]]; then
        echo "Can't find crucible binary at $bin" >&2
        exit 1
    fi
done

if pgrep -fl -U "$(id -u)" "$cds"; then
    echo "Downstairs already running" >&2
    echo Run: pkill -f -U "$(id -u)" "$cds" >&2
    exit 1
fi

WORK_ROOT=${WORK_ROOT:-/tmp}
TEST_ROOT="$WORK_ROOT/hammer_loop"
if [[ -d "$TEST_ROOT" ]]; then
    # Delete previous test data
    rm -r "$TEST_ROOT"
fi
mkdir -p "$TEST_ROOT"
if [[ $? -ne 0 ]]; then
    echo "Failed to make test root $TEST_ROOT"
    exit 1
fi

REGION_ROOT=${REGION_ROOT:-/var/tmp}
MY_REGION_ROOT=${REGION_ROOT}/hammer_loop
if [[ -d "$MY_REGION_ROOT" ]]; then
    rm -rf "$MY_REGION_ROOT"
fi
mkdir -p "$MY_REGION_ROOT"
if [[ $? -ne 0 ]]; then
    echo "Failed to make region root $MY_REGION_ROOT"
    exit 1
fi

loop_log="$TEST_ROOT/hammer_loop.log"
test_log="$TEST_ROOT/hammer_loop_test.log"
dsc_ds_log="$TEST_ROOT/hammer_loop_dsc.log"

loops=20

usage () {
    echo "Usage: $0 [-l #]" >&2
    echo " -l loops   Number of test loops to perform (default 20)" >&2
}

while getopts 'l:' opt; do
    case "$opt" in
        l)  loops=$OPTARG
            ;;
        *)  echo "Invalid option"
            usage
            exit 1
            ;;
    esac
done

if ! "$dsc" create --cleanup --ds-bin "$cds" --extent-count 60 \
    --output-dir "$dsc_ds_log" \
    --extent-size 50 --region-dir "$MY_REGION_ROOT"
then
    echo "Failed to create region"
    exit 1
fi

# Start up dsc, verify it really did start.
"$dsc" start --ds-bin "$cds" --region-dir "$MY_REGION_ROOT" \
    --output-dir "$dsc_ds_log" &
dsc_pid=$!
sleep 5
if ! pgrep -P $dsc_pid; then
    echo "Failed to start dsc"
    exit 1
fi
# Make sure automatic restart is not enabled.
if ! "$dsc" cmd disable-restart-all; then
    echo "Failed to disable auto-restart on dsc"
    exit 1
fi

# Control-C to cleanup.
trap ctrl_c INT
function ctrl_c() {
    echo "Stopping at your request"
    if [[ -n "$dsc_pid" ]]; then
        kill "$dsc_pid"
    fi
    if pgrep -fl -U "$(id -u)" "$cds"; then
        pkill -f -U "$(id -u)" "$cds"
    fi
    exit 1
}
echo "" > ${loop_log}
echo "starting Hammer test on $(date)" | tee ${loop_log}
echo "Tail $test_log for test output"

gen=1
# This is held at 1 loop till we fix the #389 issue, or implement
# generation numbers properly.
count=1
while [[ $count -le $loops ]]; do
    SECONDS=0
    echo "" > "$test_log"
    echo "New loop with gen $gen starts now $(date)" >> "$test_log"
    "$hammer" -t 127.0.0.1:8810 -t 127.0.0.1:8820 \
        -t 127.0.0.1:8830 -g "$gen" >> "$test_log" 2>&1
    result=$?
    if [[ $result -ne 0 ]]; then
        touch /tmp/ds_test/up 2> /dev/null
        (( err += 1 ))
        duration=$SECONDS
        printf "[%03d] Error $result after %d:%02d\n" "$count" \
                $((duration / 60)) $((duration % 60)) | tee -a ${loop_log}
        mv "$test_log" "$test_log".lastfail
        echo "Failing test log at: $test_log.lastfail"
        break
    fi
    if grep -i panic "$test_log"; then
        echo "Panic detected"
        exit 1
    fi
    if grep -i assertion "$test_log"; then
        echo "assertion detected"
        exit 1
    fi
    duration=$SECONDS
    # Each loop of the hammer test uses 5 generation numbers, give it more
    (( gen += 10 ))
    (( pass_total += 1 ))
    (( total += duration ))
    ave=$(( total / pass_total ))
    printf "[%03d/%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d \
last_run_seconds:%d\n" \
  "$count" "$loops" \
  $((duration / 60)) $((duration % 60)) \
  $((ave / 60)) $((ave % 60)) \
  $((total / 60)) $((total % 60)) \
  "$err" $duration | tee -a ${loop_log}
    (( count += 1 ))
done
echo "Final results:" | tee -a ${loop_log}
printf "[%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d last_run_seconds:%d\n" \
  "$count" \
   $((duration / 60)) $((duration % 60)) \
  $((ave / 60)) $((ave % 60)) \
  $((total / 60)) $((total % 60)) \
  "$err" $duration | tee -a ${loop_log}

echo "Stopping dsc"
"$dsc" cmd shutdown
wait $dsc_pid

# Also remove any leftover downstairs
if pgrep -fl -U "$(id -u)" "$cds" > /dev/null; then
    pkill -f -U "$(id -u)" "$cds"
fi

if [[ $err -eq 0 ]]; then
    # No errors, then cleanup all our logs and the region directories.
    rm -r "$TEST_ROOT"
    rm -rf "$MY_REGION_ROOT"
fi
exit "$err"
