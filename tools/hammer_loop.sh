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

if ! "$dsc" create --cleanup --ds-bin "$cds" --extent-count 60 --extent-size 50; then
    echo "Failed to create region"
    exit 1
fi

# Start up dsc, verify it really did start.
"$dsc" start --ds-bin "$cds" &
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

loop_log=/tmp/hammer_loop.log
test_log=/tmp/hammer_loop_test.log
echo "" > ${loop_log}
echo "starting Hammer test on $(date)" | tee ${loop_log}
echo "Tail $test_log for test output"

gen=1
# This is held at 1 loop till we fix the #389 issue, or implement
# generation numbers properly.
for i in {1..20}
do
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
        printf "[%03d] Error $result after %d:%02d\n" "$i" \
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
    printf "[%03d][%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d \
last_run_seconds:%d\n" "$i" "$gen" $((duration / 60)) $((duration % 60)) \
$((ave / 60)) $((ave % 60))  $((total / 60)) $((total % 60)) \
"$err" $duration | tee -a ${loop_log}

done
echo "Final results:" | tee -a ${loop_log}
printf "[%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d last_run_seconds:%d\n" "$i" $((duration / 60)) $((duration % 60)) $((ave / 60)) $((ave % 60)) $((total / 60)) $((total % 60)) "$err" $duration | tee -a ${loop_log}

echo "Stopping dsc"
kill $dsc_pid 2> /dev/null
wait $dsc_pid
# Also remove any leftover downstairs
if pgrep -fl -U "$(id -u)" "$cds" > /dev/null; then
    pkill -f -U "$(id -u)" "$cds"
fi

exit "$err"

