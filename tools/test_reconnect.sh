#!/bin/bash

# Start up the downstairs-deamon script.
# Run the quick client one test to verify that restarting
# crucible downstairs in a loop will still work.
err=0
total=0
pass_total=0
SECONDS=0

# Control-C to cleanup.
trap ctrl_c INT
function ctrl_c() {
    echo "Stopping at your request"
    ${dsc} cmd shutdown
}

loop_log=/tmp/test_reconnect_summary.log
test_log=/tmp/test_reconnect.log

ROOT=$(cd "$(dirname "$0")/.." && pwd)
export BINDIR=${BINDIR:-$ROOT/target/debug}
crucible_test="$BINDIR/crutest"
dsc="$BINDIR/dsc"
if [[ ! -f "$crucible_test" ]] || [[ ! -f "$dsc" ]]; then
    echo "Can't find crucible-test binary at $crucible_test"
    exit 1
fi

echo "" > ${loop_log}
echo "" > ${test_log}
echo "starting $(date)" | tee ${loop_log}
echo "Tail $test_log for test output"

${dsc} start --cleanup --create --extent-count 50 >> "$test_log" 2>&1 &
dsc_pid=$!
sleep 5
if ! ps -p $dsc_pid > /dev/null; then
    echo "$dsc failed to start"
    exit 1
fi

args=()
port_base=8810
for (( i = 0; i < 30; i += 10 )); do
    (( port = port_base + i ))
    args+=( -t "127.0.0.1:$port" )
done

# Initial seed for verify file
if ! "$crucible_test" fill "${args[@]}" -q \
          --verify-out alan --retry-activate >> "$test_log" 2>&1 ; then
    echo Failed on initial verify seed, check "$test_log"
    ${dsc} cmd shutdown
fi

# Tell dsc to restart downstairs.
if ! "$dsc" cmd enable-restart-all; then
    echo "Failed to enable auto-restart on dsc"
    exit 1
fi

# Allow the downstairs to start restarting now.
if ! ${dsc} cmd enable-random-stop; then
    echo "Failed to enable random-stop on dsc"
    exit 1
fi
sleep 5

# Now run the quick crucible client test in a loop
for i in {1..5}
do
    SECONDS=0
    echo "" > "$test_log"
    echo "New loop starts now $(date)" >> "$test_log"
    "$crucible_test" generic "${args[@]}" -c 15000 \
            -q --verify-out alan \
            --verify-in alan \
            --retry-activate >> "$test_log" 2>&1
    result=$?
    if [[ $result -ne 0 ]]; then
        touch /var/tmp/ds_test/up 2> /dev/null
        (( err += 1 ))
        duration=$SECONDS
        printf "[%03d] Error $result after %d:%02d\n" "$i" \
                $((duration / 60)) $((duration % 60)) | tee -a ${loop_log}
        mv "$test_log" "$test_log".lastfail
        break
    fi
    duration=$SECONDS
    (( pass_total += 1 ))
    (( total += duration ))
    ave=$(( total / pass_total ))
    printf "[%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d \
last_run_seconds:%d\n" "$i" $((duration / 60)) $((duration % 60)) \
$((ave / 60)) $((ave % 60))  $((total / 60)) $((total % 60)) \
"$err" $duration | tee -a ${loop_log}

done
${dsc} cmd shutdown
sleep 4
echo "Final results:" | tee -a ${loop_log}
printf "[%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d last_run_seconds:%d\n" "$i" $((duration / 60)) $((duration % 60)) $((ave / 60)) $((ave % 60)) $((total / 60)) $((total % 60)) "$err" $duration | tee -a ${loop_log}
echo "$(date) Test ends with $err" >> "$test_log" 2>&1
exit "$err"

