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
    touch /tmp/ds_test/stop
    if [[ -n "$dsd_pid" ]]; then
        kill "$dsd_pid"
    fi
}

loop_log=/tmp/reconnect.log
test_log=/tmp/reconnect_test.log
echo "" > ${loop_log}
echo "starting $(date)" | tee ${loop_log}
echo "Tail $test_log for test output"

./tools/downstairs_daemon.sh >> "$test_log" 2>&1 &
dsd_pid=$!

# Sleep 5 to give the downstairs time to get going.
sleep 5

if ! ps -p $dsd_pid > /dev/null; then
    echo "downstairs_daemon failed to start"
    exit 1
fi

args=()
port_base=8801
for (( i = 0; i < 3; i++ )); do
    (( port = port_base + i ))
    args+=( -t "127.0.0.1:$port" )
done

# Initial seed for verify file
if ! cargo run -q -p crucible-client -- "${args[@]}" \
          -w one -q --verify-out alan >> "$test_log" 2>&1 ; then
    echo Failed on initial verify seed, check "$test_log"
    touch /var/tmp/ds_test/stop
    exit 1
fi

# Now run the quick client test in a loop
for i in {1..100}
do
    SECONDS=0
    echo "" > "$test_log"
    echo "New loop starts now $(date)" >> "$test_log"
    cargo run -q -p crucible-client -- "${args[@]}" \
            -w one -q --verify-out alan \
            --verify-in alan >> "$test_log" 2>&1
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
touch /var/tmp/ds_test/stop
echo "Final results:" | tee -a ${loop_log}
printf "[%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d last_run_seconds:%d\n" "$i" $((duration / 60)) $((duration % 60)) $((ave / 60)) $((ave % 60)) $((total / 60)) $((total % 60)) "$err" $duration | tee -a ${loop_log}
exit "$err"

