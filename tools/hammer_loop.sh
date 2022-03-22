#!/bin/bash

# Run hammer command in a loop, sending output to a file.
# Report how long each loop takes and compute average times for
# successful runs.
# If we get an error, panic, or assertion failed, then stop.
err=0
total=0
pass_total=0
SECONDS=0

loop_log=/tmp/hammer_loop.log
test_log=/tmp/hammer_loop_test.log
echo "" > ${loop_log}
echo "starting Hammer test on $(date)" | tee ${loop_log}
echo "Tail $test_log for test output"

for i in {1..100}
do
    SECONDS=0
    echo "" > "$test_log"
    echo "New loop starts now $(date)" >> "$test_log"
    cargo run -q -p crucible-hammer -- -t 127.0.0.1:8810 -t 127.0.0.1:8820 \
        -t 127.0.0.1:8830 >> "$test_log" 2>&1
    result=$?
    if [[ $result -ne 0 ]]; then
        touch /tmp/ds_test/up 2> /dev/null
        (( err += 1 ))
        duration=$SECONDS
        printf "[%03d] Error $result after %d:%02d\n" "$i" \
                $((duration / 60)) $((duration % 60)) | tee -a ${loop_log}
        mv "$test_log" "$test_log".lastfail
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
    (( pass_total += 1 ))
    (( total += duration ))
    ave=$(( total / pass_total ))
    printf "[%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d \
last_run_seconds:%d\n" "$i" $((duration / 60)) $((duration % 60)) \
$((ave / 60)) $((ave % 60))  $((total / 60)) $((total % 60)) \
"$err" $duration | tee -a ${loop_log}

done
echo "Final results:" | tee -a ${loop_log}
printf "[%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d last_run_seconds:%d\n" "$i" $((duration / 60)) $((duration % 60)) $((ave / 60)) $((ave % 60)) $((total / 60)) $((total % 60)) "$err" $duration | tee -a ${loop_log}
exit "$err"

