#!/bin/bash

# Create and start up the downstairs.
# In a loop:
#  Send IO through crutest
#  Fault a downstairs
#  Let the repair start.
#  Fault the downstairs again.
#  Let the upstairs repair start over and finish.

err=0
total=0
pass_total=0
SECONDS=0
dropshot=0
ulimit -n 65536

# Control-C to cleanup.
trap ctrl_c INT
function ctrl_c() {
    echo "Stopping at your request"
    if ps -p "$dsc_pid" ; then
        ${dsc} cmd shutdown
        wait "$dsc_pid"
    fi
    if ps -p "$crutest_pid" ; then
        kill "$crutest_pid"
        wait "$crutest_pid"
    fi
    exit 1
}

loop_log=/tmp/test_fail_live_repair_summary.log
test_log=/tmp/test_fail_live_repair.log
dsc_test_log=/tmp/test_fail_live_repair_dsc.log
verify_file=/tmp/test_fail_live_verify

ROOT=$(cd "$(dirname "$0")/.." && pwd)
export BINDIR=${BINDIR:-$ROOT/target/release}
crucible_test="$BINDIR/crutest"
cds="$BINDIR/crucible-downstairs"
dsc="$BINDIR/dsc"
for bin in $cds $crucible_test $dsc; do
    if [[ ! -f "$bin" ]]; then
        echo "Can't find crucible binary at $bin" >&2
        exit 1
    fi
done

# Verify there is not a downstairs already running.
if pgrep -fl -U "$(id -u)" "$cds"; then
    echo "Downstairs already running" >&2
    echo Run: pkill -f -U "$(id -u)" "$cds" >&2
    exit 1
fi

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

echo "" > ${loop_log}
echo "" > ${test_log}
echo "" > ${dsc_test_log}
if [[ -f "$verify_file" ]]; then
    rm  ${verify_file}
fi
echo "starting $(date)" | tee ${loop_log}
echo "Tail $test_log for test output"

# Make enough extents that we can be sure to catch in while it
# is repairing.
if ! ${dsc} create --cleanup \
  --ds-bin "$cds" \
  --extent-count 200 \
  --extent-size 300 >> "$dsc_test_log"; then
    echo "Failed to create downstairs regions"
    exit 1
fi
${dsc} start --ds-bin "$cds" >> "$dsc_test_log" 2>&1 &
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

gen=1
# Initial seed for verify file
if ! "$crucible_test" fill "${args[@]}" -q -g "$gen"\
          --verify-out "$verify_file" \
          --retry-activate >> "$test_log" 2>&1 ; then
    echo Failed on initial verify seed, check "$test_log"
    ${dsc} cmd shutdown
    exit 1
fi
(( gen += 1 ))

count=1
while [[ $count -le $loops ]]; do
    SECONDS=0
    choice=$((RANDOM % 3))

    # Clear the log on each loop
    echo "" > "$test_log"
    echo "New loop starts now $(date) faulting: $choice" | tee -a "$test_log"
    # Start sending IO.
    "$crucible_test" generic "${args[@]}" --continuous \
        -q -g "$gen" --verify-out "$verify_file" \
        --verify-in "$verify_file" \
        --control 127.0.0.1:7777 \
        --retry-activate >> "$test_log" 2>&1 &
    crutest_pid=$!
    sleep 5

    ${dsc} cmd stop -c "$choice" >> "$dsc_test_log" 2>&1 &
    # Wait for our downstairs to fault
    echo Wait for our downstairs to fault | tee -a "$test_log"
    choice_state="undefined"
    while [[ "$choice_state" != "faulted" ]]; do
        sleep 3
        if [[ $choice -eq 0 ]]; then
            choice_state=$(curl -s http://127.0.0.1:7777/info | awk -F\" '{print $8}')
        elif [[ $choice -eq 1 ]]; then
            choice_state=$(curl -s http://127.0.0.1:7777/info | awk -F\" '{print $10}')
        else
            choice_state=$(curl -s http://127.0.0.1:7777/info | awk -F\" '{print $12}')
        fi
    done

    ${dsc} cmd start -c "$choice" >> "$dsc_test_log" 2>&1 &

    # Wait for our downstairs to begin live_repair
    echo Wait for our downstairs to begin live_repair | tee -a "$test_log"
    choice_state="undefined"
    while [[ "$choice_state" != "live_repair" ]]; do
        sleep 3
        if [[ $choice -eq 0 ]]; then
            choice_state=$(curl -s http://127.0.0.1:7777/info | awk -F\" '{print $8}')
        elif [[ $choice -eq 1 ]]; then
            choice_state=$(curl -s http://127.0.0.1:7777/info | awk -F\" '{print $10}')
        else
            choice_state=$(curl -s http://127.0.0.1:7777/info | awk -F\" '{print $12}')
        fi
    done

    # Give the live repair between 5 and 25 seconds to start repairing.
    rand_sleep=$((RANDOM % 20))
    ((rand_sleep += 5))
    sleep $rand_sleep
    echo "After $rand_sleep seconds, Stop $choice again" | tee -a "$test_log"
    ${dsc} cmd stop -c "$choice" >> "$dsc_test_log" 2>&1 &

    sleep 2
    echo "Start $choice for a second time" | tee -a "$test_log"
    ${dsc} cmd start -c "$choice" >> "$dsc_test_log" 2>&1 &

    # Now wait for all downstairs to be active
    echo Now wait for all downstairs to be active | tee -a "$test_log"
    all_state=$(curl -s http://127.0.0.1:7777/info | awk -F\" '{print $8","$10","$12}')
    while [[ "${all_state}" != "active,active,active" ]]; do
        sleep 5
        all_state=$(curl -s http://127.0.0.1:7777/info | awk -F\" '{print $8","$10","$12}')
    done

    echo All downstairs active, now stop IO test and wait for it to finish | tee -a "$test_log"
    kill -SIGUSR1 $crutest_pid
    wait $crutest_pid
    result=$?
    if [[ $result -ne 0 ]]; then
        if tail "$test_log" | grep dropshot > /dev/null ; then
            (( dropshot += 1 ))
        else
            (( err += 1 ))
            duration=$SECONDS
            printf "[%03d] Error $result after %d:%02d\n" "$count" \
                    $((duration / 60)) $((duration % 60)) | tee -a ${loop_log}
            mv "$test_log" "$test_log".lastfail
            break
        fi
    fi

    (( gen += 1 ))
    # Run a verify now
    if ! "$crucible_test" verify "${args[@]}" -q -g "$gen" \
      --verify-in "$verify_file" \
      --control 127.0.0.1:7777 >> "$test_log" 2>&1 ; then
        if tail "$test_log" | grep dropshot > /dev/null ; then
            (( dropshot += 1 ))
        else
            mv "$test_log" "$test_log".lastfail
            echo "verify failed on loop $count"
            (( err += 1 ))
            break
        fi
    fi

    duration=$SECONDS
    (( gen += 1 ))
    (( pass_total += 1 ))
    (( total += duration ))
    ave=$(( total / pass_total ))
    printf \
      "[%03d][%d] %d:%02d  ds_err:%d ave:%d:%02d total:%d:%02d last_run:%d\n" \
      "$count" "$choice" \
      $((duration / 60)) $((duration % 60)) \
      "$dropshot"  \
      $((ave / 60)) $((ave % 60))  $((total / 60)) $((total % 60)) \
      "$duration" | tee -a ${loop_log}

    (( count += 1 ))
done

# Stop dsc.
${dsc} cmd shutdown
wait ${dsc_pid}

echo "Final results:" | tee -a ${loop_log}
printf \
  "[%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d last_run_seconds:%d\n" \
  "$count" $((duration / 60)) $((duration % 60)) \
  $((ave / 60)) $((ave % 60)) $((total / 60)) $((total % 60)) \
  "$err" $duration | tee -a ${loop_log}
echo "$(date) Test ends with $err" | tee -a "$test_log"
exit "$err"

