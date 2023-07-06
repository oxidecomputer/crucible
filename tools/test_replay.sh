#!/bin/bash

# Create regions and start up the downstairs.
# Run the crutest replay test using the downstairs.
err=0
total=0
pass_total=0
SECONDS=0
loops=2

usage () {
    echo "Usage: $0 [-l #]]" >&2
    echo " -l loops   Number of test loops to perform (default 10)" >&2
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

# Control-C to cleanup.
trap ctrl_c INT
function ctrl_c() {
    echo "Stopping at your request"
    ${dsc} cmd shutdown
}

loop_log=/tmp/test_replay_summary.log
test_log=/tmp/test_replay.log

ROOT=$(cd "$(dirname "$0")/.." && pwd)
export BINDIR=${BINDIR:-$ROOT/target/debug}
crucible_test="$BINDIR/crutest"
dsc="$BINDIR/dsc"
downstairs="$BINDIR/crucible-downstairs"
if [[ ! -f "$crucible_test" ]] || [[ ! -f "$dsc" ]] || [[ ! -f "$downstairs" ]]; then
    echo "Can't find required binaries"
    echo "Missing $crucible_test or $dsc or $downstairs"
    exit 1
fi

echo "" > ${loop_log}
echo "" > ${test_log}
echo "starting $(date)" | tee ${loop_log}
echo "Tail $test_log for test output"

if ! ${dsc} create --cleanup --ds-bin "$downstairs" --extent-count 50 >> "$test_log"; then
    echo "Failed to create downstairs regions"
    exit 1
fi
${dsc} start --ds-bin "$downstairs" >> "$test_log" 2>&1 &
dsc_pid=$!
sleep 5
if ! ps -p $dsc_pid > /dev/null; then
    echo "$dsc failed to start"
    exit 1
fi

args=()
args+=( -t "127.0.0.1:8810" )
args+=( -t "127.0.0.1:8820" )
args+=( -t "127.0.0.1:8830" )

gen=1
# Initial seed for verify file
if ! "$crucible_test" fill "${args[@]}" -q -g "$gen"\
          --verify-out alan --retry-activate >> "$test_log" 2>&1 ; then
    echo Failed on initial verify seed, check "$test_log"
    ${dsc} cmd shutdown
fi
(( gen += 1 ))

# Now run the crutest replay test in a loop
count=1
while [[ $count -le $loops ]]; do
    SECONDS=0
    echo "" > "$test_log"
    echo "New loop starts now $(date)" >> "$test_log"
    "$crucible_test" replay "${args[@]}" -c 2 \
            --stable -g "$gen" --verify-out alan \
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
    (( gen += 1 ))
    (( pass_total += 1 ))
    (( total += duration ))
    ave=$(( total / pass_total ))
    printf "[%03d/%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d \
last_run_seconds:%d\n" "$count" "$loops" $((duration / 60)) $((duration % 60)) \
$((ave / 60)) $((ave % 60))  $((total / 60)) $((total % 60)) \
"$err" $duration | tee -a ${loop_log}
	(( count += 1 ))
done
${dsc} cmd shutdown
wait "$dsc_pid"

sleep 4
echo "Final results:" | tee -a ${loop_log}
printf "[%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d last_run_seconds:%d\n" "$i" $((duration / 60)) $((duration % 60)) $((ave / 60)) $((ave % 60)) $((total / 60)) $((total % 60)) "$err" $duration | tee -a ${loop_log}
echo "$(date) Test ends with $err" >> "$test_log" 2>&1
exit "$err"

