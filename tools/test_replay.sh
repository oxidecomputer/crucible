#!/bin/bash

# Create regions and start up the downstairs.
# Run the crutest replay test using the downstairs.
# Start the downstairs using dsc, connect to dsc from within
# crutest and use that interface to stop a downstairs.
result=0
total=0
pass_total=0
SECONDS=0

# Control-C to cleanup.
trap ctrl_c INT
function ctrl_c() {
    echo "Stopping at your request"
    ${dsc} cmd shutdown
}

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

loops=30

usage () {
    echo "Usage: $0 [-l #]]" >&2
    echo " -l loops   Number of times to cause a replay." >&2
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

echo "" > ${test_log}
echo "starting $(date)" | tee ${test_log}
echo "Tail $test_log for test output"

echo "Creating downstairs regions" | tee -a "$test_log"
if ! ${dsc} create --cleanup --ds-bin "$downstairs" --extent-count 50 >> "$test_log"; then
    echo "Failed to create downstairs regions"
    exit 1
fi

echo "Starting downstairs" | tee -a "$test_log"
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
echo "Running initial fill" | tee -a "$test_log"
if ! "$crucible_test" fill "${args[@]}" -q -g "$gen"\
          --verify-out alan --retry-activate >> "$test_log" 2>&1 ; then
    echo Failed on initial verify seed, check "$test_log"
    ${dsc} cmd shutdown
    exit 1
fi
(( gen += 1 ))

SECONDS=0
echo "Replay loop starts now $(date)" | tee -a "$test_log"
"$crucible_test" replay "${args[@]}" -c "$loops" \
        --stable -g "$gen" --verify-out alan \
        --verify-in alan \
        --retry-activate >> "$test_log" 2>&1
result=$?
duration=$SECONDS
if [[ $result -ne 0 ]]; then
    printf "Error $result after %d:%02d\n" \
            $((duration / 60)) $((duration % 60)) | tee -a ${test_log}
else
    (( gen += 1 ))
    printf "Replays:%d time: %d:%02d\n" \
      "$loops" $((duration / 60)) $((duration % 60)) | tee -a ${test_log}

    echo "Do final verify" | tee -a "$test_log"
    if ! "$crucible_test" verify "${args[@]}" -q -g "$gen"\
              --verify-out alan --verify-in alan >> "$test_log" 2>&1 ; then
        echo Failed on final verify, check "$test_log"
        result=1
    fi
fi

${dsc} cmd shutdown
wait "$dsc_pid"

sleep 4
echo "$(date) Test ends with $result" | tee -a "$test_log" 2>&1
exit "$result"
