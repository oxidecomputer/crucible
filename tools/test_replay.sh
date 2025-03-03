#!/bin/bash

# Create regions and start up the downstairs.
# Run the crutest replay test using the downstairs.
# Start the downstairs using dsc, connect to dsc from within
# crutest and use that interface to stop a downstairs.
result=0
SECONDS=0

# Control-C to cleanup.
trap ctrl_c INT
function ctrl_c() {
    echo "Stopping at your request"
    ${dsc} cmd shutdown
    exit 1
}

WORK_ROOT=${WORK_ROOT:-/tmp}
TEST_ROOT="$WORK_ROOT/test_replay"

if [[ ! -d "$TEST_ROOT" ]]; then
    mkdir -p "$TEST_ROOT"
    if [[ $? -ne 0 ]]; then
        echo "Failed to make test root $TEST_ROOT"
        exit 1
    fi
else
    # Delete previous test data
    rm -r "$TEST_ROOT"
fi

test_log="$TEST_ROOT/test_replay.log"
verify_log="$TEST_ROOT/test_replay_verify.log"
dsc_ds_log="$TEST_ROOT/test_live_repair_dsc.log"

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT" || (echo failed to cd "$ROOT"; exit 1)
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
region_sets=1

usage () {
    echo "Usage: $0 [-l #]]" >&2
    echo " -l loops     Number of times to cause a replay." >&2
    echo " -r regions   Number of region sets to create (default 1)" >&2
}

while getopts 'l:r:' opt; do
    case "$opt" in
        l)  loops=$OPTARG
            echo "Set loops"
            ;;
        r)  region_sets=$OPTARG
            echo "Set region sets"
            ;;
        *)  echo "Invalid option"
            usage
            exit 1
            ;;
    esac
done

((region_count=region_sets*3))
echo "" > "$test_log"
echo "starting $(date)" | tee "$test_log"
echo "Tail $test_log for test output"

echo "Creating $region_count downstairs regions" | tee -a "$test_log"
if ! ${dsc} create --cleanup --ds-bin "$downstairs" \
        --output-dir "$dsc_ds_log" \
        --extent-count 50 --region-count "$region_count" >> "$test_log"; then
    echo "Failed to create downstairs regions"
    exit 1
fi

echo "Starting $region_count downstairs" | tee -a "$test_log"
${dsc} start --ds-bin "$downstairs" --output-dir "$dsc_ds_log" \
	--region-count "$region_count" >> "$test_log" 2>&1 &
dsc_pid=$!
sleep 5
if ! ps -p $dsc_pid > /dev/null; then
    echo "$dsc failed to start"
    exit 1
fi

gen=1
# Initial seed for verify file
echo "Running initial fill" | tee -a "$test_log"
if ! "$crucible_test" fill -q -g "$gen"\
          --dsc 127.0.0.1:9998 \
          --verify-out "$verify_log" --retry-activate >> "$test_log" 2>&1 ; then
    echo Failed on initial verify seed, check "$test_log"
    ${dsc} cmd shutdown
    exit 1
fi
(( gen += 1 ))

SECONDS=0
echo "Replay loop starts now $(date)" | tee -a "$test_log"
"$crucible_test" replay -c "$loops" \
        --stable -g "$gen" --verify-out "$verify_log" \
        --verify-in "$verify_log" \
        --dsc 127.0.0.1:9998 \
        --retry-activate >> "$test_log" 2>&1
result=$?
duration=$SECONDS
if [[ $result -ne 0 ]]; then
    printf "Error $result after %d:%02d\n" \
            $((duration / 60)) $((duration % 60)) | tee -a "$test_log"
else
    (( gen += 1 ))
    printf "Replays:%d time: %d:%02d\n" \
      "$loops" $((duration / 60)) $((duration % 60)) | tee -a "$test_log"

    echo "Do final verify" | tee -a "$test_log"
    if ! "$crucible_test" verify -q -g "$gen"\
              --dsc 127.0.0.1:9998 \
              --verify-out "$verify_log" \
              --verify-in "$verify_log" >> "$test_log" 2>&1 ; then
        echo Failed on final verify, check "$test_log"
        result=1
    fi
fi

${dsc} cmd shutdown
wait "$dsc_pid"

sleep 4
echo "$(date) Test ends with $result" | tee -a "$test_log" 2>&1
if [[ $result -eq 0 ]]; then
    rm -r "$TEST_ROOT"
fi
exit "$result"
