#!/bin/bash

# A stress test of replacing a downstairs while reconciliation is underway.
# Using dsc, we create the regions and start up four downstairs.
# Run the crutest special replacement tests using the downstairs we just
# started, with the fourth being the first one to replace.
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

REGION_ROOT=${REGION_ROOT:-/var/tmp/test_replace_special}
mkdir -p "$REGION_ROOT"

# Location of logs and working files
WORK_ROOT=${WORK_ROOT:-/tmp}
mkdir -p "$WORK_ROOT"

loop_log="$WORK_ROOT"/test_replace_special_summary.log
test_log="$WORK_ROOT"/test_replace_special.log

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

loops=5

usage () {
    echo "Usage: $0 [-l #]]" >&2
    echo " -l loops   Number of test loops to perform (default 5)" >&2
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

echo "" > "$loop_log"
echo "" > "$test_log"
echo "starting $(date)" | tee "$loop_log"
echo "Tail $test_log for test output"

if ! ${dsc} create --cleanup \
  --region-dir "$REGION_ROOT" \
  --region-count 4 \
  --ds-bin "$downstairs" \
  --extent-count 400 \
  --block-size 4096 >> "$test_log"; then
    echo "Failed to create downstairs regions"
    exit 1
fi
${dsc} start --ds-bin "$downstairs" \
  --region-dir "$REGION_ROOT" \
  --region-count 4 >> "$test_log" 2>&1 &
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
          --verify-out alan >> "$test_log" 2>&1 ; then
    echo Failed on initial verify seed, check "$test_log"
    ${dsc} cmd shutdown
    exit 1
fi
(( gen += 1 ))

# Now run the crutest replace-reconcole test in a loop
count=1
while [[ $count -le $loops ]]; do
    SECONDS=0
    cp "$test_log" "$test_log".last
    echo "" > "$test_log"
    echo "New loop, $count starts now $(date)" >> "$test_log"
    "$crucible_test" replace-reconcile "${args[@]}" -c 5 \
            --replacement 127.0.0.1:8840 \
            --stable -g "$gen" --verify-out alan \
            --verify-at-start \
            --verify-in alan >> "$test_log" 2>&1
    result=$?
    if [[ $result -ne 0 ]]; then
        touch /var/tmp/ds_test/up 2> /dev/null
        (( err += 1 ))
        duration=$SECONDS
        printf "[%03d] Error $result after %d:%02d\n" "$count" \
                $((duration / 60)) $((duration % 60)) | tee -a "$loop_log"
        mv "$test_log" "$test_log".lastfail
        break
    fi
    duration=$SECONDS
    # Gen should grow by at least the `-c` from crutest
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
    "$err" $duration | tee -a "$loop_log"
    (( count += 1 ))

done
${dsc} cmd shutdown
wait "$dsc_pid"

sleep 4
echo "Final results:" | tee -a "$loop_log"
printf "[%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d last_run_seconds:%d\n" \
  "$count" \
  $((duration / 60)) $((duration % 60)) \
  $((ave / 60)) $((ave % 60)) \
  $((total / 60)) $((total % 60)) \
  "$err" $duration | tee -a "$loop_log"
echo "$(date) Test ends with $err" >> "$test_log" 2>&1
exit "$err"
