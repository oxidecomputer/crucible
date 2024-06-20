#!/bin/bash

# Test the repair process while the downstairs are restarting.

# The actual repair is pretty straightforward, as it's all just the
# generation number, what we are really testing here is the ability for
# the overall repair process on the upstairs to be able to handle
# downstairs going away and coming back and that eventually the repair
# will finish and verify the data is as we expect.

err=0
total=0
pass_total=0
SECONDS=0

# Control-C to cleanup.
trap ctrl_c INT
function ctrl_c() {
    echo "Stopping the test"
    if [[ -n "$dsc" ]]; then
        "$dsc" cmd shutdown > /dev/null 2>&1 || true
    fi
    exit 1
}

# Bring all downstairs online.
function bring_all_downstairs_online() {
    # dsc start all downstairs
    if ! "$dsc" cmd start-all; then
        echo "dsc: Failed to stop all downstairs"
        exit 1
    fi

    # dsc turn on automatic restart
    if ! "$dsc" cmd enable-restart-all; then
        echo "dsc: Failed to disable automatic restart"
        exit 1
    fi
}

# Stop all downstairs.
function stop_all_downstairs() {
    # dsc turn off automatic restart
    if ! "$dsc" cmd disable-restart-all; then
        echo "dsc: Failed to disable automatic restart"
        exit 1
    fi

    # dsc stop all downstairs
    if ! "$dsc" cmd stop-all; then
        echo "dsc: Failed to stop all downstairs"
        exit 1
    fi
}

ROOT=$(cd "$(dirname "$0")/.." && pwd)
export BINDIR=${BINDIR:-$ROOT/target/debug}

cds="$BINDIR/crucible-downstairs"
ct="$BINDIR/crutest"
dsc="$BINDIR/dsc"
for bin in $cds $ct $dsc; do
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

loops=10

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

WORK_ROOT=${WORK_ROOT:-/tmp}
export loop_log="$WORK_ROOT/repair_restart.log"
export test_log="$WORK_ROOT/repair_restart_test.log"
export dsc_log="$WORK_ROOT/repair_restart_dsc.log"
REGION_ROOT=${REGION_ROOT:-/var/tmp/test_restart_repair}

echo "" > "$loop_log"
echo "starting $(date)" | tee "$loop_log"
echo "" > "$test_log"
echo "Tail $test_log for test output"
echo "Tail $loop_log for summary output"
echo "Tail $dsc_log for dsc outout"

echo "Create a new region to test" | tee -a "${loop_log}"
ulimit -n 65536
if ! "$dsc" create --cleanup --ds-bin "$cds" --extent-count 61 --extent-size 5120 --region-dir "$REGION_ROOT"; then
    echo "Failed to create region at $REGION_ROOT"
    exit 1
fi

echo "Starting the downstairs" | tee -a "${loop_log}"
"$dsc" start --ds-bin "$cds" --region-dir "$REGION_ROOT" >> "$dsc_log" 2>&1 &
dsc_pid=$!
# Sleep 5 to give the downstairs time to get going.
sleep 5
if ! pgrep -P $dsc_pid; then
    echo "Failed to start dsc" | tee -a "${loop_log}"
    exit 1
else
    echo "Downstairs are now running" | tee -a "${loop_log}"
fi

os_name=$(uname)
if [[ "$os_name" == 'Darwin' ]]; then
    # stupid macos needs this to avoid popup hell.
    codesign -s - -f "$cds"
    codesign -s - -f "$ct"
fi

args=()
port_base=8810
for (( i = 0; i < 30; i += 10 )); do
    (( port = port_base + i ))
    args+=( -t "127.0.0.1:$port" )
done

gen=1
# Send something to the region so our old region files have data.
echo "$(date) pre-fill" >> "$test_log"
echo "$(date) run pre-fill of our region" | tee -a "$loop_log"
echo "$ct" fill "${args[@]}" --stable -g "$gen" >> "$test_log"
"$ct" fill "${args[@]}" --stable -g "$gen" >> "$test_log" 2>&1
if [[ $? -ne 0 ]]; then
    echo "Error in initial pre-fill"
    ctrl_c
fi
(( gen += 1 ))


echo "$(date) Stopping all downstairs" | tee -a "$loop_log"
stop_all_downstairs

# Give "pause" time to stop all running downstairs.
# We need to do this before moving the region directory out from
# under a downstairs, otherwise it can fail and exit and the
# downstairs daemon will think it is a real failure.
sleep 7

# Create the "old" region files
rm -rf "$REGION_ROOT"/8810.old "$REGION_ROOT"/8820.old "$REGION_ROOT"/8830.old
cp -R  "$REGION_ROOT"/8810 "$REGION_ROOT"/8810.old || ctrl_c
cp -R  "$REGION_ROOT"/8820 "$REGION_ROOT"/8820.old || ctrl_c
cp -R  "$REGION_ROOT"/8830 "$REGION_ROOT"/8830.old || ctrl_c

# Bring the downstairs back online.
echo "$(date) Bring downstairs back online" | tee -a "$loop_log"
bring_all_downstairs_online

# Now do second seed for verify file, this will make sure to have
# different data in current vs. old region directories.
echo "$(date) Run a second fill test" >> "$test_log"
echo "$(date) Run a second fill test" | tee -a "$loop_log"
echo "$ct" fill "${args[@]}" --stable -g "$gen" --verify-out alan >> "$test_log"
"$ct" fill "${args[@]}" --stable -g "$gen" --verify-out alan >> "$test_log" 2>&1
if [[ $? -ne 0 ]]; then
    echo "Error in initial fill"
    ctrl_c
fi
(( gen += 1 ))

echo "Fill completed, wait for downstairs to start restarting" >> "$test_log"
duration=$SECONDS
printf "Initial fill and verify took: %d:%02d \n" \
    $((duration / 60)) $((duration % 60)) | tee -a "$loop_log"

# Now run the repair loop
count=1
while [[ $count -le $loops ]]; do
    SECONDS=0
    echo "" >> "$test_log"
    echo "" >> "$test_log"
    echo "$(date) New loop starts now" >> "$test_log"
    stop_all_downstairs

    # Give "pause" time to stop all running downstairs.
    # We need to do this before moving the region directory out from
    # under a downstairs, otherwise it can fail and exit and the
    # downstairs daemon will think it is a real failure.
    sleep 5

    echo "$(date) move regions" >> "$test_log"
    choice=$((RANDOM % 3))
    if [[ $choice -eq 0 ]]; then
        rm -rf "$REGION_ROOT"/8810
        cp -R  "$REGION_ROOT"/8810.old "$REGION_ROOT"/8810
    elif [[ $choice -eq 1 ]]; then
        rm -rf "$REGION_ROOT"/8820
        cp -R  "$REGION_ROOT"/8820.old "$REGION_ROOT"/8820
    else
        rm -rf "$REGION_ROOT"/8830
        cp -R  "$REGION_ROOT"/8830.old "$REGION_ROOT"/8830
    fi
    echo "$(date) regions moved, current dump outputs:" >> "$test_log"
    $cds dump --no-color -d "$REGION_ROOT"/8810 \
        -d "$REGION_ROOT"/8820 \
        -d "$REGION_ROOT"/8830 >> "$test_log" 2>&1

    echo "$(date) resume downstairs" >> "$test_log"
    bring_all_downstairs_online

    # dsc turn on random stop of any downstairs.
    if ! "$dsc" cmd enable-random-stop; then
        echo "dsc: Failed to enable random restart"
        exit 1
    fi

    echo "$(date) do one IO" >> "$test_log"
    "$ct" one "${args[@]}" \
            -q -g "$gen" --verify-out alan \
            --verify-in alan \
            --verify-at-start \
            --retry-activate >> "$test_log" 2>&1
    result=$?
    if [[ $result -ne 0 ]]; then
        (( err += 1 ))
        duration=$SECONDS
        printf "[%03d] Error $result in one test after %d:%02d\n" "$count" \
                $((duration / 60)) $((duration % 60)) | tee -a "$loop_log"
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
"$err" $duration | tee -a "$loop_log"
    (( count += 1 ))

done
"$dsc" cmd shutdown
if [[ -n "$dsc_pid" ]]; then
    wait "$dsc_pid"
fi

echo "Final results $(date):" | tee -a "$loop_log"
printf "[%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d last_run_seconds:%d\n" \
  "$count" $((duration / 60)) $((duration % 60)) \
  $((ave / 60)) $((ave % 60)) \
  $((total / 60)) $((total % 60)) \
  "$err" $duration | tee -a "$loop_log"
exit "$err"
