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
    rm -f /var/tmp/ds_test/up
    rm -f /var/tmp/ds_test/pause
    touch /var/tmp/ds_test/stop
    sleep 5
    if [[ -n "$dsd_pid" ]]; then
        kill "$dsd_pid"
    fi
    exit 1
}

loop_log=/tmp/repair_restart.log
test_log=/tmp/repair_restart_test.log

echo "" > ${loop_log}
echo "starting $(date)" | tee ${loop_log}
echo "Tail $test_log for test output"

ROOT=$(cd "$(dirname "$0")/.." && pwd)
export BINDIR=${BINDIR:-$ROOT/target/debug}

cds="$BINDIR/crucible-downstairs"
cc="$BINDIR/crucible-client"
dsc="$BINDIR/dsc"
for bin in $cds $cc $dsc; do
    if [[ ! -f "$bin" ]]; then
        echo "Can't find crucible binary at $bin" >&2
        exit 1
    fi
done

echo "Create a new region to test" | tee "${loop_log}"
ulimit -n 65536
if ! "$dsc" create --cleanup --ds-bin "$cds" --extent-count 61 --extent-size 5120 --region-dir ./var; then
    echo "Failed to create region"
    exit 1
fi

# Create the "old" region files
rm -rf var/8810.old var/8820.old var/8830.old
cp -R  var/8810 var/8810.old || ctrl_c
cp -R  var/8820 var/8820.old || ctrl_c
cp -R  var/8830 var/8830.old || ctrl_c

./tools/downstairs_daemon.sh -u >> "$test_log" 2>&1 &
dsd_pid=$!

# Sleep 5 to give the downstairs time to get going.
sleep 5

os_name=$(uname)
if [[ "$os_name" == 'Darwin' ]]; then
    # stupid macos needs this to avoid popup hell.
    codesign -s - -f "$cds"
    codesign -s - -f "$cc"
fi

args=()
port_base=8810
for (( i = 0; i < 30; i += 10 )); do
    (( port = port_base + i ))
    args+=( -t "127.0.0.1:$port" )
done

# Initial seed for verify file
echo "$(date) fill" >> "$test_log"
echo "$cc" fill "${args[@]}" \
      -q --verify-out alan >> "$test_log"
"$cc" fill "${args[@]}" \
      -q --verify-out alan >> "$test_log" 2>&1
if [[ $? -ne 0 ]]; then
    echo "Error in initial fill"
    ctrl_c
fi

echo "Fill completed, wait for downstairs to start restarting" >> "$test_log"
rm -f /var/tmp/ds_test/up

duration=$SECONDS
printf "Initial fill and verify took: %d:%02d \n" \
    $((duration / 60)) $((duration % 60)) | tee -a ${loop_log}

# Now run the repair loop
for i in {1..10}
do
    SECONDS=0
    echo "" >> "$test_log"
    echo "" >> "$test_log"
    echo "$(date) New loop starts now" >> "$test_log"
    touch /var/tmp/ds_test/pause
    # Give "pause" time to stop all running downstairs.
    # We need to do this before moving the region directory out from
    # under a downstairs, otherwise it can fail and exit and the
    # downstairs daemon will think it is a real failure.
    sleep 5

    echo "$(date) move regions" >> "$test_log"
    choice=$((RANDOM % 3))
    if [[ $choice -eq 0 ]]; then
        rm -rf var/8810
        cp -R  var/8810.old var/8810
    elif [[ $choice -eq 1 ]]; then
        rm -rf var/8820
        cp -R  var/8820.old var/8820
    else
        rm -rf var/8830
        cp -R  var/8830.old var/8830
    fi
    echo "$(date) regions moved, current dump output:" >> "$test_log"
    cdump.sh >> "$test_log" 2>&1
    echo "$(date) resume downstairs" >> "$test_log"
    rm /var/tmp/ds_test/pause

    echo "$(date) do one IO" >> "$test_log"
    "$cc" one "${args[@]}" \
            -q --verify-out alan \
            --verify-in alan \
            --verify \
            --retry-activate >> "$test_log" 2>&1
    result=$?
    if [[ $result -ne 0 ]]; then
        touch /var/tmp/ds_test/up 2> /dev/null
        (( err += 1 ))
        duration=$SECONDS
        printf "[%03d] Error $result in one test after %d:%02d\n" "$i" \
                $((duration / 60)) $((duration % 60)) | tee -a ${loop_log}
        mv "$test_log" "$test_log".lastfail
        break
    fi

    # XXX This check is here because we don't yet have a way of getting
    # error status from the upstairs to indicate this has happened.
    if grep "read hash mismatch" "$test_log"; then
        touch /var/tmp/ds_test/up 2> /dev/null
        (( err += 1 ))
        duration=$SECONDS
        printf "[%03d] Hash fail in one test after %d:%02d\n" "$i" \
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
rm -f /var/tmp/ds_test/up
rm -f /var/tmp/ds_test/pause
echo "Final results $(date):" | tee -a ${loop_log}
printf "[%03d] %d:%02d  ave:%d:%02d  total:%d:%02d errors:%d last_run_seconds:%d\n" "$i" $((duration / 60)) $((duration % 60)) $((ave / 60)) $((ave % 60)) $((total / 60)) $((total % 60)) "$err" $duration | tee -a ${loop_log}
exit "$err"

