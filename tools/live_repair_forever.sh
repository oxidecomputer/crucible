#!/bin/bash

# Create and start up the downstairs.
# In a loop:
#  Send IO through crutest
#  Fault a downstairs
#  Let the upstairs repair.

err=0
total=0
pass_total=0
SECONDS=0
dropshot=0
ulimit -n 65536

test_log=/tmp/live_repair_forever.log
dsc_test_log=/tmp/live_repair_forever_dsc.log

# Control-C to cleanup.
trap cleanup INT
function cleanup() {
    echo "Stopping at your request"
    res=0
    # Stop crutest
    if ps -p "$crutest_pid" > /dev/null; then
        echo "Sending stop to $crutest_pid"
        kill -SIGUSR1 $crutest_pid
        wait ${crutest_pid}
        result=$?
        if [[ $result -ne 0 ]]; then
            if tail "$test_log" | grep dropshot > /dev/null ; then
                (( dropshot += 1 ))
            else
                echo "Error $result on shutdown\n"
                res=$result
            fi
        fi
    fi
    if ps -p "$dsc_pid" > /dev/null; then
        echo "Stop dsc at $dsc_pid"
        ${dsc} cmd shutdown
        wait "$dsc_pid"
    fi
    exit $res
}

ROOT=$(cd "$(dirname "$0")/.." && pwd)
export BINDIR=${BINDIR:-$ROOT/target/release}
cds="$BINDIR/crucible-downstairs"
crucible_test="$BINDIR/crutest"
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

echo "" > ${test_log}
echo "" > ${dsc_test_log}

echo "starting $(date)" | tee ${test_log}
echo "Tail $test_log for test output"

# Large extents, but not many of them means we are likely to try to write
# to an extent that is being repaired.
if ! ${dsc} create --cleanup \
  --ds-bin "$cds" \
  --extent-count 100 \
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

if ! ${dsc} cmd enable-restart-all >> "$dsc_test_log" 2>&1; then
    echo "Failed to enable random dsc restart"
    cleanup
fi

args=()
args+=( -t "127.0.0.1:8810" )
args+=( -t "127.0.0.1:8820" )
args+=( -t "127.0.0.1:8830" )

# Run IO workload
"$crucible_test" generic "${args[@]}" --continuous \
	--stable -g 1 --control 127.0.0.1:7777 >> "$test_log" 2>&1 &
crutest_pid=$!

sleep 5
count=1;
if ! ps -p $crutest_pid > /dev/null; then
    echo "$crucible_test failed to start"
    exit 1
fi

while :; do
    SECONDS=0
    choice=$((RANDOM % 3))
    sleep 5

    # Fault a downstairs.
    if ! ${dsc} cmd stop -c $choice >> "$dsc_test_log" 2>&1; then
        echo "Failed to stop random dsc $choice"
        cleanup
    fi

    # Give the fault time to be noticed
    sleep 5

	# Now wait for all downstairs to be active
    all_state=$(curl -s http://127.0.0.1:7777/info | awk -F\" '{print $8","$10","$12}')
    while [[ "${all_state}" != "active,active,active" ]]; do
        if ! ps -p $crutest_pid > /dev/null; then
            echo "$crucible_test no longer running"
            cleanup
        fi
        sleep 5
        all_state=$(curl -s http://127.0.0.1:7777/info | awk -F\" '{print $8","$10","$12}')
    done

    # At 100 loops, clear the log so we don't take all space in /tmp
    (( mc = count % 100))
    if [[ $mc -eq 0 ]]; then
        echo "" > "$test_log"
        echo "" > "$dsc_test_log"
    fi
    duration=$SECONDS
    (( pass_total += 1 ))
    (( total += duration ))
    ave=$(( total / pass_total ))
    printf \
      "[%03d][%d] %d:%02d  ave:%d:%02d total:%d:%02d last_run:%d\n" \
      "$count" "$choice" \
      $((duration / 60)) $((duration % 60)) \
      $((ave / 60)) $((ave % 60))  $((total / 60)) $((total % 60)) \
      "$duration"

    (( count += 1 ))
done

