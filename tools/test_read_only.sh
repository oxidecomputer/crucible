#!/bin/bash
#
# Tests of allowing a read only Upstairs with < 3 running downstairs
# We do an initial RW fill, and record what data we expect
# Then, stop and restart the downstairs as read only.
# Loop over 1 missing downstairs, activate and verify our volume.
# Loop over 2 missing downstairs, activate and verify our volume.
set -o pipefail
SECONDS=0

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BINDIR=${BINDIR:-$ROOT/target/debug}

echo "$ROOT"
cd "$ROOT" || (echo failed to cd "$ROOT"; exit 1)

if pgrep -fl crucible-downstairs; then
    echo 'Downstairs already running?' >&2
    exit 1
fi

cds="$BINDIR/crucible-downstairs"
crutest="$BINDIR/crutest"
dsc="$BINDIR/dsc"
for bin in $cds $crutest $dsc; do
    if [[ ! -f "$bin" ]]; then
        echo "Can't find crucible binary at $bin" >&2
        exit 1
    fi
done

# Use the id of the current user to make a unique path
user=$(id -u -n)
# Downstairs regions go in this directory
testdir="/var/tmp/test_read_only-$user"
if [[ -d "$testdir" ]]; then
    rm -rf "$testdir"
fi

# Store log files we want to keep in /tmp/test_read_only/.txt as this is what
# buildomat will look for and archive
test_output_dir="/tmp/test_read_only-$user"
rm -rf "$test_output_dir" 2> /dev/null
mkdir -p "$test_output_dir"
log_prefix="${test_output_dir}/test_read_only"
fail_log="${log_prefix}_fail.txt"
rm -f "$fail_log"
verify_file="${log_prefix}_verify"
rm -f "$verify_file"
test_log="${log_prefix}_log"
rm -f "$test_log"
dsc_output_dir="${test_output_dir}/dsc"
mkdir -p "$dsc_output_dir"
dsc_output="${test_output_dir}/dsc-out.txt"
echo "dsc output goes to $dsc_output"

region_count=3
args=()
dsc_args=()
dsc_create_args=()
upstairs_key=$(openssl rand -base64 32)
echo "Upstairs using key: $upstairs_key" | tee -a "$test_log"

args+=( --key "$upstairs_key" )
args+=( --dsc "127.0.0.1:9998" )
dsc_create_args+=( --encrypted )
dsc_create_args+=( --cleanup )
dsc_args+=( --output-dir "$dsc_output_dir" )
dsc_args+=( --ds-bin "$cds" )
dsc_args+=( --region-dir "$testdir" )

# Control-C to cleanup.
trap ctrl_c INT
function ctrl_c() {
	echo "Stopping at your request" | tee -a "$test_log"
    ${dsc} cmd shutdown
    exit 1
}

echo "Creating $region_count downstairs regions"
echo "${dsc}" create "${dsc_create_args[@]}" --region-count "$region_count" --extent-size 10 --extent-count 5 "${dsc_args[@]}" > "$dsc_output"
"${dsc}" create "${dsc_create_args[@]}" --region-count "$region_count" --extent-size 10 --extent-count 5 "${dsc_args[@]}" >> "$dsc_output" 2>&1

echo "Starting $region_count downstairs"
echo "${dsc}" start "${dsc_args[@]}" --region-count "$region_count" >> "$dsc_output"
"${dsc}" start "${dsc_args[@]}" --region-count "$region_count" >> "$dsc_output" 2>&1 &
dsc_pid=$!
echo "dsc started at PID: $dsc_pid"

while "$dsc" cmd all-running | grep false > /dev/null ; do
    echo "Wait for all clients to be running."
    sleep 3
done

echo ""
echo "Begin tests, output goes to $test_log"
gen=1

# Initial fill and creation of verify file for future use.
echo "$crutest" generic -g $gen "${args[@]}" --stable --verify-out "$verify_file" -c 250 | tee -a "$test_log"
if ! "$crutest" generic -g $gen "${args[@]}" --stable --verify-out "$verify_file" -c 250 >> "$test_log"; then
    echo "Failed initial fill"
    exit 1
fi
(( gen += 1 ))

echo "Shutdown dsc" | tee -a "$test_log"
"$dsc" cmd shutdown
wait $dsc_pid

echo "Starting dsc in read only mode with $region_count downstairs" | tee -a "$test_log"
echo "${dsc}" start "${dsc_args[@]}" --read-only --region-count $region_count >> "$dsc_output"
"${dsc}" start "${dsc_args[@]}" --read-only --region-count $region_count >> "$dsc_output" 2>&1 &
dsc_pid=$!
echo "dsc started at PID: $dsc_pid" | tee -a "$test_log"

loop=0
while [[ "$loop" -lt 10 ]]; do
    echo "$(date) Begin loop $loop" | tee -a "$test_log"
    # Begin with all downstairs running
    "$dsc" cmd start-all | tee -a "$test_log"

    # In this First loop, we just turn off one downstairs and then verify our
    # volume # with two downstairs running.
    for cid in {0..2}; do

        while "$dsc" cmd all-running | grep false > /dev/null ; do
            echo "Wait for all clients to be running." | tee -a "$test_log"
            sleep 3
        done

        echo "Stop downstairs $cid" | tee -a "$test_log"
        "$dsc" cmd stop -c "$cid" | tee -a "$test_log"

        while ! "$dsc" cmd state -c "$cid" | grep Exit > /dev/null; do
            echo "Waiting for client $cid to stop" | tee -a "$test_log"
            sleep 3
        done

        echo "Run verify test with downstairs $cid stopped" | tee -a "$test_log"
        echo "$crutest" verify -g "$gen" "${args[@]}" -q --verify-in "$verify_file" --read-only | tee -a "$test_log"
        if ! "$crutest" verify -g "$gen" "${args[@]}" -q --verify-in "$verify_file" --read-only >> "$test_log" 2>&1 ; then
            echo "Failed first test at loop $loop, cid: $cid"
            exit 1
        fi

        echo "Start downstairs $cid" | tee -a "$test_log"
        "$dsc" cmd start -c "$cid" | tee -a "$test_log"

    done

    while "$dsc" cmd all-running | grep false > /dev/null ; do
        echo "Wait for all clients to be running." | tee -a "$test_log"
        sleep 3
    done

    # Begin the next loop with all downstairs stopped
    echo "Stopping all downstairs" | tee -a "$test_log"
    "$dsc" cmd stop-all

    # Second loop. Here we just start just one downstairs (leaving all the
    # other downstairs stopped)
    for cid in {0..2}; do

        echo "Start just downstairs $cid" | tee -a "$test_log"
        "$dsc" cmd start -c "$cid"

        # Wait for just our one downstairs to be running.
        for stopped_cid in {0..2}; do
            if [[ "$stopped_cid" -eq "$cid" ]]; then
                continue
            fi
            while ! "$dsc" cmd state -c "$stopped_cid" | grep Exit > /dev/null; do
                echo "Waiting for client $stopped_cid to stop" | tee -a "$test_log"
                sleep 3
            done
        done

        echo "Run read only test with only downstairs $cid running" | tee -a "$test_log"
        echo "$crutest" verify -g "$gen" "${args[@]}" -q --verify-in "$verify_file" --read-only >> "$test_log"
        if ! "$crutest" verify -g "$gen" "${args[@]}" -q --verify-in "$verify_file" --read-only >> "$test_log" 2>&1 ; then
            echo "Failed second test at loop $loop, cid: $cid"
            exit 1
        fi

        # stop downstairs $cid
        echo "Stop downstairs $cid" | tee -a "$test_log"
        "$dsc" cmd stop -c "$cid"

    done
    echo "$(date) End of loop $loop" | tee -a "$test_log"
    ((loop += 1))
done

## loop done
echo "Shutdown dsc" | tee -a "$test_log"
"$dsc" cmd shutdown
wait $dsc_pid

duration=$SECONDS
printf "Test with %d loops took: %d:%02d\n" \
    "$loop" $((duration / 60)) $((duration % 60)) | tee -a "$test_log"
