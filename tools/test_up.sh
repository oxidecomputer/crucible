#!/bin/bash
#
# Simple script to start three downstairs, then run through all the tests
# that exist on the crucible client program, as well as a few other tests.
# This should eventually either move to some common test framework, or be
# thrown away.

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
ct="$BINDIR/crutest"
ch="$BINDIR/crucible-hammer"
dsc="$BINDIR/dsc"
for bin in $cds $ct $ch $dsc; do
    if [[ ! -f "$bin" ]]; then
        echo "Can't find crucible binary at $bin" >&2
        exit 1
    fi
done

# Control-C to cleanup.
trap ctrl_c INT
function ctrl_c() {
	echo "Stopping at your request"
    ${dsc} cmd shutdown
    exit 1
}

# Use the id of the current user to make a unique path
user=$(id -u -n)
# Downstairs regions go in this directory
testdir="/var/tmp/test_up-$user"
if [[ -d "$testdir" ]]; then
    rm -rf "$testdir"
fi

# Store log files we want to keep in /tmp/test_up/.txt as this is what
# buildomat will look for and archive
test_output_dir="/tmp/test_up-$user"
rm -rf "$test_output_dir" 2> /dev/null
mkdir -p "$test_output_dir"

log_prefix="${test_output_dir}/test_up"
fail_log="${log_prefix}_fail.txt"
rm -f "$fail_log"

args=()
hammer_args=()
dsc_args=()
dsc_create_args=()
dump_args=()
region_sets=1

function usage {
    echo "Usage: $0 [-N] [-r #] encrypted|unencrypted"
    echo "N:    Don't dump color output"
    echo "r: #  Number of region sets to create (default: 1)"
    echo "    encrypted or unencrypted must be provided"
    exit 1
}

shift_count=0
while getopts 'Nr:' opt; do
    case "$opt" in
        N) echo "Turn off color for downstairs dump"
            dump_args+=("--no-color")
            ((shift_count+=1))
            ;;
        r) region_sets=$OPTARG
            echo "Using $region_sets region sets"
            ((shift_count+=2))
            ;;
        *) usage
            ;;
    esac
done
shift "$shift_count"

case ${1} in
    "unencrypted")
        ;;
    "encrypted")
        upstairs_key=$(openssl rand -base64 32)
        echo "Upstairs using key: $upstairs_key"
        args+=( --key "$upstairs_key" )
        hammer_args+=( --key "$upstairs_key" )
        dsc_create_args+=( --encrypted )
        ;;
    *)
        echo "Unsupported option: $1"
        usage
        ;;
esac

((region_count=region_sets*3))
args+=( --dsc "127.0.0.1:9998" )

dsc_output_dir="${test_output_dir}/dsc"
mkdir -p "$dsc_output_dir"
dsc_output="${test_output_dir}/dsc-out.txt"

dsc_create_args+=( --cleanup )
dsc_args+=( --output-dir "$dsc_output_dir" )
dsc_args+=( --ds-bin "$cds" )
dsc_args+=( --region-dir "$testdir" )
echo "dsc output goes to $dsc_output"

echo "Creating $region_count downstairs regions"
echo "${dsc}" create "${dsc_create_args[@]}" --region-count $region_count --extent-size 10 --extent-count 5 "${dsc_args[@]}" > "$dsc_output"
"${dsc}" create "${dsc_create_args[@]}" --region-count $region_count --extent-size 10 --extent-count 5 "${dsc_args[@]}" >> "$dsc_output" 2>&1

echo "Starting $region_count downstairs"
echo "${dsc}" start "${dsc_args[@]}" --region-count $region_count >> "$dsc_output"
"${dsc}" start "${dsc_args[@]}" --region-count $region_count >> "$dsc_output" 2>&1 &
dsc_pid=$!

sleep 5
if ! pgrep -P $dsc_pid > /dev/null; then
    echo "Gosh diddly darn it, dsc at $dsc_pid did not start"
    echo downstairs:
    ps -ef | grep downstairs
    echo dsc:
    ps -ef | grep dsc
    echo files:
    ls -l "$test_output_dir"
    ls -l "$dsc_output_dir"
    echo cat:
    cat "$dsc_output"
    exit 1
fi

echo "dsc started at PID: $dsc_pid"
# We don't want auto-restart of downstairs, so be sure that is not enabled.
echo "Disable automatic restart on all downstairs"
if ! "${dsc}" cmd disable-restart-all; then
    echo "Failed to disable auto restart"
    exit 1
fi

echo ""
echo "Begin tests, output goes to ${log_prefix}_out.txt"
res=0
gen=1
# Keep deactivate test last, and add 15 to the generation number so
# tests that follow will be able to activate.
test_list="span big dep balloon deactivate"
for tt in ${test_list}; do
    # Add a blank line in the output file as all the output follows.
    echo "" >> "${log_prefix}_out.txt"
    echo "Running test: $tt" | tee -a "${log_prefix}_out.txt"
    echo "Running test: $tt" >> "${log_prefix}_out.txt"
    echo "$ct" "$tt" -g "$gen" --verify-at-end --stable "${args[@]}" >> "${log_prefix}_out.txt"
    if ! "$ct" "$tt" -g "$gen" --verify-at-end --stable "${args[@]}" >> "${log_prefix}_out.txt" 2>&1; then
        (( res += 1 ))
        echo ""
        echo "Failed crutest $tt test"
        echo "Failed crutest $tt test" >> "${log_prefix}_out.txt"
        echo "Failed crutest $tt test" >> "$fail_log"
        echo ""
    else
        echo "Completed test: $tt"
    fi
    (( gen += 2 ))
    # Yes, this is bad.  Sometimes we just need a little time for the test
    # to finish everything before the next one starts.
    sleep 5
done
(( gen += 10 ))

# Hammer tests only uses three downstairs.
echo "" >> "${log_prefix}_out.txt"
echo "Running hammer" | tee -a "${log_prefix}_out.txt"
hammer_args+=( -t "127.0.0.1:8810")
hammer_args+=( -t "127.0.0.1:8820")
hammer_args+=( -t "127.0.0.1:8830")
echo "$ch" -g "$gen" "${hammer_args[@]}" >> "${log_prefix}_out.txt"
if ! "$ch" -g "$gen" "${hammer_args[@]}" >> "${log_prefix}_out.txt" 2>&1; then
    echo "Failed hammer test"
    echo "Failed hammer test" >> "$fail_log"
    (( res += 1 ))
fi
# The hammer tests also updates the generation number.
(( gen += 10 ))

# Repair test
# This one goes last because it modified the args variable.
# We also test the --verify-* args here as well.
echo "" >> "${log_prefix}_out.txt"
echo "Run repair tests" | tee -a "${log_prefix}_out.txt"

new_args=( "${args[@]}" )
new_args+=( --verify-out "${testdir}/verify_file" )
echo "$ct" fill -g "$gen" -q "${new_args[@]}" | tee -a "${log_prefix}_out.txt"

if ! "$ct" fill -g "$gen" -q "${new_args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed setup repair test" | tee -a "${log_prefix}_out.txt"

    echo "Failed setup repair test" >> "$fail_log"
    echo
else
    echo "Repair setup passed" | tee -a "${log_prefix}_out.txt"

fi
(( gen += 1 ))

port=8830
echo "Copy the region for ${testdir}/$port" | tee -a "${log_prefix}_out.txt"

echo cp -r "${testdir}/${port}" "${testdir}/previous"
cp -r "${testdir}/${port}" "${testdir}/previous"

new_args+=( --verify-in "${testdir}/verify_file" )
echo "$ct" fill -g "$gen" -q "${new_args[@]}"
if ! "$ct" fill -g "$gen" -q "${new_args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed repair test part 1"
    echo "Failed repair test part 1" >> "$fail_log"
    echo
else
    echo "Repair part 1 passed"
fi
(( gen += 1 ))

echo ""

echo Kill the current downstairs
if ! "$dsc" cmd disable-restart-all; then
    (( res += 1 ))
    echo ""
    echo "Failed repair test part 1, disable auto restart"
    echo "Failed repair test part 1, disable auto restart" >> "$fail_log"
    echo
fi

if ! "$dsc" cmd stop -c 2; then
    (( res += 1 ))
    echo ""
    echo "Failed repair test part 1, stopping downstairs 2"
    echo "Failed repair test part 1, stopping downstairs 2" >> "$fail_log"
    echo
fi

echo rm -rf "${testdir:?}/${port:?}"
echo "Now put back the original so we have a mismatch"
echo mv "${testdir}/previous" "${testdir}/${port}"
rm -rf "${testdir:?}/${port:?}"
mv "${testdir}/previous" "${testdir}/${port}"

echo "Restart downstairs with old directory"

if ! "$dsc" cmd start -c 2; then
    (( res += 1 ))
    echo ""
    echo "Failed repair test part 1, starting downstairs 2"
    echo "Failed repair test part 1, starting downstairs 2" >> "$fail_log"
    echo
fi

# Put a dump test in the middle of the repair test, so we
# can see both a mismatch and that dump works.
# The dump args look different than other downstairs commands
# This port base comes from the default for dsc.  If that changes, then this
# needs to change as well.
port_base=8810
for (( i = 0; i < 30; i += 10 )); do
    (( port = port_base + i ))
    dir="${testdir}/$port"
    dump_args+=( -d "$dir" )
done
echo "$cds" dump "${dump_args[@]}"
# This should return error (a mismatch!)
if "$cds" dump "${dump_args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "repair dump test did not find the expected error"
    echo "repair dump test did not find the expected error" >> "$fail_log"
    echo ""
else
    echo "dump test found error as expected"
fi

echo ""
echo ""
echo "$ct" verify --range -g "$gen" -q "${new_args[@]}"
if ! "$ct" verify --range -g "$gen" -q "${new_args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed repair test part 2"
    echo "Failed repair test part 2" >> "$fail_log"
    echo
else
    echo "Repair part 2 passed"
fi
(( gen += 1 ))

# Now that repair has finished, make sure the dump command does not detect
# any errors
echo "$cds" dump "${dump_args[@]}"
if ! "$cds" dump "${dump_args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed crutest dump test"
    echo "Failed crutest dump test" >> "$fail_log"
    echo ""
else
    echo "dump test passed"
fi

echo "$cds" dump "${dump_args[@]}" -e 1
if ! "$cds" dump "${dump_args[@]}" -e 1; then
    (( res += 1 ))
    echo ""
    echo "Failed crutest dump extent"
    echo "Failed crutest dump extent" >> "$fail_log"

    echo ""
else
    echo "dump extent test passed"
fi

echo "$cds" dump "${dump_args[@]}" -b 20
if ! "$cds" dump "${dump_args[@]}" -b 20 ; then
    (( res += 1 ))
    echo ""
    echo "Failed crutest dump block 20"
    echo "Failed crutest dump block 20" >> "$fail_log"
    echo ""
else
    echo "dump block test passed"
fi

# Tests done, shut down the downstairs.
echo "Initial upstairs tests have completed, stopping all downstairs"
if ! "$dsc" cmd shutdown; then
    (( res += 1 ))
    echo ""
    echo "Failed dsc shutdown"
    echo "Failed dsc shutdown" >> "$fail_log"
    echo
fi

# Loop till dsc has stopped
sleep 5
while : ; do
    if ! pgrep -P $dsc_pid > /dev/null; then
        break
    fi
    echo "dsc at $dsc_pid has not yet stopped, waiting"
    "$dsc" cmd shutdown
    sleep 5
done

echo "Begin replace reconcile test" >> "${log_prefix}_out.txt"

# Create a larger region size for these tests, and add an additional
# region so we have one to use for replacement.
((region_count+=1))
echo "Creating $region_count larger downstairs regions"
echo "${dsc}" create "${dsc_create_args[@]}" --region-count "$region_count" --extent-count 300 --block-size 4096 "${dsc_args[@]}" >> "$dsc_output"
"${dsc}" create "${dsc_create_args[@]}" --region-count "$region_count" --extent-count 100 --block-size 4096 "${dsc_args[@]}" >> "$dsc_output" 2>&1

echo "Starting $region_count downstairs"
echo "${dsc}" start --region-count $region_count "${dsc_args[@]}" >> "$dsc_output"
"${dsc}" start --region-count "$region_count" "${dsc_args[@]}" >> "$dsc_output" 2>&1 &
dsc_pid=$!

sleep 5
if ! pgrep -P $dsc_pid > /dev/null; then
    echo "larger dsc at $dsc_pid failed to start"
    exit 1
fi
echo "" >> "${log_prefix}_out.txt"

echo "dsc restarted at PID: $dsc_pid"
echo "Now do the replace-reconcile test"

# Get the port number for the last client, that is our replacement.
((last_client = region_count - 1))
last_port=$("$dsc" cmd port -c "$last_client")
echo "Using $last_port for the replacement port"

echo "$ct" replace-reconcile --replacement 127.0.0.1:"$last_port" -c 4 -g "$gen" --stable "${args[@]}" >> "${log_prefix}_out.txt"
if ! "$ct" replace-reconcile --replacement 127.0.0.1:"$last_port" -c 4 -g "$gen" --stable "${args[@]}" >> "${log_prefix}_out.txt" 2>&1; then
    (( res += 1 ))
    echo ""
    echo "Failed crutest replace-reconcile test"
    echo "Failed crutest replace-reconcile test" >> "${log_prefix}_out.txt"
    echo "Failed crutest replace-reconcile test" >> "$fail_log"
    echo ""
else
    echo "Completed test: replace-reconcile"
fi

((gen += 20))
echo "Now do the replace-before-active test"
echo "$ct" replace-before-active --replacement 127.0.0.1:"$last_port" -c 4 -g "$gen" --stable "${args[@]}" >> "${log_prefix}_out.txt"
if ! "$ct" replace-before-active --replacement 127.0.0.1:"$last_port" -c 4 -g "$gen" --stable "${args[@]}" >> "${log_prefix}_out.txt" 2>&1; then
    (( res += 1 ))
    echo ""
    echo "Failed crutest replace-before-active"
    echo "Failed crutest replace-before-active" >> "${log_prefix}_out.txt"
    echo "Failed crutest replace-before-active" >> "$fail_log"
    echo ""
else
    echo "Completed test: replace-before-active"
fi

# Tests done, shut down the downstairs.
echo "All tests have completed, stopping all downstairs"
if ! "$dsc" cmd shutdown; then
    (( res += 1 ))
    echo ""
    echo "Failed dsc shutdown"
    echo "Failed dsc shutdown" >> "$fail_log"
    echo
fi

echo ""
if [[ $res != 0 ]]; then
    echo "$res Tests have failed"
    cat "$fail_log"
    echo "check output in ${log_prefix}_out.txt"
else
    echo "All Tests have passed"
fi
duration=$SECONDS
printf "%d:%02d Test duration\n" $((duration / 60)) $((duration % 60))

exit "$res"
