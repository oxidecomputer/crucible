#!/bin/bash
#
# Simple script to start three downstairs, then run through all the tests
# that exist on the crucible client program, as well as a few other tests.
# This should eventually either move to some common test framework, or be
# thrown away.

if [[ ${#} -ne 1 ]];
then
    echo "specify either 'unencrypted' or 'encrypted' string"
    exit 1
fi

set -o pipefail
SECONDS=0

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BINDIR=${BINDIR:-$ROOT/target/debug}

echo "$ROOT"
cd "$ROOT"

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

# Downstairs regions go in this directory
testdir="/var/tmp/test_up"
if [[ -d ${testdir} ]]; then
    rm -rf ${testdir}
fi

# Store log files we want to keep in /tmp/*.txt as this is what
# buildomat will look for and archive
test_output_dir="/tmp/test_up"
rm -rf ${test_output_dir} 2> /dev/null
mkdir -p ${test_output_dir}

log_prefix="${test_output_dir}/test_up"
fail_log="${log_prefix}_fail.txt"
rm -f "$fail_log"

args=()
dsc_args=()

case ${1} in
    "unencrypted")
        ;;
    "encrypted")
        upstairs_key=$(openssl rand -base64 32)
        echo "Upstairs using key: $upstairs_key"
        args+=( --key "$upstairs_key" )
        dsc_args+=( --encrypted )
        ;;
    *)
        echo "Usage: $0 encrypted|unencrypted"
        echo " encrypted or unencrypted are the only valid choices"
        exit 1
        ;;
esac

echo "Creating and starting three downstairs"
dsc_output_dir="${test_output_dir}/dsc"
mkdir -p ${dsc_output_dir}
dsc_output="${test_output_dir}/dsc-out.txt"

dsc_args+=( --cleanup --create )
dsc_args+=( --extent-size 10 --extent-count 5 )
dsc_args+=( --output-dir "$dsc_output_dir" )
dsc_args+=( --ds-bin "$cds" )

# Note, this should match the default for DSC
port_base=8810
# Build the upstairs args
for (( i = 0; i < 3; i++ )); do
    (( port_step = i * 10 ))
    (( port = port_base + port_step ))
    args+=( -t "127.0.0.1:$port" )
done

dsc_args+=( --region-dir "$testdir" )
echo "dsc output goes to $dsc_output"
echo "${dsc}" start "${dsc_args[@]}" > "$dsc_output"
"${dsc}" start "${dsc_args[@]}" >> "$dsc_output" 2>&1 &
dsc_pid=$!

sleep 2
if ! pgrep -P $dsc_pid > /dev/null; then
    echo "Gosh diddly darn it, dsc at $dsc_pid did not start"
    echo downstairs:
    ps -ef | grep downstairs
    echo dsc:
    ps -ef | grep dsc
    echo files:
    ls -l /tmp/test_up
    ls -l /tmp/test_up/dsc
    echo cat:
    cat /tmp/test_up/dsc-out.txt
    exit 1
fi

echo ""
echo "Begin tests, output goes to ${log_prefix}_out.txt"
res=0
test_list="span big dep deactivate balloon"
for tt in ${test_list}; do
    echo "Running test: $tt"
    echo "$ct" "$tt" -q "${args[@]}" >> "${log_prefix}_out.txt"
    if ! "$ct" "$tt" -q "${args[@]}" >> "${log_prefix}_out.txt" 2>&1; then
        (( res += 1 ))
        echo ""
        echo "Failed crutest $tt test"
        echo "Failed crutest $tt test" >> "$fail_log"
        echo ""
    else
        echo "Completed test: $tt"
    fi
done

echo "Running hammer"
if ! time "$ch" "${args[@]}" >> "${log_prefix}_out.txt" 2>&1; then
    echo "Failed hammer test"
    echo "Failed hammer test" >> "$fail_log"
    (( res += 1 ))
fi

# Repair test
# This one goes last because it modified the args variable.
# We also test the --verify-* args here as well.
echo "Run repair tests"
args+=( --verify-out "${testdir}/verify_file" )
echo "$ct" fill -q "${args[@]}"
if ! "$ct" fill -q "${args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed setup repair test"
    echo "Failed setup repair test" >> "$fail_log"
    echo
else
    echo "Repair setup passed"
fi

echo "Copy the $port file"

echo cp -r "${testdir}/${port}" "${testdir}/previous"
cp -r "${testdir}/${port}" "${testdir}/previous"

args+=( --verify-in "${testdir}/verify_file" )
echo "$ct" repair -q "${args[@]}"
if ! "$ct" repair -q "${args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed repair test part 1"
    echo "Failed repair test part 1" >> "$fail_log"
    echo
else
    echo "Repair part 1 passed"
fi

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
dump_args=()
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
echo "$ct" "$tt" -q "${args[@]}"
if ! "$ct" verify -q "${args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed repair test part 2"
    echo "Failed repair test part 2" >> "$fail_log"
    echo
else
    echo "Repair part 2 passed"
fi

# Now that repair has finished, make sure the dump command
# does not detect any errors
dump_args=()
for (( i = 0; i < 30; i += 10 )); do
    (( port = port_base + i ))
    dir="${testdir}/$port"
    dump_args+=( -d "$dir" )
done
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
echo "Upstairs tests have completed, stopping all downstairs"
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
else
    echo "All Tests have passed"
fi
duration=$SECONDS
printf "%d:%02d Test duration\n" $((duration / 60)) $((duration % 60))

exit "$res"
