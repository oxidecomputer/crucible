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
cc="$BINDIR/crucible-client"
ch="$BINDIR/crucible-hammer"
for bin in $cds $cc $ch; do
    if [[ ! -f "$bin" ]]; then
        echo "Can't find crucible binary at $bin" >&2
        exit 1
    fi
done

testdir="/var/tmp/test_up"
if [[ -d ${testdir} ]]; then
    rm -rf ${testdir}
fi

# Store log files we want to keep in /tmp/*.txt as this is what
# buildomat will look for and archive
log_prefix="/tmp/test_up"
fail_log="${log_prefix}_fail.txt"
rm -f "$fail_log"

args=()

case ${1} in
    "unencrypted")
        ;;
    "encrypted")
        args+=( --key "$(openssl rand -base64 32)" )
        ;;
    *)
        echo "Usage: $0 encrypted|unencrypted"
        echo " encrypted or unencrypted are the only valid choices"
        exit 1
        ;;
esac

uuidprefix="12345678-1234-1234-1234-00000000"
downstairs=()
port_base=8810

echo "Creating and starting three downstairs"
for (( i = 0; i < 3; i++ )); do
    (( port_step = i * 10 ))
    (( port = port_base + port_step ))
    dir="${testdir}/$port"
    uuid="${uuidprefix}${port}"
    args+=( -t "127.0.0.1:$port" )
    outfile="${log_prefix}-downstairs-${port}-out.txt"
    set -o errexit
    case ${1} in
        "unencrypted")
            echo "$cds" create -u "$uuid" -d "$dir" --extent-count 5 --extent-size 10 | tee "$outfile"
            ${cds} create -u "$uuid" -d "$dir" --extent-count 5 --extent-size 10| tee -a "$outfile"
            ;;
        "encrypted")
            echo "$cds" create -u "$uuid" -d "$dir" --extent-count 5 --extent-size 10 --encrypted=true | tee "$outfile"
            ${cds} create -u "$uuid" -d "$dir" --extent-count 5 --extent-size 10 --encrypted=true | tee -a "$outfile"
            ;;
    esac
    echo "Downstairs output log at $outfile"
    echo "$cds" run -p "$port" -d "$dir" | tee -a "$outfile"
    ${cds} run -p "$port" -d "$dir" > "$outfile" 2>&1 &
    downstairs[$i]=$!
    set +o errexit
done

echo ""
echo "Begin tests"
res=0
test_list="span big dep deactivate balloon"
for tt in ${test_list}; do
    echo "Running test: $tt"
    echo "$cc" "$tt" -q "${args[@]}" >> "${log_prefix}_out.txt"
    if ! "$cc" "$tt" -q "${args[@]}" >> "${log_prefix}_out.txt" 2>&1; then
        (( res += 1 ))
        echo ""
        echo "Failed crucible-client $tt test"
        echo "Failed crucible-client $tt test" >> "$fail_log"
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
echo "$cc" fill -q "${args[@]}"
if ! "$cc" fill -q "${args[@]}"; then
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
echo "$cc" repair -q "${args[@]}"
if ! "$cc" repair -q "${args[@]}"; then
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
ds_pid=$(pgrep -U "$(id -u)" -f "run -p $port")
kill "$ds_pid"

echo rm -rf "${testdir:?}/${port:?}"
echo "Now put back the original so we have a mismatch"
echo mv "${testdir}/previous" "${testdir}/${port}"
rm -rf "${testdir:?}/${port:?}"
mv "${testdir}/previous" "${testdir}/${port}"

outfile="${log_prefix}-downstairs-${port}-out.txt"
echo "Restart downstairs with old directory"
echo "$cds" run -p "$port" -d "${testdir}/$port"
${cds} run -p "$port" -d "${testdir}/$port" >> "$outfile" 2>&1 &
downstairs[4]=$!

echo ""
echo ""
echo "$cc" "$tt" -q "${args[@]}"
if ! "$cc" verify -q "${args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed repair test part 2"
    echo "Failed repair test part 2" >> "$fail_log"
    echo
else
    echo "Repair part 2 passed"
fi

# The dump args look different than other downstairs commands
args=()
for (( i = 0; i < 30; i += 10 )); do
    (( port = port_base + i ))
    dir="${testdir}/$port"
    args+=( -d "$dir" )
done
echo "$cds" dump "${args[@]}"
if ! "$cds" dump "${args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed crucible-client dump test"
    echo "Failed crucible-client dump test" >> "$fail_log"
    echo ""
else
    echo "dump test passed"
fi

echo "$cds" dump "${args[@]}" -e 1
if ! "$cds" dump "${args[@]}" -e 1; then
    (( res += 1 ))
    echo ""
    echo "Failed crucible-client dump test 2"
    echo "Failed crucible-client dump test 2" >> "$fail_log"
    echo ""
else
    echo "dump test 2 passed"
fi

echo "$cds" dump "${args[@]}" -b 20
if ! "$cds" dump "${args[@]}" -b 20 ; then
    (( res += 1 ))
    echo ""
    echo "Failed crucible-client dump test 2"
    echo "Failed crucible-client dump test 2" >> "$fail_log"
    echo ""
else
    echo "dump test 2 passed"
fi

echo "Upstairs tests have completed, stopping all downstairs"
for pid in ${downstairs[*]}; do
    kill $pid >/dev/null 2>&1
    wait $pid
done

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
