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

echo "$ROOT"
cd "$ROOT"

if pgrep -fl target/debug/crucible-downstairs; then
    echo 'Downstairs already running?' >&2
    exit 1
fi

if ! cargo build; then
    echo "Initial Build failed, no tests ran"
    exit 1
fi

cds="./target/debug/crucible-downstairs"
cc="./target/debug/crucible-client"
if [[ ! -f ${cds} ]] || [[ ! -f ${cc} ]]; then
    echo "Can't find crucible binary at $cds or $cc"
    exit 1
fi

testdir="/var/tmp/test_up"
if [[ -d ${testdir} ]]; then
    rm -rf ${testdir}
fi

uuidprefix="12345678-1234-1234-1234-00000000"
args=()
downstairs=()
port_base=8801
for (( i = 0; i < 3; i++ )); do
    (( port = port_base + i ))
    dir="${testdir}/$port"
    uuid="${uuidprefix}${port}"
    args+=( -t "127.0.0.1:$port" )
    set -o errexit
    case ${1} in
        "unencrypted")
            echo ${cds} create -u "$uuid" -d "$dir" --extent-count 5 --extent-size 10
            ${cds} create -u "$uuid" -d "$dir" --extent-count 5 --extent-size 10
            ;;
        "encrypted")
            echo ${cds} create -u "$uuid" -d "$dir" --extent-count 5 --extent-size 10 --encrypted
            ${cds} create -u "$uuid" -d "$dir" --extent-count 5 --extent-size 10 --encrypted
            ;;
    esac
    echo ${cds} run -p "$port" -d "$dir"
    ${cds} run -p "$port" -d "$dir" &
    downstairs[$i]=$!
    set +o errexit
done

case ${1} in
    "unencrypted")
        ;;
    "encrypted")
        args+=( --key "$(openssl rand -base64 32)" )
        ;;
    *)
        ;;
esac

res=0
test_list="one span big dep deactivate balloon"
for tt in ${test_list}; do
    echo ""
    echo "Running test: $tt"
    echo "$cc" -q -w "$tt" "${args[@]}"
    if ! "$cc" -q -w "$tt" "${args[@]}"; then
        (( res += 1 ))
        echo ""
        echo "Failed crucible-client $tt test"
        echo ""
    else
        echo "Completed test: $tt"
    fi
done

echo "Running hammer"
if ! time cargo run -p crucible-hammer -- \
    "${args[@]}"; then

	echo "Failed hammer test"
    (( res += 1 ))
fi

echo ""
echo "Running verify test: $tt"
vfile="${testdir}/verify"
echo "$cc" -q -w rand --verify-out "$vfile" "${args[@]}"
if ! "$cc" -q -w rand --verify-out "$vfile" "${args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed crucible-client rand verify test"
    echo ""
else
    echo "$cc" -q -w rand --verify-in "$vfile" "${args[@]}"
    if ! "$cc" -q -w rand --verify-in "$vfile" "${args[@]}"; then
        (( res += 1 ))
        echo ""
        echo "Failed crucible-client rand verify part 2 test"
        echo ""
    else
        echo "Verify test passed"
    fi
fi

# The dump args look different than other downstairs commands
args=()
for (( i = 0; i < 3; i++ )); do
    (( port = port_base + i ))
    dir="${testdir}/$port"
    args+=( -d "$dir" )
done
echo "$cds" dump "${args[@]}"
if ! "$cds" dump "${args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed crucible-client dump test"
    echo ""
else
    echo "dump test passed"
fi

args+=( -e 1 )
echo "$cds" dump "${args[@]}"
if ! "$cds" dump "${args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed crucible-client dump test 2"
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
else
    echo "All Tests have passed"
fi
duration=$SECONDS
printf "%d:%02d Test duration\n" $((duration / 60)) $((duration % 60))

exit "$res"
