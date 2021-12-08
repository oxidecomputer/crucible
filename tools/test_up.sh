#!/bin/bash
#
# Simple script to start three downstairs, then run through all the tests
# that exist on the crucible client program.  This should eventually either
# move to some common test framework, or be thrown away.

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
    echo ${cds} create -u "$uuid" -p "$port" -d "$dir" --extent-count 5 --extent-size 10
    set -o errexit
    ${cds} create -u "$uuid" -d "$dir" --extent-count 5 --extent-size 10
    echo ${cds} run -p "$port" -d "$dir"
    ${cds} run -p "$port" -d "$dir" &
    downstairs[$i]=$!
    set +o errexit
done

res=0
test_list="one big dep rand balloon"
for tt in ${test_list}; do
    echo ""
    echo "Running test: $tt"
    echo cargo run -p crucible-client -- -q -w "$tt" "${args[@]}"
    if ! cargo run -p crucible-client -- -q -w "$tt" "${args[@]}"; then
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
    "${args[@]}" \
    --key "$(openssl rand -base64 32)"; then

	echo "Failed hammer test"
    (( res += 1 ))
fi

echo ""
echo "Running verify test: $tt"
vfile="${testdir}/verify"
echo cargo run -p crucible-client -- -q -w rand --verify-out "$vfile" "${args[@]}"
if ! cargo run -p crucible-client -- -q -w rand --verify-out "$vfile" "${args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed crucible-client rand verify test"
    echo ""
else
    echo cargo run -p crucible-client -- -q -w rand --verify-in "$vfile" "${args[@]}"
    if ! cargo run -p crucible-client -- -q -w rand --verify-in "$vfile" "${args[@]}"; then
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
echo cargo run -p crucible-downstairs -- dump "${args[@]}"
if ! cargo run -p crucible-downstairs -- dump "${args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed crucible-client dump test"
    echo ""
else
    echo "dump test passed"
fi

args+=( -e 1 )
echo cargo run -p crucible-downstairs -- dump "${args[@]}"
if ! cargo run -p crucible-downstairs -- dump "${args[@]}"; then
    (( res += 1 ))
    echo ""
    echo "Failed crucible-client dump test 2"
    echo ""
else
    echo "dump test 2 passed"
fi

echo "Tests have completed, stopping all downstairs"
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
printf "%d:%2d Test duration\n" $((duration / 60)) $((duration % 60))

exit "$res"
