#!/bin/bash
#
# Simple script to start three downstairs, then run through all the tests
# that exist on the crucible client program.  This should eventually either
# move to some common test framework, or be thrown away.

set -o pipefail

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

testdir="/tmp/ds_test"
if [[ -d ${testdir} ]]; then
    rm -rf ${testdir}
fi

args=()
downstairs=()
for (( i = 0; i < 3; i++ )); do
    (( port = 3801 + i ))
    dir="${testdir}/$port"
    args+=( -t "127.0.0.1:$port" )
    echo ${cds} -c -p "$port" -d "$dir" --extent-count 5 --extent-size 10
    set -o errexit
    ${cds} -c -p "$port" -d "$dir" --extent-count 5 --extent-size 10 &
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
        res=1
        echo ""
        echo "Failed crucible-client $tt test"
        echo ""
    else
        echo "Completed test: $tt"
    fi
done

echo "Running hammer"
./hammer.sh

echo "Tests have completed, stopping all downstairs"
for pid in ${downstairs[*]}; do
    kill $pid >/dev/null 2>&1
    wait $pid
done

echo ""
if [[ $res != 0 ]]; then
    echo "Tests have failed"
else
    echo "All Tests have passed"
fi
exit $res
