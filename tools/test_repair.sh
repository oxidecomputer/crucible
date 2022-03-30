#!/usr/bin/env bash

cargo build || echo "Failed to build"

cds="./target/debug/crucible-downstairs"
cc="./target/debug/crucible-client"
if [[ ! -f ${cds} ]] || [[ ! -f ${cc} ]]; then
    echo "Can't find crucible binaries at $cds or $cc"
    exit 1
fi

# start all three downstairs
${cds} run -d var/8810 -p 8810 &> /tmp/ds1 &
ds1_pid=$!
${cds} run -d var/8820 -p 8820 &> /tmp/ds2 &
ds2_pid=$!
${cds} run -d var/8830 -p 8830 &> /tmp/ds3 &
ds3_pid=$!

trap ctrl_c INT
function ctrl_c() {
    echo "Stopping at your request"
	cleanup
}

function cleanup() {
    kill "$ds1_pid" 2> /dev/null
    kill "$ds2_pid" 2> /dev/null
    kill "$ds3_pid" 2> /dev/null
}

verify_file=/tmp/repair_test_verify.data

target_args="-t 127.0.0.1:8810 -t 127.0.0.1:8820 -t 127.0.0.1:8830"
# Do initial volume population.
if ! ${cc} fill ${target_args} --verify-out "$verify_file" -q
then
    echo "Exit on initial fill"
    cleanup
    exit 1
fi

# Stop a downstairs, we will restart with lossy in the loop
kill "$ds3_pid"

# Start loop
for (( i = 0; i < 30; i += 1 )); do

    # restart downstairs with lossy
    ${cds} run -d var/8820 -p 8830 --lossy &> /tmp/ds2 &
    ds3_pid=$!

    if ! ${cc} repair ${target_args} --verify-out "$verify_file" --verify-in "$verify_file" -c 30
    then
        echo "Exit on repair fail"
        cleanup
        exit 1
    fi

    echo ""
    # Stop --lossy downstairs
    kill "$ds3_pid"
    sleep 2

    # Did we get any mismatches?
    ${cds} dump -d var/8810 -d var/8820 -d var/8830 -o
    echo "On loop $i"

    sleep 2
    echo ""
    # Start downstairs without lossy
    ${cds} run -d var/8830 -p 8830 &> /tmp/ds2 &
    ds3_pid=$!

    echo "Verifying data now"
    if ! ${cc} verify ${target_args} --verify-out "$verify_file" --verify-in "$verify_file" -q > /tmp/verify_out
    then
        echo "Exit on verify fail"
        echo "Check /tmp/verify_out for details"
        cleanup
        exit 1
    fi

    # stop a downstairs
    kill "$ds3_pid"
done

echo "Tests all done at $(date)"
cleanup
