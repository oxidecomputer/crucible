#!/usr/bin/env bash

# A test to break, then Repair a downstairs region that is out of sync with
# the other regions. We pick a downstairs at random and restart it with
# the --lossy flag, meaning it will skip some IO requests (and have to
# come back and do them later) and will introduce some delay in completing
# IOs.  This combined with the client program exiting as soon as an IO
# is acked means that the lossy downstairs will always be missing IOs.

trap ctrl_c INT
function ctrl_c() {
    echo "Stopping at your request"
    cleanup
    exit 1
}

function cleanup() {
    kill "$ds0_pid" 2> /dev/null
    kill "$ds1_pid" 2> /dev/null
    kill "$ds2_pid" 2> /dev/null
}

cargo build || echo "Failed to build"

cds="./target/debug/crucible-downstairs"
cc="./target/debug/crucible-client"
if [[ ! -f ${cds} ]] || [[ ! -f ${cc} ]]; then
    echo "Can't find crucible binaries at $cds or $cc"
    exit 1
fi

if ! ./tools/create-generic-ds.sh -d -c 30 -s 20; then
    echo "Failed to create new region"
    exit 1
fi

# Start all three downstairs
${cds} run -d var/8810 -p 8810 &> /tmp/ds8810 &
ds0_pid=$!
${cds} run -d var/8820 -p 8820 &> /tmp/ds8820 &
ds1_pid=$!
${cds} run -d var/8830 -p 8830 &> /tmp/ds8830 &
ds2_pid=$!

os_name=$(uname)
if [[ "$os_name" == 'Darwin' ]]; then
    # stupid macos needs this to avoid popup hell.
    codesign -s - -f "$cds"
    codesign -s - -f "$cc"
fi

verify_file=/tmp/repair_test_verify.data
test_log=/tmp/verify_out

target_args="-t 127.0.0.1:8810 -t 127.0.0.1:8820 -t 127.0.0.1:8830"
# Do initial volume population.
if ! ${cc} fill ${target_args} --verify-out "$verify_file" -q
then
    echo "Exit on initial fill"
    cleanup
    exit 1
fi

# Start loop
for (( i = 0; i < 900; i += 1 )); do

    choice=$((RANDOM % 3))
    echo ""
    echo "Begin loop $i"
    echo "Downstairs to restart: $choice"

    # stop a downstairs and restart with lossy
    if [[ $choice -eq 0 ]]; then
        kill "$ds0_pid"
        wait "$ds0_pid"
        ${cds} run -d var/8810 -p 8810 --lossy &> /tmp/ds8810 &
        ds0_pid=$!
    elif [[ $choice -eq 1 ]]; then
        kill "$ds1_pid"
        wait "$ds1_pid"
        ${cds} run -d var/8820 -p 8820 --lossy &> /tmp/ds8820 &
        ds1_pid=$!
    else
        kill "$ds2_pid"
        wait "$ds2_pid"
        ${cds} run -d var/8830 -p 8830 --lossy &> /tmp/ds8830 &
        ds2_pid=$!
    fi

    if ! ${cc} repair ${target_args} --verify-out "$verify_file" --verify-in "$verify_file" -c 30
    then
        echo "Exit on repair fail, loop: $i, choice: $choice"
        cleanup
        exit 1
    fi

    echo ""
    # Stop --lossy downstairs so it can't complete all its IOs
    if [[ $choice -eq 0 ]]; then
        kill "$ds0_pid"
        wait "$ds0_pid"
    elif [[ $choice -eq 1 ]]; then
        kill "$ds1_pid"
        wait "$ds1_pid"
    else
        kill "$ds2_pid"
        wait "$ds2_pid"
    fi
    sleep 2

    # Did we get any mismatches?
    echo "Current downstairs dump:"
    ${cds} dump -d var/8810 -d var/8820 -d var/8830
    echo "On loop $i"

    sleep 2
    echo ""
    # Start downstairs without lossy
    if [[ $choice -eq 0 ]]; then
        ${cds} run -d var/8810 -p 8810 &> /tmp/ds8810 &
        ds0_pid=$!
    elif [[ $choice -eq 1 ]]; then
        ${cds} run -d var/8820 -p 8820 &> /tmp/ds8820 &
        ds1_pid=$!
    else
        ${cds} run -d var/8830 -p 8830 &> /tmp/ds8830 &
        ds2_pid=$!
    fi

    echo "Verifying data now"
    echo ${cc} verify ${target_args} --verify-out "$verify_file" --verify-in "$verify_file" --range -q > "$test_log"
    if ! ${cc} verify ${target_args} --verify-out "$verify_file" --verify-in "$verify_file" --range -q >> "$test_log"
    then
        echo "Exit on verify fail, loop: $i, choice: $choice"
        echo "Check $test_log for details"
        cleanup
        exit 1
    fi

    # XXX This check is here because we don't yet have a way of getting
    # error status from the upstairs to indicate this has happened.
	if grep "read hash mismatch" "$test_log"; then
        echo "Found Mismatch"
        echo "" >> "$test_log".new
        echo "loop $i, choice: $choice" >> "$test_log".new
        echo $(date) >> "$test_log".new
        cat "$test_log" >> "$test_log".new
    fi

    echo "Loop: $i  Downstairs dump after verify (and repair):"
    ${cds} dump -d var/8810 -d var/8820 -d var/8830

done

echo "Tests all done at $(date)"
cleanup
