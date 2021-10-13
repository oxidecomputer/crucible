#!/bin/bash
#
# A hack of downstairs restart tool
#
# Start up three downstairs in a retry loop.  If they are killed then start
# them up again.
# Start another loop to kill a downstairs at some random time.
# Let the restarts begin.
# When we have had enough, then touch the stop file and make everything
# wind down.
#
# No error checking really, we assume you have a proper directory for
# each downstairs.  Too much hard coded here.

set -o pipefail

#
# This loop will sleep some random time, then kill a downstairs.
# We currently pick
downstairs_restart() {
    echo "Kill and restart loop for the downstairs"
    while :; do
        if [[ -f ${testdir}/up ]]; then
            sleep 5
            continue
        fi

        # How long we sleep before restarting the next PID
        sleeptime=$(echo "$(date +%s) % 5" | bc )
        sleep "$sleeptime"
        if [[ -f ${testdir}/stop ]]; then
            break
        fi

        # Pick a PID and kill it
        ds_pids=( $(pgrep -fl target/debug/crucible-downstairs | awk '{print $1}') )

        # Sometimes there are no downstairs running.
        if [[ ${#ds_pids[@]} -gt 0 ]]; then
            pid_index=$((RANDOM % ${#ds_pids[@]}))

            #echo "Kill ${ds_pids[$pid_index]}"
            kill "${ds_pids[$pid_index]}"
            # > /dev/null 2>&1
        fi
    done
    # Run a final cleanup
    ds=$(pgrep -fl target/debug/crucible-downstairs | awk '{print $1}')
    for pid in ${ds}; do
        kill "$pid"
    done
    echo "exit downstairs restarter"
}

# Loop restarting a downstairs at the given port.
# If we get stopped for any reason other than 143, then report error
# and stop looping.
downstairs_daemon() {
    port=$1
    outfile="${testdir}/downstairs-out-${port}.txt"
    errfile="${testdir}/downstairs-err-${port}.txt"
    echo "" > "$outfile"
    echo "" > "$errfile"
    echo "$(date) Starting downstairs ${port}"
    while :; do
        cargo run -q -p crucible-downstairs -- run -p "$port" \
                -d var/"$port">> "$outfile" 2> "$errfile"
        res=$?
        if [[ $res -ne 143 ]]; then
            echo "Downstairs $port had bad exit $res"
            exit 1
        fi
        echo "$(date) Downstairs ${port} ended"
        sleep 1
        if [[ -f ${testdir}/stop ]]; then
            break
        fi
        echo "$(date) Restaring downstairs ${port}"
    done
    echo "$(date) downstairs ${port} exit on request"
}

ROOT=$(cd "$(dirname "$0")/.." && pwd)

cd "$ROOT" || (echo failed to cd "$ROOT"; exit 1)

if pgrep -fl target/debug/crucible-downstairs; then
    echo 'Some downstairs already running?' >&2
    exit 1
fi

if ! cargo build; then
    echo "Initial Build failed, no tests ran"
    exit 1
fi

if [[ ! -d var/3801 ]] || [[ ! -d var/3801 ]] || [[ ! -d var/3801 ]]; then
    echo "Missing var/380* directories"
    echo "This test requires you to have created a region at var/380*"
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

mkdir -p ${testdir}
downstairs_daemon 3801 2>/dev/null &
dsd_pid[0]=$!
downstairs_daemon 3802 2>/dev/null &
dsd_pid[1]=$!
downstairs_daemon 3803 2>/dev/null &
dsd_pid[2]=$!

sleep 1
echo "Downstairs started, give you a chance to start the test"
echo "Press return to begin the random restart of downstairs"
read ignored

downstairs_restart &
dsd_pid[3]=$!

# Loop our known background jobs, if any disappear, then stop
# the script.
while :; do
    for pid in ${dsd_pid[*]}; do
        if ! ps -p $pid > /dev/null; then
            echo "$pid is gone, check $testdir for errors"
            touch ${testdir}/stop
        fi
    done
    if [[ -f ${testdir}/stop ]]; then
        echo "Stopping loop"
        break;
    fi
    sleep 10
done

# Cleanup leftovers
ds=$(pgrep -fl target/debug/crucible-downstairs | awk '{print $1}')
for pid in ${ds}; do
    kill "$pid"
done

echo "Downstairs should all now stop for good"
for pid in ${dsd_pid[*]}; do
    kill "$pid"
    wait "$pid"
done
