#!/bin/bash
#
# Harness to reproduce crucible#1906 using the crutest "one906" workload
# and a DTrace triggered guest panic (see tools/test-for-1906-notes.md).
#
# Run this on the host where the three downstairs VMs live (mb-1 style
# setup, see tools/test-for-1906.sh).  This script:
#
#  1. Starts crutest with the one906 workload against all three
#     downstairs and leaves it running for the whole test.  crutest
#     pauses its own IO within about a second of a downstairs dying and
#     resumes only when all three downstairs are ACTIVE again.
#  2. In a loop:
#     a. Arm one906-panic.d in the downstairs 0 VM over ssh.  The
#        script panics the guest on the Nth ZIL log block write after
#        a dmu_sync (N randomized), cutting the ZIL chain inside the
#        vulnerable window.
#     b. When the VM dies, destroy the VMM and snapshot the dataset
#        backing its disk, preserving the pre-ZIL-replay state.
#     c. Boot the VM again.  If the downstairs comes online it has
#        self-verified its region: wait for all three downstairs to be
#        ACTIVE (LiveRepair done, crutest resumes IO on its own),
#        destroy the snapshot, and loop.
#     d. If the downstairs does not come online and its SMF log shows
#        "missing context slot", the bug is reproduced: stop crutest,
#        preserve the snapshot, and exit.
#
# Requirements:
#  - passwordless ssh as root to the downstairs 0 VM.
#  - dtrace available inside that VM.
#  - a propolis-server for the downstairs 0 VM controlled through
#    propolis-cli, restarted automatically if killed.
#  - the crutest binary built with the one906 workload.
#
# Touch /tmp/stop to end the test cleanly after the current loop.
# Most settings below can be overridden from the environment.

set -o pipefail

# Host side locations
CRUTEST=${CRUTEST:-/save/bin/crutest}
DSSTATE=${DSSTATE:-/save/dtrace/dsstate.d}
PANIC_D=${PANIC_D:-/save/dtrace/one906-panic.d}
CRUTEST_LOG=${CRUTEST_LOG:-/tmp/crutest-one906.out}

# Propolis server for the downstairs 0 VM
VM_DIR=${VM_DIR:-/save/vm}
PROPOLIS_CLI=${PROPOLIS_CLI:-./propolis-cli-boot-order}
PROPOLIS_PORT=${PROPOLIS_PORT:-12400}
VM_TOML=${VM_TOML:-illumos-cds0.toml}
VM_NAME=${VM_NAME:-cds0}
VM_CORES=${VM_CORES:-32}
VM_MEM=${VM_MEM:-32768}

# The dataset on this host backing the downstairs 0 VM data disk.
SNAP_DS=${SNAP_DS:-oxp_00/nocrypt/dump}
SNAP="$SNAP_DS@one906"

# Downstairs targets.  DS0 is the one that gets panicked.
DS0=${DS0:-192.168.100.2}
DS1=${DS1:-192.168.100.3}
DS2=${DS2:-192.168.100.4}
DS_PORT=${DS_PORT:-9000}
REPAIR_PORT=${REPAIR_PORT:-13000}

# SMF service of the downstairs inside the VM.
DS_SMF=${DS_SMF:-downstairs}

# Seconds to wait for the armed trigger to fire before disarming.
ARM_TIMEOUT=${ARM_TIMEOUT:-300}
# Seconds to wait for the downstairs to return after VM boot.
DS_WAIT=${DS_WAIT:-600}

# Private key for ssh/scp to the downstairs 0 VM, empty for default.
SSH_KEY=${SSH_KEY:-}

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=5"
if [[ -n "$SSH_KEY" ]]; then
    SSH_OPTS="$SSH_OPTS -i $SSH_KEY"
fi
SSH="ssh $SSH_OPTS root@$DS0"
gen=${GEN:-$(date +%s)}
count=0
crutest_pid=""

function msg() {
    echo "[$count] $(date) $*"
}

function ds_online() {
    curl -s --max-time 2 "http://$1:$REPAIR_PORT/work" > /dev/null 2>&1
}

function stop_crutest() {
    if [[ -n "$crutest_pid" ]] && kill -0 "$crutest_pid" 2>/dev/null; then
        msg "stopping crutest at pid $crutest_pid"
        kill -SIGUSR1 "$crutest_pid"
        wait "$crutest_pid" 2>/dev/null
    fi
    crutest_pid=""
}

# The bracket in the pkill pattern keeps pkill -f from matching the
# remote shell running the pkill command itself (its argv contains the
# pattern, and illumos pkill does not exclude ancestor processes).
function disarm_trigger() {
    $SSH "pkill -f '[o]ne906-panic'" > /dev/null 2>&1
}

trap ctrl_c INT
function ctrl_c() {
    msg "stopping at your request"
    disarm_trigger
    stop_crutest
    exit 1
}

# Preflight checks
for f in "$CRUTEST" "$DSSTATE" "$PANIC_D"; do
    if [[ ! -f "$f" ]]; then
        echo "$f does not exist"
        exit 1
    fi
done
if [[ ! -d "$VM_DIR" ]]; then
    echo "VM directory $VM_DIR does not exist"
    exit 1
fi
for ds in $DS0 $DS1 $DS2; do
    if ds_online "$ds"; then
        msg "downstairs $ds online"
    else
        msg "failed to find downstairs $ds online"
        exit 1
    fi
done
if zfs list -t snapshot "$SNAP" > /dev/null 2>&1; then
    msg "snapshot $SNAP exists on test start, please destroy it first"
    exit 1
fi
if ! $SSH true; then
    msg "cannot ssh to root@$DS0"
    exit 1
fi
if ! scp $SSH_OPTS "$PANIC_D" "root@$DS0:/var/tmp/"; then
    msg "failed to copy $PANIC_D to the VM"
    exit 1
fi
panic_d_vm="/var/tmp/$(basename "$PANIC_D")"

# Start crutest and leave it running for the whole test.
msg "starting crutest one906 with gen $gen, log at $CRUTEST_LOG"
"$CRUTEST" one906 --continuous -g "$gen" \
    -t "$DS0:$DS_PORT" -t "$DS1:$DS_PORT" -t "$DS2:$DS_PORT" \
    > "$CRUTEST_LOG" 2>&1 &
crutest_pid=$!

msg "waiting for all downstairs to be active"
sleep_time=5
while :; do
    states=$(dtrace -s "$DSSTATE" 2> /dev/null)
    msg "current states: $states"
    if [[ "$states" == "ACT ACT ACT" ]]; then
        break
    fi
    sleep $sleep_time
    sleep_time=30
done
msg "all downstairs are active, begin the main loop"

count=1
while :; do
    loop_start=$SECONDS

    if ! kill -0 "$crutest_pid" 2>/dev/null; then
        msg "crutest is gone, see $CRUTEST_LOG"
        exit 1
    fi

    # Arm the panic trigger in the VM, first thing.
    panic_at=$((2 + RANDOM % 8))
    msg "arming panic trigger in VM, panic at lwb write $panic_at"
    if ! $SSH "pkill -f '[o]ne906-panic' > /dev/null 2>&1; \
        nohup dtrace -w -s $panic_d_vm $panic_at \
        >> /var/tmp/one906-panic.log 2>&1 &"; then
        msg "failed to arm the panic trigger"
        stop_crutest
        exit 1
    fi

    # Record the VMM ID; propolis still answers after a guest panic.
    vmm_id=$($PROPOLIS_CLI --server 0.0.0.0 --port "$PROPOLIS_PORT" get |
        grep " id: " | awk '{print $2}' | tr -d ',')
    if [[ -z "$vmm_id" ]]; then
        msg "failed to get VMM ID from propolis"
        stop_crutest
        exit 1
    fi

    # Wait for the VM to die.
    armed=$SECONDS
    dead=0
    fail=0
    while :; do
        if ds_online "$DS0"; then
            fail=0
        else
            ((fail += 1))
        fi
        if [[ $fail -ge 2 ]]; then
            dead=1
            break
        fi
        if [[ $((SECONDS - armed)) -gt $ARM_TIMEOUT ]]; then
            break
        fi
        sleep 1
    done

    if [[ $dead -eq 0 ]]; then
        msg "trigger did not fire in $ARM_TIMEOUT seconds, disarming"
        disarm_trigger
        ((count += 1))
        continue
    fi
    msg "VM is down $((SECONDS - armed)) seconds after arming"

    # Make sure nothing else writes to the VM disk, then snapshot the
    # pre-ZIL-replay state.  The guest is destroyed before it can boot
    # far enough to import its pool and replay the ZIL.
    ps_pid=$(ps -ef | grep propolis-server | grep "$PROPOLIS_PORT" |
        awk '{print $2}')
    if [[ -n "$ps_pid" ]]; then
        kill "$ps_pid"
    fi
    bhyvectl --vm "$vmm_id" --destroy
    msg "taking snapshot $SNAP"
    if ! zfs snapshot "$SNAP"; then
        msg "failed to take snapshot"
        stop_crutest
        exit 1
    fi

    # Give the propolis-server restart loop time to come back.
    sleep 5
    msg "starting the VM back up"
    (cd "$VM_DIR" && $PROPOLIS_CLI --server 0.0.0.0 \
        --port "$PROPOLIS_PORT" new -c "$VM_CORES" -m "$VM_MEM" \
        --config-toml "$VM_TOML" "$VM_NAME")
    sleep 2
    (cd "$VM_DIR" && $PROPOLIS_CLI --server 0.0.0.0 \
        --port "$PROPOLIS_PORT" state run)

    # Wait for the downstairs to come back online.  Coming online
    # means the downstairs opened the region and self-verified it.
    boot_start=$SECONDS
    up=0
    while [[ $((SECONDS - boot_start)) -lt $DS_WAIT ]]; do
        if ds_online "$DS0"; then
            up=1
            break
        fi
        sleep 5
    done

    if [[ $up -eq 0 ]]; then
        msg "downstairs did not return in $DS_WAIT seconds, checking why"
        hit=$($SSH "grep -h 'missing context slot' \$(svcs -L $DS_SMF) \
            2>/dev/null | tail -3")
        stop_crutest
        if [[ -n "$hit" ]]; then
            echo "**********************************************"
            echo "[$count] $(date) reproduced crucible#1906!"
            echo "$hit"
            echo "snapshot preserved at $SNAP"
            echo "crutest log at $CRUTEST_LOG"
            echo "**********************************************"
            exit 0
        fi
        msg "no 'missing context slot' found, investigate by hand"
        $SSH "svcs -xv" 2>/dev/null
        msg "snapshot preserved at $SNAP"
        exit 1
    fi
    msg "downstairs back online in $((SECONDS - boot_start)) seconds"

    # Wait for all downstairs to be ACTIVE.  crutest resumes its IO on
    # its own once this is true.
    while :; do
        states=$(dtrace -s "$DSSTATE" 2> /dev/null)
        if [[ "$states" == "ACT ACT ACT" ]]; then
            break
        fi
        msg "current states: $states, waiting for all ACT"
        sleep 10
    done
    msg "all downstairs are active again"

    # Region verified and repaired, this loop found nothing.
    msg "destroying snapshot $SNAP"
    if ! zfs destroy "$SNAP"; then
        msg "failed to destroy snapshot $SNAP"
        stop_crutest
        exit 1
    fi

    msg "loop done in $((SECONDS - loop_start)) seconds"
    if [[ -f /tmp/stop ]]; then
        msg "exiting because /tmp/stop is present"
        rm /tmp/stop
        break
    fi
    ((count += 1))
done

stop_crutest
msg "test ends after $count loops"
