#!/bin/bash
#
# Harness to reproduce crucible#1906 using the crutest "one906" workload
# and a DTrace triggered guest panic (see tools/test-for-1906-notes.md).
# Some setup for the VMs and the host (mb-1 in this case) can be found
# at: https://github.com/oxidecomputer/chimchim/blob/main/setup/mb-1.md
# Specifically, how to create the proper network config.
#
# Run this on the host where the three downstairs VMs live.
# This script:
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
#        containing the Crucible region, preserving the pre-ZIL-replay state.
#     c. Boot the VM again.  If the downstairs comes online it has
#        self-verified its region: wait for all three downstairs to be
#        ACTIVE (LiveRepair done, crutest resumes IO on its own),
#        destroy the snapshot, and loop.
#     d. If the downstairs does not come online and its SMF log shows
#        "missing context slot", the bug is reproduced: stop crutest,
#        preserve the snapshot, and exit.
#
# Requirements:
#  - ssh allowed as root from the host to the downstairs 0 VM.
#  - dtrace available inside that VM.
#  - a propolis-server for the downstairs 0 VM controlled through
#    propolis-cli is restarted automatically if killed.
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
PROPOLIS_CLI=${PROPOLIS_CLI:-/save/vm/propolis-cli-boot-order}
PROPOLIS_PORT=${PROPOLIS_PORT:-12400}
VM_TOML=${VM_TOML:-/save/vm/illumos-cds0.toml}
VM_NAME=${VM_NAME:-cds0}
VM_CORES=${VM_CORES:-32}
VM_MEM=${VM_MEM:-32768}

# From the dataset on this host that is backing the downstairs 0
# VM data disk, we build the snapshot name
SNAP_DS=${SNAP_DS:-oxp_00/nocrypt/dump}
SNAP="$SNAP_DS@one906"

# Downstairs targets.  DS0 is the one that gets panicked.
DS0=${DS0:-192.168.100.2}
DS1=${DS1:-192.168.100.3}
DS2=${DS2:-192.168.100.4}
# All three downstairs use the same port.
DS_PORT=${DS_PORT:-9000}
REPAIR_PORT=${REPAIR_PORT:-13000}

# Name of the SMF service for the downstairs inside the VM.
DS_SMF=${DS_SMF:-downstairs}

# How many times to retry creating the VM instance after a crash.
VM_START_RETRIES=${VM_START_RETRIES:-5}

# Seconds to wait for the armed trigger to fire before disarming.
ARM_TIMEOUT=${ARM_TIMEOUT:-300}
# Seconds to wait for the downstairs to return after VM boot.
# We have to allow VM restart time, and then any repair time.
DS_WAIT=${DS_WAIT:-600}

# Private key for ssh/scp to the downstairs 0 VM, empty for default.
SSH_KEY=${SSH_KEY:-/save/vm/demo.priv}

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

# Ask crutest to stop, escalating if it does not exit: SIGUSR1 for a
# clean shutdown, then SIGTERM, then SIGKILL.
function stop_crutest() {
    if [[ -n "$crutest_pid" ]] && kill -0 "$crutest_pid" 2>/dev/null; then
        msg "stopping crutest at pid $crutest_pid"
        kill -SIGUSR1 "$crutest_pid"
        for ((i = 0; i < 15; i++)); do
            kill -0 "$crutest_pid" 2>/dev/null || break
            sleep 1
        done
        if kill -0 "$crutest_pid" 2>/dev/null; then
            msg "crutest did not exit from SIGUSR1, sending SIGTERM"
            kill "$crutest_pid"
            sleep 2
        fi
        if kill -0 "$crutest_pid" 2>/dev/null; then
            msg "crutest did not exit from SIGTERM, sending SIGKILL"
            kill -9 "$crutest_pid" 2>/dev/null
        fi
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
    msg "Stopping at your request"
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
        msg "Downstairs $ds online"
    else
        msg "Failed to find downstairs $ds online"
        exit 1
    fi
done
if zfs list -t snapshot "$SNAP" > /dev/null 2>&1; then
    msg "Snapshot $SNAP exists on test start, please destroy it first"
    exit 1
fi
if ! $SSH true; then
    msg "Cannot ssh to root@$DS0"
    exit 1
fi
if ! scp $SSH_OPTS "$PANIC_D" "root@$DS0:/var/tmp/"; then
    msg "Failed to copy $PANIC_D to the VM"
    exit 1
fi
panic_d_vm="/var/tmp/$(basename "$PANIC_D")"

# Start crutest and leave it running for the whole test.
msg "starting crutest one906 with gen $gen, log at $CRUTEST_LOG"
"$CRUTEST" one906 --continuous --quit -g "$gen" \
    -t "$DS0:$DS_PORT" -t "$DS1:$DS_PORT" -t "$DS2:$DS_PORT" \
    > "$CRUTEST_LOG" 2>&1 &
crutest_pid=$!

msg "Waiting for all downstairs to be active"
last_states=""
while :; do
    if ! kill -0 "$crutest_pid" 2>/dev/null; then
        msg "crutest exited before going active, see $CRUTEST_LOG"
        exit 1
    fi
    states=$(dtrace -s "$DSSTATE" 2> /dev/null)
    # Only print when the state changes, so the timeline reflects when
    # transitions actually happen rather than a fixed poll cadence.
    if [[ "$states" != "$last_states" ]]; then
        msg "current states: $states"
        last_states="$states"
    fi
    if [[ "$states" == "ACT ACT ACT" ]]; then
        break
    fi
    sleep 2
done
msg "all downstairs are active, begin the main loop"

count=1
while :; do
    loop_start=$SECONDS

    if ! kill -0 "$crutest_pid" 2>/dev/null; then
        msg "crutest is gone, see $CRUTEST_LOG"
        exit 1
    fi

    # Count crutest's resume lines now, so after the fault we can tell
    # when it has resumed IO for this loop (true recovery signal).
    resume_before=$(grep -c "resuming" "$CRUTEST_LOG" 2>/dev/null)

    # Arm the panic trigger in the VM, first thing.
    panic_at=$((2 + RANDOM % 8))
    msg "Arming panic trigger in VM, panic at lwb write $panic_at"
    if ! $SSH "pkill -f '[o]ne906-panic' > /dev/null 2>&1; \
        nohup dtrace -w -s $panic_d_vm $panic_at \
        >> /var/tmp/one906-panic.log 2>&1 &"; then
        msg "Failed to arm the panic trigger"
        stop_crutest
        exit 1
    fi

    # Record the VMM ID; propolis still answers after a guest panic.
    vmm_id=$($PROPOLIS_CLI --server 0.0.0.0 --port "$PROPOLIS_PORT" get |
        grep " id: " | awk '{print $2}' | tr -d ',')
    if [[ -z "$vmm_id" ]]; then
        msg "Failed to get VMM ID from propolis"
        stop_crutest
        exit 1
    fi

    # Wait for the VM to die, while also watching crutest: if crutest
    # exits here (for example because it lost quorum) we must stop
    # rather than block for the whole ARM_TIMEOUT.
    armed=$SECONDS
    dead=0
    fail=0
    while :; do
        if ! kill -0 "$crutest_pid" 2>/dev/null; then
            msg "crutest exited while waiting for the trigger, see" \
                "$CRUTEST_LOG"
            disarm_trigger
            crutest_pid=""
            exit 1
        fi
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
    msg "Taking snapshot $SNAP"
    if ! zfs snapshot "$SNAP"; then
        msg "failed to take snapshot"
        stop_crutest
        exit 1
    fi

    # Give the propolis-server restart loop time to come back.  Retry
    # the instance create a few times in case it needs longer.
    sleep 5
    msg "starting the VM back up"
    started=0
    for ((retry = 1; retry <= VM_START_RETRIES; retry++)); do
        if (cd "$VM_DIR" && $PROPOLIS_CLI --server 0.0.0.0 \
            --port "$PROPOLIS_PORT" new -c "$VM_CORES" -m "$VM_MEM" \
            --config-toml "$VM_TOML" "$VM_NAME"); then
            started=1
            break
        fi
        msg "failed to create instance (try $retry of" \
            "$VM_START_RETRIES), retry in 5 seconds"
        sleep 5
    done
    if [[ $started -eq 0 ]]; then
        msg "could not create the VM, is propolis-server running?"
        stop_crutest
        msg "snapshot left in place at $SNAP"
        exit 1
    fi
    sleep 2
    if ! (cd "$VM_DIR" && $PROPOLIS_CLI --server 0.0.0.0 \
        --port "$PROPOLIS_PORT" state run); then
        msg "could not run the VM"
        stop_crutest
        msg "snapshot left in place at $SNAP"
        exit 1
    fi

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
    msg "Downstairs back online in $((SECONDS - boot_start)) seconds"

    # Wait for the upstairs to truly recover before starting the next
    # loop.  crutest is the authority here: its prober does a positive
    # read+flush liveness probe and only resumes IO once all three
    # downstairs are actually serving again (not merely answering the
    # repair port, and not a stale pre-timeout Active).  Wait until
    # crutest logs a new "resuming" line for the fault we just caused.
    recovered=0
    recover_start=$SECONDS
    while [[ $((SECONDS - recover_start)) -lt $DS_WAIT ]]; do
        if ! kill -0 "$crutest_pid" 2>/dev/null; then
            msg "crutest exited during recovery, see $CRUTEST_LOG"
            crutest_pid=""
            msg "snapshot left in place at $SNAP"
            exit 1
        fi
        resume_now=$(grep -c "resuming" "$CRUTEST_LOG" 2>/dev/null)
        if [[ "$resume_now" -gt "$resume_before" ]]; then
            recovered=1
            break
        fi
        sleep 2
    done

    if [[ $recovered -eq 0 ]]; then
        msg "crutest did not resume IO in $DS_WAIT seconds"
        stop_crutest
        msg "snapshot left in place at $SNAP"
        exit 1
    fi
    msg "crutest resumed IO, all downstairs serving again"

    # Region verified and repaired, this loop found nothing.
    msg "Destroying snapshot $SNAP"
    if ! zfs destroy "$SNAP"; then
        msg "failed to destroy snapshot $SNAP"
        stop_crutest
        exit 1
    fi

    msg "Loop done in $((SECONDS - loop_start)) seconds"
    if [[ -f /tmp/stop ]]; then
        msg "exiting because /tmp/stop is present"
        rm /tmp/stop
        break
    fi
    ((count += 1))
done

stop_crutest
msg "Test ends after $count loops"
