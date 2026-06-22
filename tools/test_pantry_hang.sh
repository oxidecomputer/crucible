#!/bin/bash

# Demonstrate that the pantry blocks when attaching a RW volume with one
# downstairs stopped.
#
# The synchronous attach endpoint holds the pantry's entries lock for the
# entire duration of activation.  When a downstairs is unavailable, the
# upstairs cannot activate, so the lock is held indefinitely.  This blocks
# every other pantry operation that needs the entries lock (status,
# volume-status, duplicate attach, etc.).
#
# The progenitor-generated pantry client has a 15-second request timeout,
# so pantest will return an error, but the server-side handler keeps running
# (HandlerTaskMode::Detached) and continues to hold the lock.
#
# This test verifies:
#   1. The initial attach times out on the client side.
#   2. After the timeout, the pantry is still running.
#   3. A second volume (against a healthy dsc) can be attached.
#      (This requires the fix -- without it, the entries lock blocks
#      all other attach/status/detach operations.)
#   4. That second volume can be detached.
#   5. A status request works.
#   6. After starting the stopped downstairs, the original volume
#      activates and the pantry reports it correctly.

set -o pipefail

SECONDS=0

trap ctrl_c INT
function ctrl_c() {
    echo ""
    echo "Stopping at your request"
    cleanup
    exit 1
}

function cleanup() {
    # Kill any background pantest processes.
    for pid in $pantest_pid $attach2_pid $detach_pid $status_pid; do
        if [[ -n "$pid" ]] && ps -p "$pid" > /dev/null 2>&1; then
            kill "$pid" 2>/dev/null
            wait "$pid" 2>/dev/null
        fi
    done
    if [[ -n "$pantry_pid" ]] && ps -p "$pantry_pid" > /dev/null 2>&1; then
        echo "Stopping pantry (pid $pantry_pid)"
        kill "$pantry_pid"
        wait "$pantry_pid" 2>/dev/null
    fi
    if [[ -n "$dsc_pid" ]] && ps -p "$dsc_pid" > /dev/null 2>&1; then
        echo "Shutting down dsc 1"
        ${dsc} cmd shutdown 2>/dev/null
        wait "$dsc_pid" 2>/dev/null
    fi
    if [[ -n "$dsc2_pid" ]] && ps -p "$dsc2_pid" > /dev/null 2>&1; then
        echo "Shutting down dsc 2"
        ${dsc} cmd --server http://127.0.0.1:9999 shutdown 2>/dev/null
        wait "$dsc2_pid" 2>/dev/null
    fi
}

ROOT=$(cd "$(dirname "$0")/.." && pwd)
export BINDIR=${BINDIR:-$ROOT/target/release}
cds="$BINDIR/crucible-downstairs"
dsc="$BINDIR/dsc"
pantry="$BINDIR/crucible-pantry"
pantest_bin="$BINDIR/pantest"

for bin in $cds $dsc $pantry $pantest_bin; do
    if [[ ! -f "$bin" ]]; then
        echo "Can't find binary at $bin" >&2
        exit 1
    fi
done

if pgrep -fl -U "$(id -u)" "$cds" > /dev/null 2>&1; then
    echo "Downstairs already running" >&2
    echo "Run: pkill -f -U $(id -u) $cds" >&2
    exit 1
fi

if pgrep -fl -U "$(id -u)" crucible-pantry > /dev/null 2>&1; then
    echo "Pantry already running" >&2
    echo "Run: pkill -f -U $(id -u) crucible-pantry" >&2
    exit 1
fi

REGION_ROOT=${REGION_ROOT:-/var/tmp}
MY_REGION_ROOT="${REGION_ROOT}/test_pantry_hang"
if [[ -d "$MY_REGION_ROOT" ]]; then
    rm -rf "$MY_REGION_ROOT"
fi
mkdir -p "$MY_REGION_ROOT"
if [[ $? -ne 0 ]]; then
    echo "Failed to make region root $MY_REGION_ROOT"
    exit 1
fi

WORK_ROOT=${WORK_ROOT:-/tmp}
TEST_ROOT="${WORK_ROOT}/test_pantry_hang"
if [[ -d "$TEST_ROOT" ]]; then
    # Delete previous test data
    rm -r "$TEST_ROOT"
fi
mkdir -p "$TEST_ROOT"
if [[ $? -ne 0 ]]; then
    echo "Failed to make test root $TEST_ROOT"
    exit 1
fi

dsc_log=${TEST_ROOT}/test_pantry_hang_dsc.log
dsc2_log=${TEST_ROOT}/test_pantry_hang_dsc2.log
pantry_log=${TEST_ROOT}/test_pantry_hang_pantry.log
pantest_log=${TEST_ROOT}/test_pantry_hang_pantest.log

echo "" > "$dsc_log"
echo "" > "$dsc2_log"
echo "" > "$pantry_log"
echo "" > "$pantest_log"

result=0

echo "========================================="
echo "Test: pantry blocks on RW attach with a"
echo "      stopped downstairs"
echo "========================================="

# Use fixed volume IDs so we can reference them later.
volume_id="$(uuidgen)"
volume_id2="$(uuidgen)"
echo "Volume 1 ID: $volume_id  (will have a stopped downstairs)"
echo "Volume 2 ID: $volume_id2 (all downstairs healthy)"
echo ""

# Step 1: Start dsc 1 (ports 8810/8820/8830, control 9998)
echo "Step 1: Create and start dsc 1 (3 downstairs)"
if ! ${dsc} create --cleanup \
    --ds-bin "$cds" \
    --region-dir "$MY_REGION_ROOT"
    --extent-count 15 \
    --extent-size 100 >> "$dsc_log" 2>&1; then
    echo "ERROR: Failed to create dsc 1 regions"
    echo "Check $dsc_log"
    exit 1
fi

${dsc} start --ds-bin "$cds" --region-dir "$MY_REGION_ROOT" >> "$dsc_log" 2>&1 &
dsc_pid=$!
sleep 3

if ! ps -p $dsc_pid > /dev/null 2>&1; then
    echo "ERROR: dsc 1 failed to start"
    exit 1
fi
echo "  dsc 1 running (pid $dsc_pid, control 9998)"

if ! ${dsc} cmd all-running > /dev/null 2>&1; then
    echo "ERROR: not all dsc 1 downstairs are running"
    cleanup
    exit 1
fi
echo "  All 3 dsc 1 downstairs running"

# Step 2: Start dsc 2 (ports 8910/8920/8930, control 9999)
echo ""
echo "Step 2: Create and start dsc 2 (3 downstairs)"
if ! ${dsc} create --cleanup \
    --ds-bin "$cds" \
    --extent-count 15 \
    --extent-size 100 \
    --port-base 8910 \
    --output-dir /tmp/dsc2 \
    --region-dir /var/tmp/dsc2/region >> "$dsc2_log" 2>&1; then
    echo "ERROR: Failed to create dsc 2 regions"
    cleanup
    exit 1
fi

${dsc} start --ds-bin "$cds" \
    --control 127.0.0.1:9999 \
    --port-base 8910 \
    --output-dir /tmp/dsc2 \
    --region-dir /var/tmp/dsc2/region >> "$dsc2_log" 2>&1 &
dsc2_pid=$!
sleep 3

if ! ps -p $dsc2_pid > /dev/null 2>&1; then
    echo "ERROR: dsc 2 failed to start"
    cleanup
    exit 1
fi
echo "  dsc 2 running (pid $dsc2_pid, control 9999)"

if ! ${dsc} cmd --server http://127.0.0.1:9999 \
    all-running > /dev/null 2>&1; then
    echo "ERROR: not all dsc 2 downstairs are running"
    cleanup
    exit 1
fi
echo "  All 3 dsc 2 downstairs running"

# Step 3: Start the pantry
echo ""
echo "Step 3: Start the pantry"
${pantry} run -l 127.0.0.1:17000 >> "$pantry_log" 2>&1 &
pantry_pid=$!
sleep 2

if ! ps -p $pantry_pid > /dev/null 2>&1; then
    echo "ERROR: pantry failed to start"
    cleanup
    exit 1
fi
echo "  Pantry running (pid $pantry_pid)"

if ! ${pantest_bin} -p 127.0.0.1:17000 status >> "$pantest_log" 2>&1; then
    echo "ERROR: pantry status check failed"
    cleanup
    exit 1
fi
echo "  Pantry responding to status requests"

# Step 4: Stop one downstairs on dsc 1, then attempt attach
echo ""
echo "Step 4: Stop downstairs 0 on dsc 1"
${dsc} cmd disable-restart --cid 0 > /dev/null 2>&1
${dsc} cmd stop --cid 0 > /dev/null 2>&1
sleep 2

state=$(${dsc} cmd state --cid 0 2>/dev/null)
echo "  Downstairs 0 state: $state"

# Step 5: Attach volume 1
echo ""
echo "Step 5: Attach volume 1 via dsc 1 (expect client timeout)"
echo "  The progenitor client has a 15s request timeout."
echo "  The attach will fail on the client side, but the server"
echo "  handler keeps running and holds the entries lock."
echo ""

${pantest_bin} -p 127.0.0.1:17000 \
    attach --dsc 127.0.0.1:9998 -v "$volume_id" \
    >> "$pantest_log" 2>&1 &
pantest_pid=$!

echo "  Waiting for pantest attach to timeout..."
count=0
while ps -p "$pantest_pid" > /dev/null 2>&1; do
    sleep 2
    (( count += 2 ))
    if (( count > 30 )); then
        echo "  pantest has not timed out after 30s (unexpected)"
        break
    fi
done

wait "$pantest_pid" 2>/dev/null
attach_rc=$?
echo "  pantest attach exited with rc=$attach_rc (expected non-zero)"

if (( attach_rc == 0 )); then
    echo "  FAIL: attach succeeded, but should have timed out"
    result=1
else
    echo "  PASS: attach timed out as expected"
fi

# Step 6: Verify the pantry is still running
echo ""
echo "Step 6: Verify pantry process is still alive"
if ! ps -p "$pantry_pid" > /dev/null 2>&1; then
    echo "  FAIL: pantry process exited!"
    result=1
    cleanup
    exit 1
fi
echo "  PASS: pantry still running (pid $pantry_pid)"

# Step 7: Attach volume 2 via dsc 2 (all healthy)
# Without the fix, this will hang because the entries lock is held by the
# stuck volume 1 activation.  With the fix, this should succeed.
echo ""
echo "Step 7: Attach volume 2 via dsc 2 (all downstairs healthy)"
echo "  Without the fix this will hang (entries lock held)."
echo "  With the fix this should succeed."

${pantest_bin} -p 127.0.0.1:17000 \
    attach --dsc 127.0.0.1:9999 -v "$volume_id2" \
    >> "$pantest_log" 2>&1 &
attach2_pid=$!

# Give it time to complete -- 20s is enough if the lock is free, but will hit
# the client timeout if the lock is held.
count=0
while ps -p "$attach2_pid" > /dev/null 2>&1; do
    sleep 2
    (( count += 2 ))
    if (( count > 20 )); then
        echo "  Volume 2 attach still running after 20s"
        echo "  FAIL: attach is blocked (entries lock held by volume 1)"
        kill "$attach2_pid" 2>/dev/null
        wait "$attach2_pid" 2>/dev/null
        attach2_pid=""
        result=1
        break
    fi
done

if [[ -n "$attach2_pid" ]]; then
    wait "$attach2_pid" 2>/dev/null
    attach2_rc=$?
    if (( attach2_rc == 0 )); then
        echo "  PASS: volume 2 attached successfully"
    else
        echo "  FAIL: volume 2 attach returned rc=$attach2_rc"
        result=1
    fi
    attach2_pid=""
fi

# Step 8: Detach volume 2
# This also requires the entries lock, so without the fix it will also hang.
echo ""
echo "Step 8: Detach volume 2"

${pantest_bin} -p 127.0.0.1:17000 \
    detach "$volume_id2" \
    >> "$pantest_log" 2>&1 &
detach_pid=$!

count=0
while ps -p "$detach_pid" > /dev/null 2>&1; do
    sleep 2
    (( count += 2 ))
    if (( count > 20 )); then
        echo "  Detach still running after 20s"
        echo "  FAIL: detach is blocked (entries lock held)"
        kill "$detach_pid" 2>/dev/null
        wait "$detach_pid" 2>/dev/null
        detach_pid=""
        result=1
        break
    fi
done

if [[ -n "$detach_pid" ]]; then
    wait "$detach_pid" 2>/dev/null
    detach_rc=$?
    if (( detach_rc == 0 )); then
        echo "  PASS: volume 2 detached successfully"
    else
        echo "  FAIL: volume 2 detach returned rc=$detach_rc"
        result=1
    fi
    detach_pid=""
fi

# Step 9: Check pantry status. Without the fix, this would hang too.
echo ""
echo "Step 9: Check pantry status"

${pantest_bin} -p 127.0.0.1:17000 status \
    >> "$pantest_log" 2>&1 &
status_pid=$!

count=0
while ps -p "$status_pid" > /dev/null 2>&1; do
    sleep 2
    (( count += 2 ))
    if (( count > 20 )); then
        echo "  Status still running after 20s"
        echo "  FAIL: status is blocked (entries lock held)"
        kill "$status_pid" 2>/dev/null
        wait "$status_pid" 2>/dev/null
        status_pid=""
        result=1
        break
    fi
done

if [[ -n "$status_pid" ]]; then
    wait "$status_pid" 2>/dev/null
    status_rc=$?
    if (( status_rc == 0 )); then
        echo "  PASS: pantry status responded"
    else
        echo "  FAIL: pantry status returned rc=$status_rc"
        result=1
    fi
    status_pid=""
fi

# -----------------------------------------------------------
# Step 10: Restart the stopped downstairs and verify the
#   original volume activates.
# -----------------------------------------------------------
echo ""
echo "Step 10: Restart downstairs 0 on dsc 1"
${dsc} cmd enable-restart --cid 0 > /dev/null 2>&1
${dsc} cmd start --cid 0 > /dev/null 2>&1
sleep 5

state=$(${dsc} cmd state --cid 0 2>/dev/null)
echo "  Downstairs 0 state: $state"

echo "  Waiting for volume 1 activation to complete..."
sleep 10

echo ""
echo "Step 11: Verify volume 1 is now attached and active"
if timeout 15 ${pantest_bin} -p 127.0.0.1:17000 \
    volume-status "$volume_id" >> "$pantest_log" 2>&1; then
    echo "  PASS: volume 1 status responded after downstairs restart"
else
    echo "  FAIL: volume 1 status not available after restart"
    result=1
fi

# Final status check to make sure everything is healthy.
if timeout 15 ${pantest_bin} -p 127.0.0.1:17000 status \
    >> "$pantest_log" 2>&1; then
    echo "  PASS: pantry status healthy"
else
    echo "  FAIL: pantry status not responding"
    result=1
fi

# -----------------------------------------------------------
# Summary
# -----------------------------------------------------------
echo ""
echo "========================================="
if (( result == 0 )); then
    echo "ALL CHECKS PASSED"
else
    echo "SOME CHECKS FAILED"
fi
echo "========================================="
echo ""
echo "Logs:"
echo "  dsc 1:   $dsc_log"
echo "  dsc 2:   $dsc2_log"
echo "  pantry:  $pantry_log"
echo "  pantest: $pantest_log"
echo ""

cleanup
echo "Done (${SECONDS}s elapsed)"
exit $result
