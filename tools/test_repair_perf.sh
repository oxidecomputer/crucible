#!/bin/bash

# Test the performance of repair

# The actual repair is pretty straightforward, one DS is all old.

# Control-C to cleanup.
trap stop_test INT
function stop_test() {
    echo "Stopping the test"
    "$dsc" cmd shutdown
    sleep 5
    if [[ -n "$dsc_pid" ]]; then
        kill "$dsc_pid"
    fi
    exit 1
}

loop_log=/tmp/repair_perf.log
test_log=/tmp/repair_perf_test.log
echo "" > ${loop_log}
echo "starting $0 on $(date)" | tee ${loop_log}
echo "Tail $test_log for detailed test output"

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BINDIR=${BINDIR:-$ROOT/target/release}
echo "working from $ROOT"
cd "$ROOT" || stop_test

if pgrep -fl -U "$(id -u)" crucible-downstairs; then
    echo "Downstairs already running" >&2
    echo Run: pkill -f -U "$(id -u)" crucible-downstairs >&2
    exit 1
fi

ct="$BINDIR/crutest"
dsc="$BINDIR/dsc"
cds="$BINDIR/crucible-downstairs"

if [[ ! -f ${dsc} ]] || [[ ! -f ${cds} ]] || [[ ! -f ${ct} ]]; then
    echo "Can't find crucible binary at $cds or $ct or $dsc"
    exit 1
fi

os_name=$(uname)
if [[ "$os_name" == 'Darwin' ]]; then
    # stupid macos needs this to avoid popup hell.
    codesign -s - -f "$cds"
    codesign -s - -f "$ct"
fi

region_dir="/var/tmp/dsc/region"

function repair_round() {
    es=$1
    ec=$2

    total=0
    pass_total=0
    SECONDS=0

    (( bytes = es * 4096 ))
    (( size = bytes / 1024 / 1024 ))

    echo "    ES  EC  Extent Size: $size MiB" | tee -a ${loop_log}
    echo "Create region with ES:$es and EC:$ec to test" >> "${test_log}"
    ulimit -n 65536
    if ! "$dsc" create --cleanup --ds-bin "$cds" --block-size 4096 \
            --extent-count "$ec" --extent-size "$es" >> "$test_log" ; then
        echo "Failed to create new region"
        exit 1
    fi

    # Create the "old" region files
    # This assumes DSC uses the default directories
    for port in 8810 8820 8830
    do
        rm -rf "$region_dir"/"$port".old
        cp -R  "$region_dir"/"$port" "$region_dir"/"$port".old || stop_test
    done

    "$dsc" start >> "$test_log" 2>&1 &
    dsc_pid=$!

    args=()
    for port in 8810 8820 8830
    do
        args+=( -t "127.0.0.1:$port" )
    done

    # Do one IO to each block, verify.
    echo "$(date) fill" >> "$test_log"
    echo "$ct" fill "${args[@]}" -q --verify-out alan >> "$test_log"
    "$ct" fill "${args[@]}" -q --verify-out alan >> "$test_log" 2>&1
    if [[ $? -ne 0 ]]; then
        echo "Error in initial fill"
        stop_test
    fi

    echo "Fill completed" >> "$test_log"

    duration=$SECONDS
    printf "%6d %3d Create, fill, and verify took: %d:%02d \n" \
            "$es" "$ec" $((duration / 60)) $((duration % 60)) | tee -a ${loop_log}

    # We do this down here because we want to be sure all the downstairs
    # have started (meaning dsc has also started) and because our fill has
    # completed, we know dsc should be ready to receive commands.
    echo "Disable auto restart of downstairs" >> "${loop_log}"
    "$dsc" cmd disable-restart-all >> "${loop_log}"
    if [[ $? -ne 0 ]]; then
        echo "Error on disable restart"
        stop_test
    fi

    # Now run the repair loop
    for i in {1..2}
    do
        for ds in 0 1 2
        do
            echo "$(date) New loop starts now" >> "$test_log"
            "$dsc" cmd stop-all
            echo "" >> "$test_log"
            echo "" >> "$test_log"

            # Give "pause" time to stop all running downstairs.
            # We need to do this before moving the region directory out from
            # under a downstairs, otherwise it can fail and exit and the
            # downstairs daemon will think it is a real failure.
            sleep 5

            echo "$(date) move regions" >> "$test_log"
            if [[ "$ds" -eq 0 ]]; then
                rm -rf "$region_dir"/8810
                cp -R  "$region_dir"/8810.old "$region_dir"/8810
            elif [[ "$ds" -eq 1 ]]; then
                rm -rf "$region_dir"/8820
                cp -R  "$region_dir"/8820.old "$region_dir"/8820
            else
                rm -rf "$region_dir"/8830
                cp -R  "$region_dir"/8830.old "$region_dir"/8830
            fi

            echo "$(date) regions moved, current dump output:" >> "$test_log"
            "$cds" dump \
                    -d  "$region_dir"/8810 \
                    -d "$region_dir"/8820 \
                    -d "$region_dir"/8830 >> "$test_log" 2>&1
            echo "$(date) resume downstairs" >> "$test_log"
            "$dsc" cmd start-all

            # Start the upstairs and do one IO, this will force a repair.
            SECONDS=0
            echo "$(date) do one IO" >> "$test_log"
            "$ct" one "${args[@]}" \
                    -q --verify-out alan \
                    --verify-in alan \
                    --verify \
                    --retry-activate >> "$test_log" 2>&1
            result=$?

            if [[ $result -ne 0 ]]; then
                touch /var/tmp/ds_test/up 2> /dev/null
                printf "Error $result in test\n" "$i" | tee -a ${loop_log}
                mv "$test_log" "$test_log".lastfail
                return 1
            fi
            printf "%6d %3d [%d][%d] " \
                    "$es" "$ec" "$i" "$ds" | tee -a "${loop_log}"

            grep "extents repaired" "$test_log" | tail -1 | tee -a ${loop_log}
            (( pass_total += 1 ))
            duration=$SECONDS
            (( total += duration ))
        done
    done

    # Shutdown dsc
    "$dsc" cmd shutdown
    wait "$dsc_pid"

    ave=$(( total / pass_total ))
    printf "%6d %3d Loop ave:%d:%02d  total loop time:%d:%02d\n" \
        $es $ec $((ave / 60)) $((ave % 60)) \
        $((total / 60)) $((total % 60)) | tee -a ${loop_log}

    return 0
}

### extent size loop starts here
#               ES   EC
repair_round     10 40
repair_round    512 40
repair_round   4096 40
repair_round   8192 40
repair_round  16384 40
repair_round  32768 40
repair_round  65536 40
repair_round 131072 40

echo "Test done"
