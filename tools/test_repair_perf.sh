#!/bin/bash

# Test the performance of repair
# The test does the following:
# - Create a region set with three downstairs.
# - Make an entire downstairs set of extents require repair.
# - Run the regular upstairs and let it perform the repair.
# - Use output from the upstairs to determine how long that repair takes,
#   and divide by the number of extents to get an average.
#
# The actual repair is pretty straightforward, one complete downstairs
# region # is replaced with an older version.

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

# This is the main perf loop.
# It expects $1 to be Extent Size (blocks in an extent).
# It expects $2 to be Extent Count (number of extent files in the region).
function repair_round() {
    es=$1
    ec=$2

    total=0
    pass_total=0
    SECONDS=0

    (( bytes = es * block_size ))
    (( size = bytes / 1024 / 1024 ))

    echo "    ES  EC  Extent Size: $size MiB" | tee -a "$loop_log"
    echo "Create region with ES:$es and EC:$ec to test" >> "$test_log"
    ulimit -n 65536
    if ! "$dsc" create --cleanup --ds-bin "$cds" --block-size "$block_size" \
            --region-dir "$region_one" \
            --region-dir "$region_two" \
            --region-dir "$region_three" \
            --extent-count "$ec" --extent-size "$es" >> "$test_log" ; then
        echo "Failed to create new region"
        exit 1
    fi

    # Create the "old" region files
    rm -f "$region_one"/8810.old
    cp -R "$region_one"/8810 "$region_one"/8810.old || stop_test
    rm -f "$region_two"/8820.old
    cp -R "$region_two"/8820 "$region_two"/8820.old || stop_test
    rm -f "$region_three"/8830.old
    cp -R "$region_three"/8830 "$region_three"/8830.old || stop_test

    "$dsc" start \
            --region-dir "$region_one" \
            --region-dir "$region_two" \
            --region-dir "$region_three" >> "$test_log" 2>&1 &
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
            "$es" "$ec" \
            $((duration / 60)) \
            $((duration % 60)) | tee -a "$loop_log"

    # We do this down here because we want to be sure all the downstairs
    # have started (meaning dsc has also started) and because our fill has
    # completed, we know dsc should be ready to receive commands.
    echo "Disable auto restart of downstairs" >> "$loop_log"
    "$dsc" cmd disable-restart-all >> "$loop_log"
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
                rm -rf "$region_one"/8810
                cp -R  "$region_one"/8810.old "$region_one"/8810
            elif [[ "$ds" -eq 1 ]]; then
                rm -rf "$region_two"/8820
                cp -R  "$region_two"/8820.old "$region_two"/8820
            else
                rm -rf "$region_three"/8830
                cp -R  "$region_three"/8830.old "$region_three"/8830
            fi

            echo "$(date) regions moved, current dump output:" >> "$test_log"
            "$cds" dump \
                    -d  "$region_one"/8810 \
                    -d "$region_two"/8820 \
                    -d "$region_three"/8830 >> "$test_log" 2>&1
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
                printf "Error %d in test\n" "$result" | tee -a "$loop_log"
                mv "$test_log" "$test_log".lastfail
                return 1
            fi
            printf "%6d %3d [%d][%d] " \
                    "$es" "$ec" "$i" "$ds" | tee -a "${loop_log}"

            grep "extents repaired" "$test_log" | tail -1 | tee -a "$loop_log"
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
        "$es" "$ec" $((ave / 60)) $((ave % 60)) \
        $((total / 60)) $((total % 60)) | tee -a "$loop_log"

    return 0
}

# The locations for each of the three regions that make up
# the region set.

# These hard coded values correspond to the zpools on Folgers.
# region_one=/oxp_d462a7f7-b628-40fe-80ff-4e4189e2d62b/repair_perf
# region_two=/oxp_e4b4dc87-ab46-49fb-a4b4-d361ae214c03/repair_perf
# region_three=/oxp_f4b4dc87-ab46-49fb-a4b4-d361ae214c03/repair_perf

# This is the default for dsc, and should work on most systems, but
# will not give the best performance.
region_one=/var/tmp/dsc/region1
region_two=/var/tmp/dsc/region2
region_three=/var/tmp/dsc/region3

block_size=4096
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
    codesign -s - -f "$dsc"
fi

echo "Begin $0 on system $(hostname) $(date)"
echo "Using downstairs: $cds"

### extent size loop starts here
#               ES   EC
repair_round     10  10  # This one just for sanity and to detect problems
repair_round   4096 100  #  16 MiB @ 4k block size
repair_round   8192 100  #  32 MiB
repair_round  16384 100  #  64 MiB
repair_round  32768 100  # 128 MiB
repair_round  49152 100  #  64 MiB
repair_round  65536 100  # 256 MiB

echo "Test done"
