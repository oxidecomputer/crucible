#!/bin/bash

# Memory usage test shell script.

set -o errexit
set -o pipefail

# Control-C to cleanup.
trap ctrl_c INT
function ctrl_c() {
    echo "Stopping at your request"
    if [[ -n "$dsc" ]]; then
        "$dsc" cmd shutdown > /dev/null 2>&1 || true
    fi
    exit 1
}

usage () {
    echo "Usage: $0 [f] [-b #] [-g <PATH>]" >&2
    echo " -b block_size  Block size for the region        (default 4096)" >&2
    echo " -g REGION_DIR  Directory where regions will be created" >&2
    echo "                                          (default /var/tmp/dsc)" >&2
}

block_size=4096
region_dir="/var/tmp/dsc"

while getopts 'b:g:h' opt; do
    case "$opt" in
        b)  block_size=$OPTARG
            echo "Using block size $block_size"
            ;;
        g)  region_dir=$OPTARG
            echo "Using region dir of $region_dir"
            ;;
        h)  usage
            exit 0
            ;;
        *)  echo "Invalid option"
            usage
            exit 1
            ;;
    esac
done

# Print some memory stats and how they relate to extent_count ($1)
function show_mem_summary() {
    ec=$1

    ds1=$(ps -ef | grep downstairs | grep 8810 | awk '{print $2}')
    ds2=$(ps -ef | grep downstairs | grep 8820 | awk '{print $2}')
    ds3=$(ps -ef | grep downstairs | grep 8830 | awk '{print $2}')

    total_downstairs=0
    # Header for the memory usage summary
    printf "%6s %7s %6s %7s %6s %7s %7s %7s %8s %6s\n" \
        "PID" "RSS" "RSS/EC" "VSZ" "VSZ/EC" "HEAP" "HEAP/EC" \
        "TOTAL" "TOTAL/EC" "EC"
    for ds_pid in "$ds1" "$ds2" "$ds3"; do
        ds_rss=$(pmap -x "$ds_pid" | grep "total" | awk '{print $4}')
        ds_rss_per_ec=$(echo "$ds_rss / $ec" | bc)

        ds_vsz=$(ps -o vsz= -p "$ds_pid")
        ds_vsz_per_ec=$(echo "$ds_vsz / $ec" | bc)

        ds_heap=$(pmap -x "$ds_pid" | grep " heap " | awk '{print $2}')
        ds_heap_per_ec=$(echo "$ds_heap / $ec" | bc)

        ds_total=$(pmap -x "$ds_pid" | grep "total" | awk '{print $3}')
        ds_total_per_ec=$(echo "$ds_total / $ec" | bc)

        printf "%6d %7d %6d %7d %6d %7d %7d %7d %8d %6d\n" \
            "$ds_pid" \
            "$ds_rss" "$ds_rss_per_ec" "$ds_vsz" "$ds_vsz_per_ec" \
            "$ds_heap" "$ds_heap_per_ec" "$ds_total" "$ds_total_per_ec" "$ec"

            (( total_downstairs += ds_total ))
    done
    # Abuse of global namespace warning for total_downstairs

}

# Location of logs and working files
WORK_ROOT=${WORK_ROOT:-/tmp}
mkdir -p "$WORK_ROOT"
test_mem_log="$WORK_ROOT/test_mem_log.txt"
# Create a region with the given extent_size ($1) and extent_count ($2)
# Once created, write to every block in the region, then display memory usage.
function mem_test() {
    if [[ $# -ne 2 ]]; then
        echo "Missing EC and ES for mem_test()" >&2
        exit 1
    fi
    es=$1
    ec=$2

    total_size=$(echo "$es * $ec * $block_size" | bc)
    size_mib=$(echo "$total_size / 1024 / 1024" | bc)
    size_gib=$(echo "$size_mib / 1024" | bc)

    echo -n "Region with ES:$es EC:$ec BS:$block_size  "
    if [[ "$size_gib" -gt 0 ]]; then
        reported_size="$size_gib GiB"
    elif [[ "$size_mib" -gt 0 ]]; then
        reported_size="$size_mib MiB"
    else
        reported_size="$total_size"
    fi
    echo -n "Size: $reported_size  "

    each_extent=$(echo "$es * $block_size" | bc)
    each_extent_mib=$(echo "$each_extent / 1024 / 1024" | bc)
    if [[ "$each_extent_mib" -gt 0 ]]; then
        reported_extent_size="$each_extent_mib MiB"
    else
        reported_extent_size="$each_extent"
    fi
    echo "Extent Size: $reported_extent_size"


    "$dsc" create --ds-bin "$downstairs" --cleanup \
        --extent-size "$es" --extent-count "$ec" \
	    --region-dir "$region_dir" --block-size "$block_size" \
        > "$test_mem_log" 2>&1

    "$dsc" start --ds-bin "$downstairs" --region-dir "$region_dir" \
        > "$test_mem_log" 2>&1 &
    dsc_pid=$!
    sleep 5
    if ! pgrep -P $dsc_pid > /dev/null; then
        echo "Failed to start dsc"
        exit 1
    fi
    if ! "$dsc" cmd disable-restart-all; then
        echo "Failed to disable auto-restart on dsc"
        exit 1
    fi

    # Args for crutest.  Using the default IP:port for dsc
    args="-t 127.0.0.1:8810 -t 127.0.0.1:8820 -t 127.0.0.1:8830 -q"

    # Fill the region
    echo "$ct" fill $args --skip-verify -g 1 >> "$test_mem_log" 2>&1
    "$ct" fill $args --skip-verify -g 1 >> "$test_mem_log" 2>&1

    show_mem_summary "$ec"
    echo -n "Region:$reported_size  Extent:$reported_extent_size  "
    total_downstairs_mib=$(echo "$total_downstairs / 1024" | bc)
    if [[ "$total_downstairs_mib" -gt 0 ]]; then
        reported_downstairs="$total_downstairs_mib MiB"
    else
        reported_downstairs="$total_downstairs"
    fi
    echo "Total downstairs (pmap -x): $reported_downstairs"

    region_summary=$(du -sAh "$region_dir" | awk '{print $1}')
    region_size=$(du -sA "$region_dir" | awk '{print $1}')
    echo "Size on disk of all region dirs: $region_summary or $region_size"

    set +o errexit
    "$dsc" cmd shutdown >> "$test_mem_log" 2>&1
    wait $dsc_pid
    unset dsc_pid
    set -o errexit
    echo ""
}

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BINDIR=${BINDIR:-$ROOT/target/release}

echo "$ROOT"
cd "$ROOT"

ct="$BINDIR/crutest"
dsc="$BINDIR/dsc"
downstairs="$BINDIR/crucible-downstairs"

for bin in $dsc $ct $downstairs; do
    if [[ ! -f "$bin" ]]; then
        echo "Can't find crucible binary at $bin" >&2
        exit 1
    fi
done

if pgrep -fl -U "$(id -u)" "$downstairs"; then
    echo "Downstairs already running" >&2
    echo Run: pkill -f -U "$(id -u)" crucible-downstairs >&2
    exit 1
fi

echo "Memory usage test begins at $(date)"
echo "Memory usage values in kilobytes unless specified otherwise"

#            ES   EC
mem_test 16384    16
mem_test 16384   160
mem_test 16384  1600

echo "Memory usage test finished on $(date)"
