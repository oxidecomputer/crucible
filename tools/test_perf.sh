#!/bin/bash

# Perf test shell script.

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
    echo "Usage: $0 [f] [-b #] [-c #] [-g <PATH>] [-r #] [-w #]" >&2
    echo " -b block_size    Block size for the region     (default 4096)" >&2
    echo " -c count         Count of IOs in the perf test (default 30000)" >&2
    echo " -f               Write to all blocks once before testing" >&2
    echo " -g REGION_DIR    Directory where regions will be created" >&2
    echo "                                           (default /var/tmp/dsc)" >&2
    echo " -r read_loops    Number of read loops to do    (default 2)" >&2
    echo " -w write_loops   Number of write loops to do   (default 2)" >&2
}

block_size=4096
count=30000
prefill=0
read_loops=2
write_loops=2
region_dir="/var/tmp/dsc"

while getopts 'b:c:g:hfr:w:' opt; do
    case "$opt" in
        b)  block_size=$OPTARG
            echo "Using block size $block_size"
            ;;
        c)  count=$OPTARG
            echo "Using count of $count"
            ;;
        f)  prefill=1
            echo "Region will be filled once before testing"
            ;;
        g)  region_dir=$OPTARG
            echo "Using region dir of $region_dir"
            ;;
        h)  usage
            exit 0
            ;;
        r)  read_loops=$OPTARG
            echo "Using read_loops: $read_loops"
            ;;
        w)  write_loops=$OPTARG
            echo "Using write_loops: $write_loops"
            ;;
        *)  echo "Invalid option"
            usage
            exit 1
            ;;
    esac
done

# Create a region with the given extent_size ($1) and extent_count ($2)
# Once created, run the client perf test on that region, recording the
# output to a csv file.
function perf_round() {
    if [[ $# -ne 2 ]]; then
        echo "Missing EC and ES for perf_round()" >&2
        exit 1
    fi
    es=$1
    ec=$2

    echo Create region with ES:"$es" EC:"$ec" BS:"$block_size"

    "$dsc" create --ds-bin "$downstairs" --cleanup \
        --extent-size "$es" --extent-count "$ec" \
        --region-dir "$region_dir" --block-size "$block_size"

    "$dsc" start --ds-bin "$downstairs" --region-dir "$region_dir" &
    dsc_pid=$!
    sleep 5
    if ! pgrep -P $dsc_pid; then
        echo "Failed to start dsc"
        exit 1
    fi
    # Sometimes, this command fails.  When it does, gather a bit more
    # information to help us debug it.  And, throw in a sleep/retry.
    if ! "$dsc" cmd disable-restart-all; then
        echo "Failed to disable auto-restart on dsc"
        echo "dsc_pid: $dsc_pid"
        ps -ef | grep $dsc_pid
        echo "ps grep downstairs"
        ps -ef | grep downstairs
        echo "ps grep dsc"
        ps -ef | grep dsc
        sleep 20
        if ! "$dsc" cmd disable-restart-all; then
            echo "Failed twice to disable auto-restart on dsc"
            exit 1
        fi
    fi

    # Args for crutest.  Using the default IP:port for dsc
    gen=1
    args="-t 127.0.0.1:8810 -t 127.0.0.1:8820 -t 127.0.0.1:8830 -c $count -q"

    echo "A new loop begins at $(date)" | tee -a "$outfile"

    if [[ "$prefill" -eq 1 ]]; then
        # Fill the region
        echo "$ct" fill $args --skip-verify -g "$gen" | tee -a "$outfile"
        "$ct" fill $args --skip-verify -g "$gen" | tee -a "$outfile"
        echo "IOPs for es=$es ec=$ec" >> "$outfile"
        (( gen += 1 ))
        echo "" | tee -a "$outfile"
    fi

    # Test after the fill
    echo "$ct" perf -g "$gen" $args --write-loops "$write_loops" --read-loops "$read_loops" --perf-out /tmp/perf-ES-"$es"-EC-"$ec".csv | tee -a "$outfile"
    "$ct" perf -g "$gen" $args \
        --write-loops "$write_loops" --read-loops "$read_loops" \
        --perf-out /tmp/perf-ES-"$es"-EC-"$ec".csv 2>&1 | tee -a "$outfile"
    echo "" >> "$outfile"

    echo Perf test completed, stop all downstairs
    set +o errexit
    "$dsc" cmd shutdown
    wait $dsc_pid
    unset dsc_pid
    set -o errexit
}

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BINDIR=${BINDIR:-$ROOT/target/release}

echo "$ROOT"
cd "$ROOT"

ct="$BINDIR/crutest"
dsc="$BINDIR/dsc"
downstairs="$BINDIR/crucible-downstairs"
outfile="/tmp/perfout.txt"

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

echo "Perf test begins at $(date)" > "$outfile"

#            ES   EC
perf_round  65536  3200
perf_round 131072  1600

# Print out a nice summary of all the perf results.  This depends
# on the header and client perf output matching specific strings.
# A hack, yes, sorry. We are in bash hell here.
echo ""
grep TEST $outfile | head -1
grep rwrites $outfile || true
echo ""
grep TEST $outfile | head -1
grep rreads $outfile || true

echo "Perf test finished on $(date)" | tee -a "$outfile"
