#!/bin/bash

# Perf test shell script.

set -o errexit
set -o pipefail

# Control-C to cleanup.
trap ctrl_c INT
function ctrl_c() {
    echo "Stopping at your request"
    if [[ -n "$fds" ]]; then
        "$dsc" cmd shutdown
    fi
    exit 1
}

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
    # Args for crutest.  Using the default IP:port for dsc
    args="-g 1 -t 127.0.0.1:8810 -t 127.0.0.1:8820 -t 127.0.0.1:8830 -c 16000 -q"

    echo Create region with ES:"$es" EC:"$ec"
    "$dsc" create --ds-bin "$downstairs" --cleanup --extent-size  "$es" --extent-count "$ec"
    "$dsc" start --ds-bin "$downstairs" &
    dsc_pid=$!
    sleep 5
    if ! pgrep -P $dsc_pid; then
        echo "Failed to start dsc"
        exit 1
    fi
    if ! "$dsc" cmd disable-restart-all; then
        echo "Failed to disable auto-restart on dsc"
        exit 1
    fi
    echo "IOPs for es=$es ec=$ec" >> "$outfile"
    echo "$ct" perf $args --perf-out /tmp/perf-ES-"$es"-EC-"$ec".csv | tee -a "$outfile"
    "$ct" perf $args --perf-out /tmp/perf-ES-"$es"-EC-"$ec".csv 2>&1 | tee -a "$outfile"
    echo "" >> "$outfile"
    echo Perf test completed, stop all downstairs
    "$dsc" cmd shutdown
    unset dsc_pid
}

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BINDIR=${BINDIR:-$ROOT/target/release}

echo "$ROOT"
cd "$ROOT"

if pgrep -fl -U "$(id -u)" crucible-downstairs; then
    echo "Downstairs already running" >&2
    echo Run: pkill -f -U "$(id -u)" crucible-downstairs >&2
    exit 1
fi

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

echo "Perf test with timeout begins at $(date)" > "$outfile"

#            ES   EC
#perf_round  4096 6400
#perf_round  8192 3200
#perf_round 16384 1600
perf_round 32768  800

# Print out a nice summary of all the perf results.  This depends
# on the header and client perf output matching specific strings.
# A hack, yes, sorry. We are in bash hell here.
echo ""
grep TEST $outfile | head -1
grep rwrites $outfile
echo ""
grep TEST $outfile | head -1
grep rreads $outfile

echo "Perf test finished on $(date)" | tee -a "$outfile"
