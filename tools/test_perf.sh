#!/bin/bash

# Perf test shell script.

set -o errexit
set -o pipefail

# Control-C to cleanup.
trap ctrl_c INT
function ctrl_c() {
    echo "Stopping at your request"
    pkill -f -U "$(id -u)" downstairs
    if [[ -n "$dsc_pid" ]]; then
        kill "$dsc_pid"
    fi
    exit 1
}

# Create a region with the given extent_size ($1) and extent_count ($2)
# Once created, run the client perf test on that region, recording the
# output to a csv file.
function perf_round() {
    if [[ $# -ne 2 ]]; then
        echo "Missing EC and ES for perf_round()"
        exit -1
    fi
    es=$1
    ec=$2
    # Args for crucible-client.  Using the default IP:port for dsc
    args="-t 127.0.0.1:8810 -t 127.0.0.1:8820 -t 127.0.0.1:8830 -c 4000 -q"

    echo Create region with ES:"$es" EC:"$ec"
    "$dsc" start --ds-bin "$downstairs" --cleanup --extent-size  "$es" --extent-count "$ec" &
    dsc_pid=$!
    sleep 5
    pfiles $dsc_pid > /dev/null
    echo "IOPs for es=$es ec=$ec" >> "$outfile"
    echo "$cc" perf $args --perf-out perf-ES-"$es"-EC-"$ec".csv | tee -a "$outfile"
    "$cc" perf $args --perf-out perf-ES-"$es"-EC-"$ec".csv | tee -a "$outfile"
    echo "" >> "$outfile"
    echo Perf test completed, stop all downstairs
    pkill -f -U "$(id -u)" downstairs
    kill $dsc_pid
    wait $dsc_pid || true
}

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BINDIR=${BINDIR:-$ROOT/target/release}

echo "$ROOT"
cd "$ROOT"

if pgrep -fl -U "$(id -u)" downstairs; then
    echo "Downstairs already running"
    echo Run: pkill -f -U "$(id -u)" downstairs
    exit 1
fi

cc="$BINDIR/crucible-client"
dsc="$BINDIR/dsc"
downstairs="$BINDIR/crucible-downstairs"
outfile="/tmp/perfout.txt"

for bin in $dsc $cc $downstairs; do
    if [[ ! -f "$bin" ]]; then
        echo "Can't find crucible binary at $bin" >&2
        exit 1
    fi
done

echo "Perf test begins at $(date)" > "$outfile"

#            ES   EC
perf_round  4096 6400
perf_round  8192 3200
perf_round 16384 1600
perf_round 32768  800

# Print out a nice summary of all the perf results.  This depends
# on the header and client perf output matching specific strings.
# A hack, yes, sorry. We are in bash hell here.
echo ""
grep TEST $outfile | head -1
grep rwrites $outfile
grep rreads $outfile

echo "Perf test finished on $(date)" | tee -a "$outfile"
