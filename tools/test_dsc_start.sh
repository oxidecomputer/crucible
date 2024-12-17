#!/bin/bash
#
# Test stopping and starting downstairs via dsc.

set -o pipefail
SECONDS=0

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BINDIR=${BINDIR:-$ROOT/target/debug}

echo "$ROOT"
cd "$ROOT" || (echo failed to cd "$ROOT"; exit 1)

if pgrep -fl crucible-downstairs | grep -v "/opt/oxide"; then
    echo 'Downstairs already running?' >&2
    sleep 5
fi

cds="$BINDIR/crucible-downstairs"
dsc="$BINDIR/dsc"
for bin in $cds $dsc; do
    if [[ ! -f "$bin" ]]; then
        echo "Can't find crucible binary at $bin" >&2
        exit 1
    fi
done

# Control-C to cleanup.
trap ctrl_c INT
function ctrl_c() {
	echo "Stopping at your request"
    "$dsc" cmd --server $dsc_control shutdown
    exit 1
}

loop=0
# Use the id of the current user to make a unique path
user=$(id -u -n)
port_base=8810
dsc_control="127.0.0.1:9998"
region_sets=1

function usage {
    echo "Usage: $0 [-c IP:PORT] [-p #] [-r #] [-u name]"
    echo "c: IP:PORT  IP:Port where dsc should listen (default: 127.0.0.1:9998)"
    echo "p: PORT  Port number base for downstairs to use (default: 8810)"
    echo "P:       Pause when we get an error and wait forever."
    echo "r: #     Number of region sets to create (default: 1)"
    echo "u: name  Name to append to test dir (default: id -un )"
    exit 1
}

dsc_args=()
dsc_create_args=()
res=0
pause_on_error=0

shift_count=0
while getopts 'c:hPp:r:u:' opt; do
    case "$opt" in
	c) dsc_control=$OPTARG
	    echo "Using $dsc_control for dsc control"
	    ;;
    h) usage
        ;;
	p) port_base=$OPTARG
	    echo "Using $port_base for downstairs port base"
	    ;;
	P) pause_on_error=1
	    echo "Pause forever when we hit an error"
	    ;;
	r) region_sets=$OPTARG
	    echo "Using $region_sets region sets"
	    ;;
	u) user=$OPTARG
	    echo "Using $user for test name"
	    ;;
	*) usage
	    ;;
    esac
done

((region_count=region_sets*3))

loops=0
region_dir="/var/tmp/test_dsc_start-$user"
test_output_dir="/tmp/test_dsc_start-$user"

log_prefix="${test_output_dir}/test_dsc_start"
fail_log="${log_prefix}_fail.txt"
dsc_output_dir="${test_output_dir}/dsc"

mkdir -p "$dsc_output_dir"
dsc_output="${test_output_dir}/dsc-out.txt"
echo "dsc output goes to $dsc_output"

dsc_create_args+=( --cleanup )
dsc_args+=( --output-dir "$dsc_output_dir" )
dsc_args+=( --ds-bin "$cds" )
dsc_args+=( --region-dir "$region_dir" )
dsc_args+=( --port-base "$port_base" )

# Pulled creation out of the loop to just do once.
# Downstairs regions go in this directory
if [[ -d "$region_dir" ]]; then
    rm -rf "$region_dir"
fi

rm -rf "$test_output_dir" 2> /dev/null
mkdir -p "$test_output_dir"
rm -f "$fail_log"

echo "[$loops] Creating $region_count downstairs regions"
echo "${dsc}" create "${dsc_create_args[@]}" --region-count $region_count --extent-size 10 --extent-count 5 "${dsc_args[@]}" > "$dsc_output"
"${dsc}" create "${dsc_create_args[@]}" --region-count $region_count --extent-size 10 --extent-count 5 "${dsc_args[@]}" >> "$dsc_output" 2>&1
if [[ $? -ne 0 ]]; then
    echo "Failed on initial creation"
    exit 1
fi

# Loop on dsc creation forever.
errors=0
while :; do

	echo "[$loops][$errors] Starting $region_count downstairs                 "
	echo "${dsc}" start "${dsc_args[@]}" --control $dsc_control --region-count $region_count >> "$dsc_output"
	"${dsc}" start "${dsc_args[@]}" --control $dsc_control --region-count $region_count >> "$dsc_output" 2>&1 &
	dsc_pid=$!
	echo "[$loops] dsc started at PID: $dsc_pid, control: $dsc_control"

	sleep 2
	if ! pgrep -P $dsc_pid > /dev/null; then
        echo ""
	    echo "[$loops] Gosh diddly darn it, dsc at $dsc_pid did not start"
	    exit 1
	fi

	# Wait for all clients to be running.
	echo "[$loops] waiting for $region_count downstairs to be running             "
	cid=0
    sleep_time=1
    wait_count=0
    found_error=0
	while [[ "$cid" -lt "$region_count" ]]; do
	    state=$("$dsc" cmd --server $dsc_control state -c "$cid")
	    if [[ "$state" != "Running" ]]; then
	        ((wait_count += 1))
            echo "[$loops][$wait_count] Downstairs client $cid not running, waiting for it"
            sleep "$sleep_time"
            if [[ "$sleep_time" -lt 5 ]]; then
                ((sleep_time += 1))
            fi

            if [[ "$wait_count" -gt 20 ]]; then
                echo ""
                echo "[$loops] I've waited long enough, giving up"
                date
                ps -ef | grep crucible-downstairs | grep -v oxide | grep -v grep
	            ((loops+=1))
	            ((errors+=1))
                # Kill our downstairs (but not any oxide created ones)
	        if [[ pause_on_error -eq 1 ]]; then
                    echo "[$loops] Pause here forever"
		    while :; do
		        sleep 300
		    done
		fi
                for pid in $(ptree $$ | grep "crucible-downstairs run" | grep -v oxide | grep -v grep | awk '{print $1}'); do
                    kill $pid;
                done
                # Kill any dsc:
                kill $(ptree $$ | grep "dsc start" | grep -v grep| awk '{print $1}')
                found_error=1
                break
            fi
            sleep $sleep_time
        else
	        ((cid += 1))
            sleep_time=1
	    fi
	done

	# Tests done, shut down the downstairs.
    if [[ $found_error -eq 0 ]]; then
        echo "[$loops] All downstairs started, shutdown dsc              "
        if ! "$dsc" cmd --server $dsc_control shutdown; then
            (( res += 1 ))
            echo ""
            echo "Failed dsc shutdown"
            echo "Failed dsc shutdown" >> "$fail_log"
            exit 1
        fi
    fi
	echo "[$loops] Wait for dsc $dsc_pid to exit                     "
	wait $dsc_pid

	duration=$SECONDS
	printf "[$loops][$errors] %d:%02d Test duration                    \n" \
      $((duration / 60)) $((duration % 60))
	((loops+=1))
done
