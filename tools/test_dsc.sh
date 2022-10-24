#!/bin/bash
#
# Test dsc
# This walks through the list of dsc dropshot endpoints and makes sure
# we return the proper things in the proper situations.
#
# This, as in all things bash, should not live to a ripe old age, but
# instead, be replaced by a proper test using the API endpoints that
# I still have to write.

set -o pipefail
SECONDS=0

trap ctrl_c INT
function ctrl_c() {
    echo "Stopping at your request"
    cleanup
    exit 1
}

function cleanup() {
    if [[ -n $dsc_pid ]]; then
		kill "$dsc_pid" 2> /dev/null || true
        sleep 1
		kill -9 "$dsc_pid" 2> /dev/null || true
	fi
}

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BINDIR=${BINDIR:-$ROOT/target/debug}

echo "$ROOT"
cd "$ROOT" || exit 1

if pgrep -fl crucible-downstairs; then
    echo 'Downstairs already running?' >&2
    exit 1
fi

downstairs="$BINDIR/crucible-downstairs"
dsc="$BINDIR/dsc"
for bin in $downstairs $dsc; do
    if [[ ! -f "$bin" ]]; then
        echo "Can't find crucible binary at $bin" >&2
        exit 1
    fi
done

os_name=$(uname)
if [[ "$os_name" == 'Darwin' ]]; then
    # stupid macos needs this to avoid popup hell.
    codesign -s - -f "$dsc"
    codesign -s - -f "$downstairs"
fi

testdir="/var/tmp/test_dsc"
if [[ -d ${testdir} ]]; then
    rm -rf ${testdir}
fi

# Store log files we want to keep in /tmp/*.txt as this is what
# buildomat will look for and archive

log_prefix="/tmp/test_dsc"
fail_log="${log_prefix}_fail.txt"
rm -f "$fail_log"
res=0

outfile="/tmp/test_dsc.txt"
echo "" > $outfile

r1="/var/tmp/test_dsc_r1"
if [[ -d ${r1} ]]; then
    rm -rf ${r1}
fi
r2="/var/tmp/test_dsc_r2"
if [[ -d ${r2} ]]; then
    rm -rf ${r2}
fi
r3="/var/tmp/test_dsc_r3"
if [[ -d ${r3} ]]; then
    rm -rf ${r3}
fi
# test with three different directories
echo "$dsc" create --ds-bin "$downstairs" --extent-count 5 \
    --extent-size 5 --output-dir "$testdir" \
    --region-dir "$r1" \
    --region-dir "$r2" \
    --region-dir "$r3" | tee "$outfile"

"$dsc" create --ds-bin "$downstairs" --extent-count 5 \
    --extent-size 5 --output-dir "$testdir" \
    --region-dir "$r1" \
    --region-dir "$r2" \
    --region-dir "$r3" \
    | tee "$outfile"
if [[ $? -ne 0 ]]; then
    echo "Failed to create multi dir region" | tee -a "$fail_log"
    (( res += 1 ))
fi

# Assuming the default port of 8810 here
for rdirs in "$r1"/8810 "$r2"/8820 "$r3"/8830
do
    if [[ ! -d "$rdirs" ]]; then
        echo "Failed to create $rdirs region" | tee -a "$fail_log"
        (( res += 1 ))

    fi
done

# Cleanup after above test
rm -rf ${testdir} ${r1} ${r2} ${r3}

echo "$dsc" create --ds-bin "$downstairs" --extent-count 5 \
    --extent-size 5 --output-dir "$testdir" \
    --region-dir "$testdir" \
    --encrypted | tee "$outfile"

${dsc} create --ds-bin "$downstairs" --extent-count 5 \
    --extent-size 5 --output-dir "$testdir" \
    --region-dir "$testdir" \
    --encrypted | tee -a "$outfile"
if [[ $? -ne 0 ]]; then
    # If this fails, then all the rest is going to fail as well.
    echo "Failed to create regions for dsc" | tee -a "$fail_log"
    cleanup
    exit 1
fi

echo "Running dsc:"
echo "${dsc}" start --ds-bin "$downstairs" \
    --output-dir "$testdir" \
    --region-dir "$testdir" | tee -a "$outfile"

${dsc} start --ds-bin "$downstairs" \
    --output-dir "$testdir" \
    --region-dir "$testdir" >> "$outfile" 2>&1 &
dsc_pid=$!

echo "dsc running at: $dsc_pid  log at $outfile"
sleep 3
if ! pgrep -P $dsc_pid; then
    echo "Failed to start dsc"
    exit 1
fi

# Some common flags for all to use.  If we need to break out the port or
# build specific URLs for endpoints, then we can do that here, but I'm
# not doing that work till we need it., and like stated above, this should
# all go away with a proper API client test.
curl_flags="-o /dev/null -s "
dsc_url="http://127.0.0.1:9998/"

echo "Enable automatic restart on all downstairs"
echo "${dsc}" cmd enable-restart-all
if ! "${dsc}" cmd enable-restart-all; then
    echo "Failed to enable auto restart" | tee -a "$fail_log"
    (( res += 1 ))
fi

# Loop through all the valid client IDs and verify that each
# one will restart automatically after stopping it.  This tests
# both the stop, the auto start, and returning the PID
for cid in {0..2}; do
    # This is used to compare our PID value to make sure it is a digit
    isdigit='^[0-9]+$'

	# First, get the PID for our client ID, make sure it is valid
    echo "Get first pid for client $cid"
    pid_a=$(curl -s "${dsc_url}"pid/cid/"$cid")
    echo "curl for cid $cid returns $?, pid is $pid_a"
    if [[ -z $pid_a ]]; then
        echo "Failed to get pid for cid $cid" | tee -a "$fail_log"
        (( res += 1 ))
        continue
    fi
	if ! [[ $pid_a =~ $isdigit ]] ; then
		echo "Pid for $cid not a number: $pid_a" | tee -a "$fail_log"
        (( res += 1 ))
       continue
	fi

	# Next, stop the downstairs.  It should restart with a new pid.
    echo "Stop client id $cid"
    hc=$(curl ${curl_flags} -X POST -H "Content-Type: application/json" -w "%{http_code}\n" "${dsc_url}"stop/cid/"$cid")
    if [[ "$hc" -ne 204 ]]; then
        echo "Failed to stop cid:$cid http:$hc" | tee -a "$fail_log"
        (( res += 1 ))
        continue
    fi

	# Give the restart code time to restart.
    sleep 2
    echo "Get 2nd pid for client $cid"
    pid_b=$(curl -s "${dsc_url}"pid/cid/"$cid")
    if [[ -z $pid_b ]]; then
        echo "Failed to get 2nd pid for cid:$cid" | tee -a "$fail_log"
        (( res += 1 ))
        continue
    fi
	if ! [[ $pid_b =~ $isdigit ]] ; then
		echo "2nd pid for $cid not a number: $pid_b" | tee -a "$fail_log"
        (( res += 1 ))
        continue
    fi

    echo "Verify pids are different after restart"
    if [[ "$pid_a" -eq "$pid_b" ]]; then
        echo "Failed: no new pid on restart test cid:$cid" | tee -a "$fail_log"
        (( res += 1 ))
    fi
done

sleep 2
echo "Test disable/all"
hc=$(curl ${curl_flags} -X POST -H "Content-Type: application/json" -w "%{http_code}\n" "${dsc_url}"disablerestart/all)
if [[ "$hc" -ne 204 ]]; then
    echo "Failed to disable restart all" | tee -a "$fail_log"
    (( res += 1 ))
fi

echo "Test stop/all"
hc=$(curl ${curl_flags} -w "%{http_code}\n" -X POST -H "Content-Type: application/json" "${dsc_url}"stop/all)
if [[ "$hc" -ne 204 ]]; then
    echo "Failed to stop all" | tee -a "$fail_log"
    (( res += 1 ))
fi

# After disabling restart, then stopping all, none of the
# downstairs should automatically restart.
sleep 4
echo "Verify all ds are stopped"
for cid in {0..2}; do
    retry=1
    # Loop until we find exit, or have exhausted our retry count
	while :; do
		state=$(curl -s "${dsc_url}"state/cid/0 | tr -d \")
		if [[ "$state" == "Exit" ]]; then
			echo "cid $cid in $state after $retry attempt(s)"
			break;
		fi
		echo "cid $cid Failed to stop: $state, retry" | tee -a "$fail_log"
		if [[ "$retry" -ge 10 ]]; then
			echo "cid $cid Failed to stop: $state, abort" | tee -a "$fail_log"
			(( res += 1 ))
			exit 1
			break;
		fi
		(( retry += 1 ))
		sleep 3
	done
done

echo "Start up ds 1 manually"
hc=$(curl ${curl_flags} -X POST -H "Content-Type: application/json" -w "%{http_code}\n" "${dsc_url}"start/cid/1)
if [[ "$hc" -ne 204 ]]; then
    echo "Failed to signal start to cid:1 after stop/all" | tee -a "$fail_log"
    (( res += 1 ))
fi

# It can take a few seconds for dsc to notice that things are
# down, then maybe a few more to resume after that.  This retry
# loop should be more than enough to cover any normal issues, but
# not so long that if things are broken, the tests will take forever.
sleep 2
echo "Verify ds 1 is now running"
retry=1
while :; do
	state=$(curl -s "${dsc_url}"state/cid/1 | tr -d \")
	if [[ "$state" == "Running" ]]; then
		echo "cid 1 restart: $state after $retry attempt(s)"
		break;
    fi
	echo "cid 1 Failed to restart: $state RETRY:$retry" | tee -a "$fail_log"
	if [[ "$retry" -ge 10 ]]; then
		echo "cid 1 Failed to restart: $state" | tee -a "$fail_log"
		(( res += 1 ))
		exit 1
		break
	fi
    (( retry += 1 ))
    sleep 3
done

echo "Stop ds 1, should not restart"
hc=$(curl ${curl_flags} -X POST -H "Content-Type: application/json" -w "%{http_code}\n" "${dsc_url}"stop/cid/1)
if [[ "$hc" -ne 204 ]]; then
    echo "Failed to stop cid:1 after stop/all then start" | tee -a "$fail_log"
    (( res += 1 ))
fi

sleep 2
state=$(curl -s "${dsc_url}"state/cid/1 | tr -d \")
if [[ "$state" != "Exit" ]]; then
    echo "cid 1 Failed to stop after stop/all then start, retry" | tee -a "$fail_log"
	sleep 4
	state=$(curl -s "${dsc_url}"state/cid/1 | tr -d \")
	if [[ "$state" != "Exit" ]]; then
		echo "cid 1 Failed to stop after stop/all then start" | tee -a "$fail_log"
		(( res += 1 ))
	fi
fi

# Tests using an invalid CID.
echo "Test invalid cid for pid"
hc=$(curl ${curl_flags} -w "%{http_code}\n" "${dsc_url}"pid/cid/3)
if [[ "$hc" -ne 400 ]]; then
    echo ""
    echo Failed to fail pid request with bad cid: "$hc" >> "$fail_log"
    (( res += 1 ))
fi

echo "Test invalid cid for state"
hc=$(curl ${curl_flags} -w "%{http_code}\n" "${dsc_url}"state/cid/3)
if [[ "$hc" -ne 400 ]]; then
    echo ""
    echo Failed to fail state request with bad cid: "$hc" >> "$fail_log"
    (( res += 1 ))
fi

echo "Test invalid cid for start"
hc=$(curl ${curl_flags} -X POST -H "Content-Type: application/json" -w "%{http_code}\n" "${dsc_url}"start/cid/3)
if [[ "$hc" -ne 400 ]]; then
    echo ""
    echo Failed to fail start request with bad cid: "$hc" >> "$fail_log"
    (( res += 1 ))
fi

echo "Test invalid cid for stop"
hc=$(curl ${curl_flags} -X POST -H "Content-Type: application/json" -w "%{http_code}\n" "${dsc_url}"stop/cid/3)
if [[ "$hc" -ne 400 ]]; then
    echo ""
    echo Failed to fail stop request with bad cid: "$hc" >> "$fail_log"
    (( res += 1 ))
fi

# The final test is to clean it all up!
echo "Test /shutdown endpoint"
hc=$(curl ${curl_flags} -w "%{http_code}\n" -X POST -H "Content-Type: application/json" "${dsc_url}"shutdown)
if [[ "$hc" -ne 204 ]]; then
    echo "Failed to shutdown, got $hc" | tee -a "$fail_log"
    (( res += 1 ))
else
    retry=0
    # Loop until we exit, or have exhausted our retry count
    while :; do
        if ps -p $dsc_pid > /dev/null; then
            (( retry += 1 ))
        else
            break;
        fi
        if [[ $retry -eq 5 ]]; then
            echo "Failed to shutdown with /shutdown" | tee -a "$fail_log"
            (( res += 1 ))
            break;
        fi
        sleep 1
    done
fi

# Tests are done
echo ""
echo "Tests done"
if ps -p $dsc_pid > /dev/null; then
    echo "Cleanup $dsc_pid"
    kill $dsc_pid
    sleep 1
    kill -9 $dsc_pid 2> /dev/null
    echo "Wait on pid $dsc_pid"
    wait $dsc_pid
fi

if [[ $res != 0 ]]; then
    echo "$res Tests have failed"
    cat "$fail_log"
else
    echo "All Tests have passed"
fi

echo ""
echo ""
echo "All done"
duration=$SECONDS
printf "%d:%02d Test duration\n" $((duration / 60)) $((duration % 60))

exit "$res"
