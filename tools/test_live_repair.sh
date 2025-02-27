#!/bin/bash

# Create regions and start up four downstairs.
# We create four and use the forth downstairs as the replacement
# downstairs.
# Run the crutest replace (live_repair) test using the downstairs
# we just started.  Each test lap will do 50 replacements and we
# verify our volume on start and record what we wrote on exit
# so the next loop will start assuming to read the data the previous
# loop wrote.
err=0
total=0
pass_total=0
SECONDS=0

# Control-C to cleanup.
trap ctrl_c INT
function ctrl_c() {
    echo "Stopping at your request"
    ${dsc} cmd shutdown
    exit 1
}

REGION_ROOT=${REGION_ROOT:-/var/tmp/test_live_repair}
mkdir -p "$REGION_ROOT"

# Location of logs and working files
WORK_ROOT=${WORK_ROOT:-/tmp}
TEST_ROOT="$WORK_ROOT/test_live_repair"
if [[ ! -d "$TEST_ROOT" ]]; then
    mkdir -p "$TEST_ROOT"
    if [[ $? -ne 0 ]]; then
        echo "Failed to make test root $TEST_ROOT"
    exit 1
    fi  
else
    # Delete previous test data
    rm -r "$TEST_ROOT"
fi

loop_log="$TEST_ROOT"/test_live_repair_summary.log
test_log="$TEST_ROOT"/test_live_repair.log
verify_log="$TEST_ROOT/test_live_repair_verify.log"
dsc_ds_log="$TEST_ROOT/test_live_repair_dsc.log"

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT" || (echo failed to cd "$ROOT"; exit 1)
export BINDIR=${BINDIR:-$ROOT/target/debug}
crucible_test="$BINDIR/crutest"
dsc="$BINDIR/dsc"
downstairs="$BINDIR/crucible-downstairs"
if [[ ! -f "$crucible_test" ]] || [[ ! -f "$dsc" ]] || [[ ! -f "$downstairs" ]]; then
    echo "Can't find required binaries"
    echo "Missing $crucible_test or $dsc or $downstairs"
    exit 1
fi

loops=5
region_sets=1

usage () {
    echo "Usage: $0 [-l #] [-r #]" >&2
    echo " -l loops       Number of replacement loops to perform (default 5)" >&2
    echo " -r region_sets Number of region sets to create (default 1)" >&2
}

while getopts 'l:r:' opt; do
    case "$opt" in
        l)  loops=$OPTARG
            ;;
        r)  region_sets=$OPTARG
            ;;
        *)  echo "Invalid option"
            usage
            exit 1
            ;;
    esac
done

((region_count=region_sets*3))
((region_count+=1))
rm -f "$loop_log"
rm -f "$test_log"
echo "starting $(date)" | tee "$loop_log"
echo "Tail $test_log for test output"

# No real data was used to come up with these numbers.  If you have some data
# then feel free to change things.
if [[ $region_sets -eq 1 ]]; then
    extent_size=3000
elif [[ $region_sets -eq 2 ]]; then
    extent_size=1500
elif [[ $region_sets -eq 3 ]]; then
    extent_size=750
else
    extent_size=500
fi

# NOTE: we create the requested number of regions here plus one more region to
# be used by the replace test.  We can use dsc to determine what the port will
# be for the final region.
if ! ${dsc} create --cleanup \
  --region-dir "$REGION_ROOT" \
  --region-count "$region_count" \
  --output-dir "$dsc_ds_log" \
  --ds-bin "$downstairs" \
  --extent-size "$extent_size" \
  --extent-count 200 >> "$test_log"; then
    echo "Failed to create downstairs regions"
    exit 1
fi
${dsc} start --ds-bin "$downstairs" \
  --region-dir "$REGION_ROOT" \
  --output-dir "$dsc_ds_log" \
  --region-count "$region_count" >> "$test_log" 2>&1 &
dsc_pid=$!
sleep 5
if ! ps -p $dsc_pid > /dev/null; then
    echo "$dsc failed to start"
    exit 1
fi

gen=1
# Seed the initial volume
echo "$(date) Begin pretest initial fill" | tee -a "$test_log"
if ! "$crucible_test" fill --dsc 127.0.0.1:9998 -q -g "$gen"\
          --skip-verify >> "$test_log" 2>&1 ; then
    echo Failed on initial fill, check "$test_log"
    ${dsc} cmd shutdown
    exit 1
fi
(( gen += 1 ))

# Figure out the port of the last dsc client, this is what we will use for the
# replacement address.
((last_client=region_count - 1))
replacement_port=$(${dsc} cmd port -c $last_client)

# Now run the crutest replace test
SECONDS=0
cp "$test_log" "$test_log".last
echo "" > "$test_log"
echo "$(date) Replacement test starts now" | tee -a "$test_log"
"$crucible_test" replace -c "$loops" \
        --dsc 127.0.0.1:9998 \
        --replacement 127.0.0.1:"$replacement_port" \
        --stable -g "$gen" >> "$test_log" 2>&1
result=$?
duration=$SECONDS
if [[ $result -ne 0 ]]; then
    printf "Error $result after %d:%02d\n" \
            $((duration / 60)) $((duration % 60)) | tee -a "$loop_log"
    cp "$test_log" "$test_log".lastfail
    echo "See ${test_log}.lastfail for more info"
else
    printf "Test took: %d:%02d\n" \
      $((duration / 60)) $((duration % 60)) | tee -a "$loop_log"
fi

${dsc} cmd shutdown
wait "$dsc_pid"

echo "$(date) Test ends with $result" | tee -a "$test_log"

if [[ $result -eq 0 ]]; then
    echo "DELETE 8840" | tee -a "$test_log"
    ps -ef | grep crucible-downstairs
    rm -rf "$REGION_ROOT"/8810
    rm -rf "$REGION_ROOT"/8820
    rm -rf "$REGION_ROOT"/8830
    rm -rf "$REGION_ROOT"/8840
    rm -rf "$TEST_ROOT"
fi

exit $result
