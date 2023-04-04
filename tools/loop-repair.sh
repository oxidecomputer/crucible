#!/bin/bash

# Script to trigger a repair on a random downstairs.
# Each loop will select a new downstairs and stop/fault/start
# that downstairs which should kick off a repair.


#
# Make a test that:
# starts IO
# Faults a downstairs.
# waits for repair to start.
# Faults a downstairs again.

# Also,
# Faults a downstairs
# waits for repair to start.
# Faults a 2nd downstairs.
# waits for repair to finish on 1
# waits for 2nd repair to start on 2

set -eu
total=0

upstairs="http://127.0.0.1:7777"
info_cmd="curl -s $upstairs/info"
# If no jq, then exit

# Gather some initial repair/confirm stats before we start.
initial_repair_zero=$($info_cmd | jq ".extents_repaired[0]")
initial_confirm_zero=$($info_cmd | jq ".extents_confirmed[0]")
initial_repair_one=$($info_cmd | jq ".extents_repaired[1]")
initial_confirm_one=$($info_cmd | jq ".extents_confirmed[1]")
initial_repair_two=$($info_cmd | jq ".extents_repaired[2]")
initial_confirm_two=$($info_cmd | jq ".extents_confirmed[2]")

# Start some IO test that generates IO to the downstairs.
# Have that test take somewhere around the time this loop will take.

for i in {1..10}; do

    SECONDS=0

    choice=$((RANDOM % 3))

    repair_start=$($info_cmd | jq ".extents_repaired[$choice]")
    confirm_start=$($info_cmd | jq ".extents_confirmed[$choice]")

    # ./target/release/dsc cmd stop -c "$choice"
    if ! curl -X POST ${upstairs}/downstairs/fault/"$choice"; then
        echo "Failed to send fault request"
        exit 1
    fi

    # Wait for upstairs to see this has faulted.
    #while :; do
    #    if $info_cmd | jq ".ds_state[$choice]" | grep faulted > /dev/null; then
    #        break
    #    fi
    #    sleep 1
    #done

    #./target/release/dsc cmd start -c "$choice"

    # Wait for the repair to start
    # This will catch both start and start and completed if it happens fast.
    while :; do
        repaired=$($info_cmd | jq ".extents_repaired[$choice]")
        confirmed=$($info_cmd | jq ".extents_confirmed[$choice]")

        if [[ "$repaired" -gt "repair_start" ]] || [[ "$confirmed" -gt "$confirm_start" ]]; then
            break
        fi
        sleep 5
    done

    # Now, wait for online_repair to finish.  Note that if the repair has
    # already finished that is okay.
    while $info_cmd | jq ".ds_state[$choice]" | grep online_repair > /dev/null; do
        sleep 5
    done

    # Make sure the upstairs is still running
    # This can become a return code from the pid generated above?
    if ! curl -s -X POST http://127.0.0.1:7777/info > /dev/null; then
        echo "Failed to verify upstairs is running"
        exit 1
    fi

    duration=$SECONDS
    (( total += duration ))
    ave=$(( total / i ))
    printf "[%03d][%d] %2d:%02d  ave:%2d:%02d  total:%3d:%02d\n" \
        "$i" \
        $choice \
        $((duration / 60)) $((duration % 60)) \
        $((ave / 60)) $((ave % 60)) \
        $((total / 60)) $((total % 60))

done

# Now, wait on the test to finish and verify?
# Make sure tests exits okay.

# Gather up some stats and make it look pretty.
up_jobs=$(curl -s http://127.0.0.1:7777/info | jq ".up_jobs")
ds_jobs=$(curl -s http://127.0.0.1:7777/info | jq ".ds_jobs")
echo "Running Upstairs jobs: $up_jobs   Running downstairs jobs: $ds_jobs"

# How much repair did we do.
final_repair_zero=$($info_cmd | jq ".extents_repaired[0]")
final_confirm_zero=$($info_cmd | jq ".extents_confirmed[0]")
final_repair_one=$($info_cmd | jq ".extents_repaired[1]")
final_confirm_one=$($info_cmd | jq ".extents_confirmed[1]")
final_repair_two=$($info_cmd | jq ".extents_repaired[2]")
final_confirm_two=$($info_cmd | jq ".extents_confirmed[2]")

diff_repair_zero=$((final_repair_zero - initial_repair_zero))
diff_repair_one=$((final_repair_one - initial_repair_one))
diff_repair_two=$((final_repair_two - initial_repair_two))
diff_confirm_zero=$((final_confirm_zero - initial_confirm_zero))
diff_confirm_one=$((final_confirm_one - initial_confirm_one))
diff_confirm_two=$((final_confirm_two - initial_confirm_two))

# Display the summary results.
printf "       %5d %5d %5d           %5d %5d %5d\n" 0 1 2 0 1 2
printf "REPAIR %5d %5d %5d   CONFIRM %5d %5d %5d\n" \
    "$initial_repair_zero" "$initial_repair_one" "$initial_repair_two" \
    "$initial_confirm_zero" "$initial_confirm_one" "$initial_confirm_two"
printf "REPAIR %5d %5d %5d   CONFIRM %5d %5d %5d\n" \
    "$final_repair_zero" "$final_repair_one" "$final_repair_two" \
    "$final_confirm_zero" "$final_confirm_one" "$final_confirm_two"
printf " DELTA %5d %5d %5d           %5d %5d %5d\n" \
    "$diff_repair_zero" "$diff_repair_one" "$diff_repair_two" \
    "$diff_confirm_zero" "$diff_confirm_one" "$diff_confirm_two"

date
