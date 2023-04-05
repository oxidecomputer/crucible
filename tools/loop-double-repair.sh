#!/bin/bash

# Script to trigger a repair on a random downstairs.
# Each loop will select a new downstairs and fault that downstairs which
# should kick off a repair.
# Once we see the repair is going, we fail a random downstairs, which
# could include our already repairing downstairs.
# Then, we wait for all three downstairs to be active again, and loop.

set -eu
total=0

upstairs="http://127.0.0.1:7777"
info_cmd="curl -s $upstairs/info"
info_log=/tmp/startinfo


$info_cmd > "$info_log"
#all_states=$(awk -F, '{print $2","$3","$4}' /tmp/startinfo)
#ds0state=$(echo $all_states | awk -F\" '{print $4}')
#ds1state=$(echo $all_states | awk -F\" '{print $6}')
#ds2state=$(echo $all_states | awk -F\" '{print $8}')

repair_info=$(awk -F, '{print $9","$10","$11}' $info_log | tr '[' ',' | tr ']' ',')
initial_repair_zero=$(echo $repair_info | awk -F, '{print $2}')
initial_repair_one=$(echo $repair_info | awk -F, '{print $3}')
initial_repair_two=$(echo $repair_info | awk -F, '{print $4}')

confirm_info=$(awk -F, '{print $12","$13","$14}' $info_log | tr '[' ',' | tr ']' ',')
initial_confirm_zero=$(echo $confirm_info | awk -F, '{print $2}')
initial_confirm_one=$(echo $confirm_info | awk -F, '{print $3}')
initial_confirm_two=$(echo $confirm_info | awk -F, '{print $4}')

# Start some IO test that generates IO to the downstairs.
# Have that test take somewhere around the time this loop will take.

for i in {1..200}; do

    SECONDS=0

    choice=$((RANDOM % 3))

    $info_cmd > "$info_log"
    repair_info=$(awk -F, '{print $9","$10","$11}' $info_log | tr '[' ',' | tr ']' ',')
    confirm_info=$(awk -F, '{print $12","$13","$14}' $info_log | tr '[' ',' | tr ']' ',')

    if [[ $choice -eq 0 ]]; then
        repair_start=$(echo $repair_info | awk -F, '{print $2}')
        confirm_start=$(echo $confirm_info | awk -F, '{print $2}')
    elif [[ $choice -eq 1 ]]; then
        repair_start=$(echo $repair_info | awk -F, '{print $3}')
        confirm_start=$(echo $confirm_info | awk -F, '{print $3}')
    else
        repair_start=$(echo $repair_info | awk -F, '{print $4}')
        confirm_start=$(echo $confirm_info | awk -F, '{print $4}')
    fi

    if ! curl -X POST ${upstairs}/downstairs/fault/"$choice"; then
        echo "Failed to send fault request"
        exit 1
    fi

    # Wait for the repair to start
    # This will catch both start and start and completed if it happens fast.
    while :; do
    	$info_cmd > "$info_log"
        repair_info=$(awk -F, '{print $9","$10","$11}' $info_log | tr '[' ',' | tr ']' ',')
        confirm_info=$(awk -F, '{print $12","$13","$14}' $info_log | tr '[' ',' | tr ']' ',')
        if [[ $choice -eq 0 ]]; then
            repaired=$(echo $repair_info | awk -F, '{print $2}')
            confirmed=$(echo $confirm_info | awk -F, '{print $2}')
        elif [[ $choice -eq 1 ]]; then
            repaired=$(echo $repair_info | awk -F, '{print $3}')
            confirmed=$(echo $confirm_info | awk -F, '{print $3}')
        else
            repaired=$(echo $repair_info | awk -F, '{print $4}')
            confirmed=$(echo $confirm_info | awk -F, '{print $4}')
        fi

        if [[ "$repaired" -gt "repair_start" ]] || [[ "$confirmed" -gt "$confirm_start" ]]; then
            break
        fi
        sleep 5
    done

    sleep 5
    second_choice=$((RANDOM % 3))
    if ! curl -X POST ${upstairs}/downstairs/fault/"$second_choice"; then
        echo "Failed to send fault request"
        exit 1
    fi

    # Now, wait for live_repair to finish.  Note that if the repair has
    # already finished that is okay.
    while :; do
        sleep 5
        $info_cmd > "$info_log"
        all_states=$(awk -F, '{print $2","$3","$4}' $info_log)
        ds0state=$(echo $all_states | awk -F\" '{print $4}')
        ds1state=$(echo $all_states | awk -F\" '{print $6}')
        ds2state=$(echo $all_states | awk -F\" '{print $8}')
	    if [[ $ds0state != "active" ]]; then
            continue
        fi
	    if [[ $ds1state != "active" ]]; then
            continue
        fi
	    if [[ $ds2state != "active" ]]; then
            continue
        fi
        # All states are active
        break
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
curl -s http://127.0.0.1:7777/info
echo ""

# How much repair did we do.
$info_cmd > "$info_log"
repair_info=$(awk -F, '{print $9","$10","$11}' $info_log | tr '[' ',' | tr ']' ',')
final_repair_zero=$(echo $repair_info | awk -F, '{print $2}')
final_repair_one=$(echo $repair_info | awk -F, '{print $3}')
final_repair_two=$(echo $repair_info | awk -F, '{print $4}')

confirm_info=$(awk -F, '{print $12","$13","$14}' $info_log | tr '[' ',' | tr ']' ',')
final_confirm_zero=$(echo $confirm_info | awk -F, '{print $2}')
final_confirm_one=$(echo $confirm_info | awk -F, '{print $3}')
final_confirm_two=$(echo $confirm_info | awk -F, '{print $4}')

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
