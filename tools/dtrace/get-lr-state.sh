#!/bin/bash

# This script will display the downstairs live repair for each
# pid/session it finds running on a system.
filename='/tmp/get-lr-state.out'

# Clear out any previous state
echo "" > "$filename"
# Gather state on all running propolis servers, record summary to a file
dtrace -s /opt/oxide/dtrace/crucible/get-lr-state.d | sort -n | uniq | awk 'NF' > "$filename"
# Walk the lines in the file, append the zone name to each line.
while read -r p; do
        # For each line in the file, pull out the PID we are looking at and
        # print the zone that process is running in.
        pid=$(echo $p | awk '{print $1}')
        zone=$(ps -o zone -p $pid | tail -1 | cut -c 1-28)
        # Our zone string size is already set from above, force the
        # rest of the line to take up 26 columns, this prevents PIDs
        # with fewer than 5 digits from using less columns.
        printf "%s %26s\n" "$zone" "$p"
done < "$filename"
