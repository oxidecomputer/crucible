#!/bin/bash

# This script will display the downstairs live repair for each
# pid/session it finds running on a system.
filename='/tmp/get-lr-state.out'

# Gather state on all running propolis servers, record summary to a file
dtrace -s /opt/oxide/crucible_dtrace/get-lr-state.d | sort -n | uniq | awk 'NF' > "$filename"
# Walk the lines in the file, append the zone name to each line.
while read -r p; do
        # For each line in the file, pull out the PID we are looking at and
        # print the zone that process is running in.
        pid=$(echo $p | awk '{print $1}')
        zone=$(ps -o zone -p $pid | tail -1 | cut -c 1-28)
        echo "$zone $p"
done < "$filename"
