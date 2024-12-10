#!/bin/bash

set -o pipefail

filename='/tmp/get-up-state.out'
final='/tmp/get-up-state.final'
rm -f $final

# Gather our output first.
dtrace -s /opt/oxide/crucible_dtrace/get-up-state.d | awk 'NF' > "$filename"
if [[ $? -ne 0 ]]; then
    exit 1
fi

# For each session we find, get the latest line and store that in
# the result file.
for id in $(cat $filename | grep -v SESSION | awk '{print $2}' | sort -n | uniq); do
    # Find our session, then print the final line
    grep "$id" "$filename" | tail -1 >> $final
done
# Print the header
grep "SESSION" "$filename"
# Sort our result by PID and print it out.
sort -n < $final
