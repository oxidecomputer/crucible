#!/bin/bash

UUID=1ade0b6e-9330-472b-a0c9-c60f1b9ab815
# Show me the stats for a hard coded upstairs UUID
# All sorts of assumptions here.  Make it better if you so desire.
if [[ -f ../omicron/target/debug/oxdb ]]; then
    OXDB="../omicron/target/debug/oxdb"
elif [[ -f ../omicron/target/release/oxdb ]]; then
    OXDB="../omicron/target/release/oxdb"
else
    echo "Can't find oxdb"
    exit 1
fi

for stat in flush read write activated write_bytes read_bytes; do
    last_time=$($OXDB query crucible_upstairs:$stat upstairs_uuid=="$UUID" | jq '.[].measurements[].timestamp '| sort -n | tail -1)

    count=$($OXDB query crucible_upstairs:$stat upstairs_uuid=="$UUID" | jq ".[].measurements[] | select(.timestamp == $last_time) | .datum.datum.value")

    echo "$last_time count: $count for $stat"
done
