#!/bin/bash

UUID=e2d8e8ad-17bf-4d03-b364-02fe8f41d064
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

echo "Showing upstairs stats for UUID: $UUID"
for stat in flush read write activated write_bytes read_bytes; do
    last_time=$($OXDB query crucible_upstairs:$stat upstairs_uuid=="$UUID" | jq '.[].measurements[].timestamp '| sort -n | tail -1)

    count=$($OXDB query crucible_upstairs:$stat upstairs_uuid=="$UUID" | jq ".[].measurements[] | select(.timestamp == $last_time) | .datum.datum.value")

    echo "$last_time count: $count for $stat"
done
