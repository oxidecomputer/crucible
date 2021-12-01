#!/bin/bash

# Show me the stats for a hard coded downstairs UUID
# All sorts of assumptions here.  Make it better if you so desire.
if [[ -f ../omicron/target/debug/oxdb ]]; then
    OXDB="../omicron/target/debug/oxdb"
elif [[ -f ../omicron/target/release/oxdb ]]; then
    OXDB="../omicron/target/release/oxdb"
else
    echo "Can't find oxdb"
    exit 1
fi

for stat in flush read write connect; do
    last_time=$($OXDB query crucible_downstairs:$stat downstairs_uuid=12345678-3801-3801-3801-000000003801 | jq '.[].measurements[].timestamp '| sort -n | tail -1)

    flush_count=$($OXDB query crucible_downstairs:$stat downstairs_uuid=12345678-3801-3801-3801-000000003801 | jq ".[].measurements[] | select(.timestamp == $last_time) | .datum.CumulativeI64.value")

    echo "$last_time count: $flush_count for $stat"
done
