#!/bin/bash

if [[ -n $1 ]]; then
    UUID=$1
else
    echo "Please provide the Crucible Upstairs UUID"
    exit 1
fi

# Show me the stats for $UUID
# All sorts of assumptions here.  Make it better if you so desire.

if which oxdb > /dev/null; then
    OXDB=$(which oxdb)
elif [[ -f ../omicron/target/debug/oxdb ]]; then
    OXDB="../omicron/target/debug/oxdb"
elif [[ -f ../omicron/target/release/oxdb ]]; then
    OXDB="../omicron/target/release/oxdb"
else
    echo "Can't find oxdb"
    exit 1
fi

echo "Showing upstairs stats for UUID: $UUID"
for stat in activated flush read read_bytes write write_bytes ; do
    last_time=$($OXDB -a fd00:1122:3344:101::5 query crucible_upstairs:$stat upstairs_uuid=="$UUID" | jq '.[].measurements[].timestamp '| sort -n | tail -1)
    if [[ -z "$last_time" ]]; then
        echo "Error finding last timestamp for $stat"
        continue
    fi

    count=$($OXDB -a fd00:1122:3344:101::5 query crucible_upstairs:$stat upstairs_uuid=="$UUID" | jq ".[].measurements[] | select(.timestamp == $last_time) | .datum.datum.value")
    if [[ -z "$count" ]]; then
        echo "Error finding count value for $stat"
        continue
    fi

    last_time=$(echo $last_time | awk -F\. '{print $1}' | tr 'T' ' ' | tr -d '"')
    echo "$last_time  count: $count for $stat"
done
