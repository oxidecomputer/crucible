#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

. /lib/svc/share/smf_include.sh

DATALINK="$(svcprop -c -p config/datalink "${SMF_FMRI}")"
GATEWAY="$(svcprop -c -p config/gateway "${SMF_FMRI}")"
DATASET="$(svcprop -c -p config/dataset "${SMF_FMRI}")"
LISTEN_ADDR="$(svcprop -c -p config/listen_addr "${SMF_FMRI}")"
LISTEN_PORT="$(svcprop -c -p config/listen_port "${SMF_FMRI}")"
PORTBASE="$(svcprop -c -p config/portbase "${SMF_FMRI}")"
DOWNSTAIRS_PREFIX="$(svcprop -c -p config/downstairs_prefix "${SMF_FMRI}")"
SNAPSHOT_PREFIX="$(svcprop -c -p config/snapshot_prefix "${SMF_FMRI}")"

if [[ $DATALINK == unknown ]] || [[ $GATEWAY == unknown ]]; then
    printf 'ERROR: missing datalink or gateway' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

ipadm show-addr "$DATALINK/ll" || ipadm create-addr -t -T addrconf "$DATALINK/ll"
ipadm show-addr "$DATALINK/omicron6"  || ipadm create-addr -t -T static -a "$LISTEN_ADDR" "$DATALINK/omicron6"
route get -inet6 default -inet6 "$GATEWAY" || route add -inet6 default -inet6 "$GATEWAY"

args=(
  '-D' '/opt/oxide/crucible/bin/crucible-downstairs'
  '--dataset' "$DATASET"
  '-l' "[$LISTEN_ADDR]:$LISTEN_PORT"
  '-P' "$PORTBASE"
  '-p' "$DOWNSTAIRS_PREFIX"
  '-s' "$SNAPSHOT_PREFIX"
)

exec /opt/oxide/crucible/bin/crucible-agent run "${args[@]}"
