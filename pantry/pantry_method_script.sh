#!/bin/bash

set -o errexit
set -o pipefail

DATALINK="$(svcprop -c -p config/datalink "${SMF_FMRI}")"
GATEWAY="$(svcprop -c -p config/gateway "${SMF_FMRI}")"
LISTEN_ADDR="$(svcprop -c -p config/listen_addr "${SMF_FMRI}")"
LISTEN_PORT="$(svcprop -c -p config/listen_port "${SMF_FMRI}")"

ipadm show-addr "$DATALINK/linklocal" || ipadm create-addr -t -T addrconf "$DATALINK/linklocal"
ipadm show-addr "$DATALINK/omicron6"  || ipadm create-addr -t -T static -a "$LISTEN_ADDR" "$DATALINK/omicron6"
route get -inet6 default -inet6 "$GATEWAY" || route add -inet6 default -inet6 "$GATEWAY"

args=(
  '-l' "[$LISTEN_ADDR]:$LISTEN_PORT"
)

exec /opt/oxide/crucible_pantry/bin/crucible-pantry run "${args[@]}"
