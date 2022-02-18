#!/bin/bash

set -o errexit
set -o pipefail

args=(
        '--data' "$(svcprop -c -p config/directory "${SMF_FMRI}")"
        '--port' "$(svcprop -c -p config/port "${SMF_FMRI}")"
        '--mode' "$(svcprop -c -p config/mode "${SMF_FMRI}")"
)

val="$(svcprop -c -p config/cert_pem_path "${SMF_FMRI}")"
if [[ -n "$val" ]]; then
        args+=( '--cert-pem' )
        args+=( "$val" )
fi

val="$(svcprop -c -p config/key_pem_path "${SMF_FMRI}")"
if [[ -n "$val" ]]; then
        args+=( '--key-pem' )
        args+=( "$val" )
fi

val="$(svcprop -c -p config/root_pem_path "${SMF_FMRI}")"
if [[ -n "$val" ]]; then
        args+=( '--root-cert-pem' )
        args+=( "$val" )
fi

exec /opt/oxide/crucible/bin/downstairs run "${args[@]}"
