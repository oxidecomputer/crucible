#!/bin/bash

D0=$(dig +short downstairs0.private.lan | tail -1)
D1=$(dig +short downstairs1.private.lan | tail -1)
D2=$(dig +short downstairs2.private.lan | tail -1)

set -e

#./target/release/crucible-hammer \
#  -t $D0:3801 -t $D1:3801 -t $D2:3801 \
#  --key "ukJBfV956H22EH5Qv4L0iKPWdtTYhdsdw1+eV5/6xdU=" --num-upstairs 1 >/dev/null

# downstairs uses 512b sectors
./target/release/measure-iops \
  -t $D0:3801 -t $D1:3801 -t $D2:3801 --samples 30 \
  --io-depth 8 --io-size-in-bytes $((512 * 5)) # --iop-limit 250

