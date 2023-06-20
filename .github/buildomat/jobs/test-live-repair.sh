#!/bin/bash
#:
#: name = "test-live-repair"
#: variety = "basic"
#: target = "helios"
#: output_rules = [
#:	"/tmp/*.txt",
#:	"/tmp/*.log",
#: ]
#: skip_clone = true
#:
#: [dependencies.build]
#: job = "build"

input="/input/build/work"

set -o errexit
set -o pipefail
set -o xtrace

pfexec chmod +x "$input"/scripts/* || true

echo "input bins dir contains:"
ls -ltr "$input"/bins || true
echo "input script dir contains:"
ls -ltr "$input"/scripts || true

banner Unpack
mkdir -p /var/tmp/bins
for t in "$input/bins/"*.gz; do
	b=$(basename "$t")
	b=${b%.gz}
	gunzip < "$t" > "/var/tmp/bins/$b"
	chmod +x "/var/tmp/bins/$b"
done

export BINDIR=/var/tmp/bins

echo "BINDIR is $BINDIR"
echo "bindir contains:"
ls -ltr "$BINDIR" || true

banner CreateDS
echo $BINDIR/dsc create \
  --ds-bin "$BINDIR"/crucible-downstairs \
  --region-count 4 \
  --cleanup
$BINDIR/dsc create \
  --ds-bin "$BINDIR"/crucible-downstairs \
  --region-count 4 \
  --cleanup

banner StartDS
$BINDIR/dsc start \
  --ds-bin "$BINDIR"/crucible-downstairs \
  --region-count 4 >> /tmp/dsc.log 2>&1 &

# This gives dsc time to fail, as it is known to happen.  If we don't check,
# then the later test will just hang forever waiting for downstairs that
# will never show up.
sleep 5
dsc_pid=$(pgrep dsc);

if [[ "$dsc_pid" -eq 0 ]]; then
    echo "dsc_pid is zero, which is bad, exit"
    cat /tmp/dsc.log || true
    exit 1
fi

if ps -p "$dsc_pid"; then
    echo "Found dsc running, continue tests"
else
    echo "dsc failed"
    cat /tmp/dsc.log || true
    exit 1
fi

banner LR
ptime -m "$BINDIR"/crutest replace \
  -t 127.0.0.1:8810 -t 127.0.0.1:8820 -t 127.0.0.1:8830 \
  --replacement 127.0.0.1:8840 \
  -g 1 -c 10 -q > /tmp/crutest-replace.log

banner StopDSC
$BINDIR/dsc cmd shutdown

banner WaitStop
wait "$dsc_pid"

# Save the output files?
