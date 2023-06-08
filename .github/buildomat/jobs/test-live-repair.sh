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

banner unpack
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

banner StartDSC
echo $BINDIR/dsc create --downstairs-bin "$BINDIR"/crucible-downstairs --cleanup
$BINDIR/dsc create --downstairs-bin "$BINDIR"/crucible-downstairs --cleanup
echo "dsc try 1 done"
sleep 1
echo "Try again"

$BINDIR/dsc start --downstairs-bin "$BINDIR"/crucible-downstairs --create --cleanup > /tmp/dsc.log 2>&1 &
dsc_pid=$?

sleep 5
ls -l $BINDIR/dsc


echo dsc_pid is $dsc_pid
ls -l /tmp
cat /tmp/dsc.log

if ps -p $dsc_pid; then
    echo "Found dsc"
else
    echo "dsc failed"
    exit 1
fi

banner LR
ptime -m "$BINDIR"/crutest replay \
  -t 127.0.0.1:8810 -t 127.0.0.1:8820 -t 127.0.0.1:8830 \
  -g 1 -c 40 -q | tee /tmp/crutest-replay.log

banner StopDSC
$input/bins/dsc cmd shutdown
wait "$dsc_pid"

# Save the output files?
