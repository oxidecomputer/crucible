#!/bin/bash
#:
#: name = "test-live-repair"
#: variety = "basic"
#: target = "helios-2.0"
#: output_rules = [
#:	"/tmp/*.txt",
#:	"/tmp/*.log",
#:	"/tmp/core.*",
#:	"/tmp/dsc/*.txt",
#:	"/tmp/dsc.tar",
#: ]
#: skip_clone = true
#:
#: [dependencies.build]
#: job = "build"

#
# If we fail, try to collect some debugging information
#
_exit_trap() {
    local status=$?
    [[ $status -eq 0 ]] && exit 0

    set +o errexit
    set -o xtrace
    sleep 5
    banner evidence

    CORES=$(ls /tmp/core*)
    for c in $CORES; do
        echo "Stack for Core file $c"
        pfexec pstack "$c"
    done

    tar cf /tmp/dsc.tar /var/tmp/dsc/region

    echo "Final region compare"
    $BINDIR/crucible-downstairs dump \
        -d /var/tmp/dsc/region/8810 \
        -d /var/tmp/dsc/region/8820 \
        -d /var/tmp/dsc/region/8830

    exit $status
}

trap _exit_trap EXIT

input="/input/build/work"

set -o errexit
set -o pipefail
set -o xtrace

banner cores
pfexec coreadm -i /tmp/core.%f.%p \
 -g /tmp/core.%f.%p \
 -e global \
 -e log \
 -e proc-setid \
 -e global-setid

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
  --extent-size 4000 \
  --extent-count 200 \
  --region-count 4 \
  --cleanup
$BINDIR/dsc create \
  --ds-bin "$BINDIR"/crucible-downstairs \
  --extent-size 4000 \
  --extent-count 200 \
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

banner dtrace
# Start up a dtrace script to record upstairs activity.
ls -l $input/scripts
pfexec dtrace -Z -s $input/scripts/upstairs-info.d > /tmp/upstairs_info.txt 2>&1 &

banner why
ps -ef | grep dtrace
ls -l /tmp

banner LR
ptime -m "$BINDIR"/crutest replace \
  -t 127.0.0.1:8810 -t 127.0.0.1:8820 -t 127.0.0.1:8830 \
  --replacement 127.0.0.1:8840 \
  -g 1 -c 10 --stable > /tmp/crutest-replace.log 2>&1

banner StopDSC
$BINDIR/dsc cmd shutdown

banner WaitStop
wait "$dsc_pid"

# Save the output files?
