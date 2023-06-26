#!/bin/bash
#:
#: name = "test-region-create"
#: variety = "basic"
#: target = "helios"
#: output_rules = [
#:	"/tmp/region.csv",
#: ]
#: skip_clone = true
#:
#: [dependencies.rbuild]
#: job = "rbuild"

input="/input/rbuild/work"

set -o errexit
set -o pipefail
set -o xtrace
banner skipped
exit 0
echo "input rbins dir contains:"
ls -ltr "$input"/rbins || true

banner unpack
mkdir -p /var/tmp/bins
for t in "$input/rbins/"*.gz; do
	b=$(basename "$t")
	b=${b%.gz}
	gunzip < "$t" > "/var/tmp/bins/$b"
	chmod +x "/var/tmp/bins/$b"
done

export BINDIR=/var/tmp/bins

banner region
pfexec plimit -n 9123456 $$

ptime -m "$BINDIR/dsc" region-perf \
        --ds-bin "${BINDIR}/crucible-downstairs" \
        --csv-out /tmp/region.csv
