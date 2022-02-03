#!/bin/bash
set -o errexit
set -o pipefail

source ./args.sh

cleanup() {
    ./cleanup.sh "${OS}" "${REGION}"
}

# with errexit, cleanup
trap 'cleanup' ERR

# check pre-reqs
function program_required() {
    if ! type "${1}";
    then
        echo "please install ${1}";
        exit 1
    fi
}

program_required terraform
program_required virtualenv
program_required python3
program_required pip

# bring up resources, run the benchmark, and clean up
./bring_up_resources.sh "${OS}" "${REGION}"
./run_benchmark.sh "${OS}" "${REGION}"
cleanup

# show results
set -x

# was job based flow control triggered?
grep "flow control" results.txt || true

# show IOPS measurements
grep "IOPS mean " results.txt || true

