#!/bin/bash
set -o errexit
set -o pipefail

source ./args.sh

# re-upload + build crucible code and bench.sh for rerun
ansible-playbook -i inventory install_crucible.yml -e user="${user}" -e os="${OS}" -t rerun

# run upstairs benchmark, collect results
# warm up 3 times
for i in $(seq 1 3);
do
    ssh -o "StrictHostKeyChecking no" "${user}@$(terraform output -raw upstairs_ip)" \
        "cd /opt/crucible/ && /usr/bin/time -p ./bench.sh"
done

## 25 (or user configurable) real runs
rm results.txt
for i in $(seq 1 "${RUNS:-25}");
do
    ssh -o "StrictHostKeyChecking no" "${user}@$(terraform output -raw upstairs_ip)" \
        "cd /opt/crucible/ && /usr/bin/time -p ./bench.sh 2>&1" | tee -a results.txt
done

