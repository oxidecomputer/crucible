#!/bin/bash
set -o errexit
set -o pipefail

source <(cat ./args.sh);

ansible-playbook -i inventory cleanup.yml -e user="${user}" -e os="${OS}" || true

terraform apply -destroy -auto-approve \
    -var "ami_id=${ami_id}" -var "region=${REGION}" \
    -var "instance_type=${instance_type}" \
    -var "user_data_path=${user_data_path}" >/dev/null

