#!/bin/bash
set -o errexit
set -o pipefail

source <(cat ./args.sh);

if [[ ! -e "${HOME}/.ssh/id_ed25519.pub" ]];
then
    echo "terraform code expects ${HOME}/.ssh/id_ed25519.pub to exist, please create: ssh-keygen -t ed25519 ..."
    exit 1
fi

# make sure the chosen instance type is available in at least 3 azs,
# otherwise terraform will fail
azs=$(aws ec2 describe-instance-type-offerings \
    --location-type availability-zone \
    --filters "Name=instance-type,Values=${instance_type}" \
    --region "${REGION}" \
    --output text | wc -l)

if [[ ${azs} -lt 3 ]];
then
    echo "instance ${instance_type} only available in ${azs} azs!"
    exit 1
fi

set -o xtrace

# bring up aws resources
terraform init
terraform apply -auto-approve \
    -var "ami_id=${ami_id}" -var "region=${REGION}" \
    -var "instance_type=${instance_type}" \
    -var "user_data_path=${user_data_path}" >/dev/null

# create ansible inventory from terraform outputs
./inv.sh

# install ansible into python virtualenv
if [[ ! -e .venv/bin/activate ]];
then
    virtualenv -p python3 .venv
fi

source .venv/bin/activate

if ! pip show ansible;
then
    pip install "ansible==5.0.1"
fi

# wait for instance status ok (status checks ok, user data has run)
INSTANCE_ID_0=$(terraform output -raw upstairs_id)
INSTANCE_ID_1=$(terraform output -json downstairs_ids 2>&1 | jq -r .[0])
INSTANCE_ID_2=$(terraform output -json downstairs_ids 2>&1 | jq -r .[1])
INSTANCE_ID_3=$(terraform output -json downstairs_ids 2>&1 | jq -r .[2])

aws ec2 wait instance-status-ok --region "${REGION}" --instance-ids \
    "$INSTANCE_ID_0" "$INSTANCE_ID_1" "$INSTANCE_ID_2" "$INSTANCE_ID_3"

# prepare instances - install crucible, run downstairs, etc
ansible-playbook -i inventory install_crucible.yml -e user="${user}" -e os="${OS}"

