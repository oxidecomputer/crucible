#!/bin/bash
set -o errexit
set -o pipefail

if [[ ${#} -lt 2 ]];
then
    echo "usage: <OS> <REGION>"
    exit 1
fi

case "${1}" in
    "ubuntu")
        # ok
        ;;
    "helios")
        # ok
        ;;
    *)
        echo "for OS, choose ubuntu or helios!"
        exit 1
        ;;
esac

if [[ ! -e "${HOME}/.ssh/id_ed25519.pub" ]];
then
    echo "terraform code expects ${HOME}/.ssh/id_ed25519.pub to exist, please create: ssh-keygen -t ed25519 ..."
    exit 1
fi

OS="${1}"
REGION="${2}"

cleanup() {
    ansible-playbook -i inventory cleanup.yml -e user="${user}" -e os="${OS}" || true

    terraform apply -destroy -auto-approve \
        -var "ami_id=${ami_id}" -var "region=${REGION}" \
        -var "instance_type=${instance_type}" \
        -var "user_data_path=${user_data_path}" >/dev/null
}

# with errexit, cleanup
trap 'cleanup' ERR

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

# set AMI, instance type, user data path
case "${OS}" in
    "ubuntu")
        # get the latest ami that matches the filter
        ami_id=$(aws ec2 describe-images --region "${REGION}" --owner 099720109477 \
                 --filter "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-*-21.04-amd64-server-*" \
                 --query 'Images[*].[ImageId,CreationDate]' --output text \
                 | sort -k2 \
                 | tail -n1 | awk '{ print $1 }')
        echo "ubuntu ami: ${ami_id} $(aws ec2 describe-images --region "${REGION}" --image-id "${ami_id}" --query 'Images[*].[Name]' --output text)"

        instance_type="m5d.2xlarge"
        user_data_path="ubuntu_user_data.sh"
        user="ubuntu"
        ;;

    "helios")
        # get the latest ami that matches the filter
        ami_id=$(aws ec2 describe-images --region "${REGION}" --owner 128433874814 \
                 --filter "Name=name,Values=helios-full-*-*" \
                 --query 'Images[*].[ImageId,CreationDate]' --output text \
                 | sort -k2 \
                 | tail -n1 | awk '{ print $1 }')
        echo "helios ami: ${ami_id} $(aws ec2 describe-images --region "${REGION}" --image-id "${ami_id}" --query 'Images[*].[Name]' --output text)"

        # TODO: rpz's ena patch for m5d? need an updated helios-full-* image.
        # need: 6f443ebc1fb4fec01d6e8fa8ca4648182ed215bb, so helios version at least 20793
        instance_type="m4.2xlarge"
        user_data_path="helios_user_data.sh"
        user="helios"
        ;;

    *)
        echo "for OS, choose ubuntu or helios!"
        exit 1
        ;;
esac

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

# run upstairs benchmark, collect results
## warm up 3 times
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

# done! clean up
cleanup

cat results.txt

