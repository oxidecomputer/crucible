#!/bin/bash

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

OS="${1}"
REGION="${2}"

# Prevent re-entry from hitting AWS api again
if [[ -n "${user}" ]];
then
    return
fi

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

        export instance_type="m5d.2xlarge"
        export user_data_path="ubuntu_user_data.sh"
        export user="ubuntu"
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
        export instance_type="m4.2xlarge"
        export user_data_path="helios_user_data.sh"
        export user="helios"
        ;;

    *)
        echo "for OS, choose ubuntu or helios!"
        exit 1
        ;;
esac

