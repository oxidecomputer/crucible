#!/bin/bash
set -ex

# format data volume
apt update
apt install -y gdisk

if [[ -e /dev/nvme1n1 ]];
then
    sgdisk --zap /dev/nvme1n1
    sgdisk -n1 /dev/nvme1n1
    mkfs.ext4 /dev/nvme1n1p1
    mkdir /data
    mount /dev/nvme1n1p1 /data
    chown -R ubuntu /data
fi

# done
touch /var/booted_ok

