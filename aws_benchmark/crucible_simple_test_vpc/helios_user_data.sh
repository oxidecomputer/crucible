#!/bin/bash
export PATH="/usr/bin/:/sbin/:/usr/sbin/:$PATH"

zfs create -o mountpoint=/home/helios 'rpool/helios'
# XGECOS=$(getent passwd helios | cut -d: -f5) == helios,,,
useradd -u '1000' -g staff -c 'helios,,,' -d '/home/helios' -P 'Primary Administrator' -s /bin/bash 'helios'
passwd -N 'helios'
mkdir '/home/helios/.ssh'
cp /root/.ssh/authorized_keys /home/helios/.ssh/authorized_keys
chown -R 'helios:staff' '/home/helios'
chmod 0700 '/home/helios'
sed -i -e '/^PATH=/s#\$#:/opt/ooce/bin:/opt/ooce/sbin#' /etc/default/login
ntpdig -S 0.pool.ntp.org || true

# passwordless sudo for ansible
echo 'helios ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/helios

cd /home/helios

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > rustup.sh
chmod u+x rustup.sh
sed -i -e 's_/bin/sh_/bin/bash_g' rustup.sh
sed -i -e '/shellcheck shell=dash/d' rustup.sh
chown helios rustup.sh
sudo -u helios ./rustup.sh -y

pkg install htop
pkg install rsync

touch /var/booted_ok

