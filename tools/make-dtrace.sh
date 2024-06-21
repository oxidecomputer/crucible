#!/bin/bash
set -eux

rm -f out/crucible-dtrace.tar.gz 2> /dev/null

mkdir -p out

echo "$(date) Create DTrace archive on $(hostname)" > /tmp/dtrace-info.txt
echo "git log -1:" >> dtrace-info.txt
git log -1 >> dtrace-info.txt
echo "git status:" >> dtrace-info.txt
git status >> dtrace-info.txt

tar cavf out/crucible-dtrace.tar.gz \
  tools/dtrace/* \
  dtrace-info.txt

ls -l out/crucible-dtrace.tar.gz
rm dtrace-info.txt
