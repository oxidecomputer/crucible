#!/bin/bash
set -eux

rm -f out/crucible-dtrace.tar.gz 2> /dev/null

mkdir -p out

echo "$(date) Create DTrace archive on $(hostname)" > /tmp/dtrace-info.txt
echo "git log -1:" >> dtrace-info.txt
git log -1 >> dtrace-info.txt
echo "git status:" >> dtrace-info.txt
git status >> dtrace-info.txt
mv dtrace-info.txt tools/dtrace

pushd tools/dtrace
tar cvf ../../out/crucible-dtrace.tar \
  *

rm dtrace-info.txt
popd
ls -l out/crucible-dtrace.tar
