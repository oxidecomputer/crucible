#!/bin/bash
set -eux

cargo build --release --all-features

rm -f out/crucible-nightly.tar.gz 2> /dev/null

mkdir -p out

echo "$(date) Create nightly archive on $(hostname)" > /tmp/nightly-info.txt
echo "git log -1:" >> nightly-info.txt
git log -1 >> nightly-info.txt
echo "git status:" >> nightly-info.txt
git status >> nightly-info.txt

tar cavf out/crucible-nightly.tar.gz \
  target/release/crutest \
  target/release/crucible-downstairs \
  target/release/crucible-hammer \
  target/release/dsc \
  tools/hammer_loop.sh \
  tools/test_perf.sh \
  tools/test_reconnect.sh \
  tools/test_repair.sh \
  tools/test_restart_repair.sh \
  tools/test_nightly.sh \
  nightly-info.txt

ls -l out/crucible-nightly.tar.gz
rm nightly-info.txt
