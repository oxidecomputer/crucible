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
  target/release/cmon \
  target/release/crudd \
  target/release/crutest \
  target/release/crucible-agent \
  target/release/crucible-downstairs \
  target/release/crucible-hammer \
  target/release/dsc \
  tools/crudd-speed-battery.sh \
  tools/dtrace/* \
  tools/hammer_loop.sh \
  tools/test_live_repair.sh \
  tools/test_fail_live_repair.sh \
  tools/test_mem.sh \
  tools/test_perf.sh \
  tools/test_replay.sh \
  tools/test_repair.sh \
  tools/test_replace_special.sh \
  tools/test_restart_repair.sh \
  tools/test_nightly.sh \
  tools/loop-repair.sh \
  tools/loop-double-repair.sh \
  nightly-info.txt

ls -l out/crucible-nightly.tar.gz
rm nightly-info.txt
