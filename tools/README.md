# Oxide Crucible tools

Various scripts used for Crucible

## dtrace
A collection of dtrace scripts for use on Crucible.  A README.md in that
directory contains more information.

## hammer-loop.sh
A loop test that runs the crucible-hammer test in a loop.  It is expected
that you already have downstairs running on port 88[1-3]0.
The test will check for panic or assert in the output and stop if it
detects them or a test exits with an error.

## make-dtrace.sh
Build and package the DTrace scripts for use in the global zone of each sled.
The output of this script is published as an artifact by buildomat.

## make-nightly.sh
A simple script to build and package all that is required to run the
test_nightly.sh script.  Use this when you want to manually create and
run the nightly tests on a system.

## show_ox_propolis.sh
A sample script that uses `oxdb` and `jq` to dump some oximeter stats
produced from running propolis and requesting metrics. This requires
oximeter running and collecting stats from propolis.

## show_ox_stats.sh
A sample script that uses `oxdb` and `jq` to dump some oximeter stats
produced from running downstairs with the `--oximeter` option.  This script
is hard coded with a downstairs UUID and is intended to provide a sample to
build off of.

## show_ox_upstairs.sh
A sample script that uses `oxdb` and `jq` to dump some oximeter stats
produced from running the upstairs.  This script is hard coded with a
downstairs UUID and is intended to provide a sample to build off of.

## test_ds.sh
Test import then export for crucible downstairs.
Then, test the clone subcommand and verify that the cloned downstairs
exports the same file as the original downstairs.

## test_nightly.sh
This runs a selection of tests from this directory and reports their
results.  It is intended to be a test for Crucible that runs nightly
and does deeper/longer tests than what we do as part of every push.

## test_perf.sh
A test that creates three downstairs regions of ~100G each and then runs
the crutest perf test using those regions.
A variety of extent size and extent counts are used (always the same total
region size of ~100G).

## test_repair.sh
A test to break, then repair a downstairs region that is out of sync with
the other regions, in a loop

## test_replay.sh
A test that checks the replay code path, if a downstairs disconnects and
then reconnects, we replay jobs to it.  This is a thin wrapper around the
crutest replay test.  We use dsc to start and run the downstairs, then
subject the crutest upstairs to disconnecting downstairs.

## test_restart_repair.sh
Test the repair process while the downstairs are restarting, in a loop.

## test_up.sh
A simple script that will start three downstairs, then run through some tests in
client/src/main.  It's an easy way to quickly run some simple tests without
having to spin up a bunch of things.  These tests are limited in their scope and
should not be considered substantial.

Specify "unencrypted" or "encrypted" when running the script to test both code
paths.

That's all for now!
