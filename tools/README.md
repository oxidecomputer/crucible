# Oxide Crucible tools

Various scripts used for Crucible

## dtrace directory
A collection of dtrace scripts for use on Crucible.  A README.md in that
directory contains more information.

## Notes on tests to be run by the nightly framework.
Tests that run under the nightly collection should respect the meaning of
the following variables, and make use of them.
WORK_ROOT    The top level path inside which test logs and result files will be.
ROOT         The top level path for the crucible repo.
BINDIR       The location from ROOT where we should expect various crucible
             binaries to be found.
REGION_SETS  The number of downstairs region sets (groups of three downstairs)
             that a test will create, if the test supports it.
REGION_ROOT  Top level directory where Downstairs regions will be created.

Specific to each test, a test should assign these for themselves:
TEST_ROOT       A specific subdirectory inside WORK_ROOT where all the
                tests logs and result files will be.  This directory will be
                re-created on each run and any older directories with the same
                name will be destroyed.
MY_REGION_ROOT  Specific subdirectory inside REGION_ROOT where this specific
                tests region directories will be created.  This directory
                will be destroyed and re-created on test startup.

Having all these global variables allows a higher level framework to
supply locations for test artifacts as well as clean up or archive results
for tests in the event of failures.  Tests themselves should clean out or
handle a previous runs results if they exist when a test starts.

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

## test_repair.sh
A test to break, then repair a downstairs region that is out of sync with
the other regions, in a loop

## test_replace_special.sh
A test to verify that we can replace a downstairs while reconciliation is
underway.

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
