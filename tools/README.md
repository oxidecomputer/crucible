# Oxide Crucible tools

Various scripts used for Crucible

## create-generic-sd.sh
A simple script to create three downstairs regions at var/88[1-3]0

## downstairs_daemon.sh
A highly custom script that starts three downstairs in a loop and will
keep them restarted when they are killed.  A bunch of assumptions are made
around where the region directory is and which ports the downstairs use.
You can pause the downstairs kill by creating the /tmp/ds_test/up file.
To stop the script all together, create the /tmp/ds_test/stop file.

After starting the downstairs, the user can hit enter and the script
will randomly kill (and then restart) a downstairs process.

If a downstairs dies for any other reason then being killed with the
generic default kill signal, the script will stop everything and leave
the logs behind in /tmp/ds_test/

## dtrace
A collection of dtrace scripts for use on Crucible.  A README.md in that
directory contains more information.

## hammer-loop.sh
A loop test that runs the crucible-hammer test in a loop.  It is expected
that you already have downstairs running on port 88[1-3]0.
The test will check for panic or assert in the output and stop if it
detects them or a test exits with an error.

## show_ox_stats.sh
A sample script that uses `oxdb` and `jq` to dump some oximeter stats
produced from running downstairs with the `--oximeter` option.  This script
is hard coded with a downstairs UUID and is intended to provide a sample to
build off of.

## test_ds.sh
Test import then export for crucible downstairs.

## test_perf.sh
A test that creates three downstairs regions of ~100G each and then runs
the crucible-client perf test using those regions.
A variety of extent size and extent counts are used (always the same total
region size of ~100G).

## test_reconnect.sh
A stress test of the reconnect code path.
Start up the "downstairs_daemon" script that will start three downstairs, then
in a loop kill and restart one at random.
Then, run in a loop the client "one" test which tries to start the upstairs
and do one IO, wait for the result, then exit.

## test_repair.sh
A test to break, then repair a downstairs region that is out of sync with
the other regions, in a loop

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
