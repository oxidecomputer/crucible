# Oxide Crucible tools

Various scripts used for Crucible

## create-generic-sd.sh
A simple script to create three downstairs regions at var/380[1-3]

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

## ds_state.d
This is a dtrace script for printing some crude upstairs state info.
If the upstairs is not yet running, add the -Z flag to dtrace so it will
wait to find the matching probe.
```
pfexec dtrace -s tools/ds_state.d
```

You start crucible, then run the above script.  Output should start appearing
right away with the state of the three downstairs and a count of active
jobs for upstairs and downstairs.

Here is an example of how it might look:
```
alan@cat:crucible$ pfexec dtrace -s tools/ds_state.d
["Active","Active","Active"] Upstairs:   1 Downstairs:   3
["Active","Active","Active"] Upstairs:   1 Downstairs:   3
["Active","Active","Active"] Upstairs:   1 Downstairs:   6
["Active","Active","Active"] Upstairs:   1 Downstairs:   6
```

## hammer-loop.sh
A loop test that runs the crucible-hammer test in a loop.  It is expected
that you already have downstairs running on port 380[1-3].
The test will check for panic or assert in the output and stop if it
detects them or a test exits with an error.

## tracegw.d
This is a dtrace example script for counting IOs into and out of
crucible from the guest.
If the upstairs is not yet running, add the -Z flag to dtrace so it will
wait to find the matching probe.
```
sudo dtrace -s tools/tracegw.d
```

When you are ready to see results, hit Control-C and the final counts will
be printed.

Here is an example of how it might look:
```
final:crucible alan$ sudo dtrace -Z -s tools/tracegw.d
dtrace: system integrity protection is on, some features will not be available

^C
 read_start:1000    read_end:1000
 write_start:1000   write_end:1000
 flush_start:1000   flush_end:1000
```

## tracegw.d
This is a simple dtrace script that measures latency times for when a r/w/f
job is submitted to the internal upstairs work queue, to when that job has
completed and the notification was sent back to the guest.
If the upstairs is not yet running, add the -Z flag to dtrace so it will
wait to find the matching probe.
```
sudo dtrace -s tools/perfgw.d
```

Here is an example of how it might look:
```
$ sudo dtrace -s tools/perfgw.d
dtrace: system integrity protection is on, some features will not be available

dtrace: script 'tools/perfgw.d' matched 6 probes
^C

  write
           value  ------------- Distribution ------------- count
        16777216 |                                         0
        33554432 |@@@@@@@@@@@@@@                           355
        67108864 |@@@@@@@@@@@@@@@@@@@@@@@@@@               645
       134217728 |                                         0

  read
           value  ------------- Distribution ------------- count
        16777216 |                                         0
        33554432 |@@@@@@@@@@@@@@                           353
        67108864 |@@@@@@@@@@@@@@@@@@@@@@@@@@               647
       134217728 |                                         0

  flush
           value  ------------- Distribution ------------- count
          524288 |                                         0
         1048576 |                                         1
         2097152 |                                         0
         4194304 |                                         0
         8388608 |                                         0
        16777216 |                                         0
        33554432 |@@@@@@@@@@@@@@                           353
        67108864 |@@@@@@@@@@@@@@@@@@@@@@@@@@               647
       134217728 |
```

## test_reconnect.sh
A stress test of the reconnect code path.
Start up the "downstairs_daemon" script that will start three downstairs, then
in a loop kill and restart one at random.
Then, run in a loop the client "one" test which tries to start the upstairs
and do one IO, wait for the result, then exit.

## test_up.sh
A simple script that will start three downstairs, then run through some tests in
client/src/main.  It's an easy way to quickly run some simple tests without
having to spin up a bunch of things.  These tests are limited in their scope and
should not be considered substantial.

That's all for now!
