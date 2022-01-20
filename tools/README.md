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

## show_ox_stats.sh
A sample script that uses `oxdb` and `jq` to dump some oximeter stats
produced from running downstairs with the `--oximeter` option.  This script
is hard coded with a downstairs UUID and is intended to provide a sample to
build off of.

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

## perfdw.d
This is a simple dtrace script that measures latency times for when a r/w/f
job is sent over the network to each downstairs to when the ACK for that job
is returned to the upstairs. Jobs are sorted by type (r/w/f) and by each
downstairs client ID.
```
sudo dtrace -s tools/perfdw.d
```

Here is an example of how it might look:
```
final:crucible alan$ sudo sudo dtrace -Z -s tools/perfdw.d
Password:
dtrace: system integrity protection is on, some features will not be available

dtrace: script 'tools/perfdw.d' matched 0 probes
^C

  flush for downstairs client                                       1
           value  ------------- Distribution ------------- count
          262144 |                                         0
          524288 |@                                        19
         1048576 |@@@@@                                    134
         2097152 |@@@@@@@@@@@@                             317
         4194304 |@@@@@@@@@@@@@@@@@@@@                     502
         8388608 |@@                                       46
        16777216 |                                         5
        33554432 |                                         2
        67108864 |                                         0

  flush for downstairs client                                       2
           value  ------------- Distribution ------------- count
          262144 |                                         0
          524288 |@                                        15
         1048576 |@@@@@                                    132
         2097152 |@@@@@@@@@@@@                             316
         4194304 |@@@@@@@@@@@@@@@@@@@@                     505
         8388608 |@@                                       48
        16777216 |                                         7
        33554432 |                                         2
        67108864 |                                         0

  flush for downstairs client                                       0
           value  ------------- Distribution ------------- count
          262144 |                                         0
          524288 |@                                        16
         1048576 |@@@@@                                    136
         2097152 |@@@@@@@@@@@@                             309
         4194304 |@@@@@@@@@@@@@@@@@@@@                     505
         8388608 |@@                                       50
        16777216 |                                         6
        33554432 |                                         3
        67108864 |                                         0

  write for downstairs client                                       1
           value  ------------- Distribution ------------- count
          262144 |                                         0
          524288 |@                                        16
         1048576 |@                                        37
         2097152 |@@                                       62
         4194304 |@@@@@@                                   154
         8388608 |@@@@@@@@@@                               243
        16777216 |@@@@@@@@@@@@@@@@@@                       456
        33554432 |@                                        31
        67108864 |                                         1
       134217728 |                                         0

  write for downstairs client                                       0
           value  ------------- Distribution ------------- count
          262144 |                                         0
          524288 |@                                        16
         1048576 |@                                        35
         2097152 |@@@                                      64
         4194304 |@@@@@@                                   152
         8388608 |@@@@@@@@@@                               241
        16777216 |@@@@@@@@@@@@@@@@@@                       458
        33554432 |@                                        33
        67108864 |                                         1
       134217728 |                                         0

  write for downstairs client                                       2
           value  ------------- Distribution ------------- count
          262144 |                                         0
          524288 |@                                        14
         1048576 |@@                                       39
         2097152 |@@                                       60
         4194304 |@@@@@@                                   153
         8388608 |@@@@@@@@@@                               239
        16777216 |@@@@@@@@@@@@@@@@@@                       462
        33554432 |@                                        31
        67108864 |                                         2
       134217728 |                                         0

  read for downstairs client                                        0
           value  ------------- Distribution ------------- count
          131072 |                                         0
          262144 |@@@                                      205
          524288 |@@@@                                     284
         1048576 |@@@@                                     274
         2097152 |@@@@@@                                   440
         4194304 |@@@@@@@@@                                646
         8388608 |@@@@@@@@@                                655
        16777216 |@@@@@@                                   448
        33554432 |@                                        48
        67108864 |                                         0

  read for downstairs client                                        2
           value  ------------- Distribution ------------- count
          131072 |                                         0
          262144 |@@@                                      204
          524288 |@@@@                                     283
         1048576 |@@@@                                     271
         2097152 |@@@@@@                                   442
         4194304 |@@@@@@@@@                                647
         8388608 |@@@@@@@@@                                658
        16777216 |@@@@@@                                   447
        33554432 |@                                        46
        67108864 |                                         0
       134217728 |                                         2
       268435456 |                                         0

  read for downstairs client                                        1
           value  ------------- Distribution ------------- count
          131072 |                                         0
          262144 |@@@                                      218
          524288 |@@@@                                     284
         1048576 |@@@@                                     267
         2097152 |@@@@@                                    399
         4194304 |@@@@@@@@                                 610
         8388608 |@@@@@@@@@@                               714
        16777216 |@@@@@@                                   461
        33554432 |@                                        46
        67108864 |                                         1
       134217728 |                                         0

```

## perfgw.d
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

Specify "unencrypted" or "encrypted" when running the script to test both code
paths.

That's all for now!
