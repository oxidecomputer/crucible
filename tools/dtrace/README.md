# Oxide DTrace Crucible scripts

## perf-upstairs-rw.d
A DTrace script to track writes and flushes through the upstairs.
The shows the time in three parts:
1: From IO received in the upstairs to IO being submitted to the
queue of work for the three downstairs.
2: From IO on downstairs queue, to enough downstairs completing the
IO that it is ready to ack.
3: From the IO being ready to ack, to that ack being sent.

## perfdw.d
This is a simple dtrace script that measures latency times for when a r/w/f
job is sent over the network to each downstairs to when the ACK for that job
is returned to the upstairs. Jobs are sorted by type (r/w/f) and by each
downstairs client ID.
```
sudo dtrace -s perfdw.d
```

Here is an example of how it might look:
```
final:crucible alan$ sudo sudo dtrace -Z -s perfdw.d
Password:
dtrace: system integrity protection is on, some features will not be available

dtrace: script 'perfdw.d' matched 0 probes
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
sudo dtrace -s perfgw.d
```

Here is an example of how it might look:
```
$ sudo dtrace -s perfgw.d
dtrace: system integrity protection is on, some features will not be available

dtrace: script 'perfgw.d' matched 6 probes
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

## upstairs_info.d
This is a dtrace script for printing some simple upstairs state info.
If the upstairs is not yet running, add the -Z flag to dtrace so it will
wait to find the matching probe.
```
pfexec dtrace -s upstairs_info.d
```

You start crucible, then run the above script.  Output should start appearing
right away with the state of the three downstairs and a count of active
jobs for upstairs and downstairs.

Here is an example of how it might look:
```
alan@cat:crucible$ pfexec dtrace -s upstairs_info.d
["Active","Active","Active"] Upstairs:   1 Downstairs:   3
["Active","Active","Active"] Upstairs:   1 Downstairs:   3
["Active","Active","Active"] Upstairs:   1 Downstairs:   6
["Active","Active","Active"] Upstairs:   1 Downstairs:   6
```

## tracegw.d
This is a dtrace example script for counting IOs into and out of
crucible from the guest.
If the upstairs is not yet running, add the -Z flag to dtrace so it will
wait to find the matching probe.
```
sudo dtrace -s tracegw.d
```

When you are ready to see results, hit Control-C and the final counts will
be printed.

Here is an example of how it might look:
```
final:crucible alan$ sudo dtrace -Z -s tracegw.d
dtrace: system integrity protection is on, some features will not be available

^C
 read_start:1000    read_end:1000
 write_start:1000   write_end:1000
 flush_start:1000   flush_end:1000
```
