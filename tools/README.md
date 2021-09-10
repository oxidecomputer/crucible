# Oxide Crucible tools

Various scripts used for Crucible

## tracegw.d
This is a dtrace example script for counting IOs into and out of
crucible from the guest.  The way this works requires that crucible is
running before you run this, otherwise it won't find the probes.
```
sudo dtrace -s tools/tracegw.d
```

You start crucible, then run the above script.  When you are ready to see
results, hit Control-C and the final counts will be printed.

Here is an example of how it might look:
```
final:crucible alan$ sudo dtrace -s tools/tracegw.d
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

## test_up.sh
A simple script that will start three downstairs, then run through some tests in
client/src/main.  It's an easy way to quickly run some simple tests without
having to spin up a bunch of things.  These tests are limited in their scope and
should not be considered substantial.

That's all for now!
