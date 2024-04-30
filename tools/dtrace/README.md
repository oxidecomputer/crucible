# Oxide DTrace Crucible scripts

## downstairs_count.d
A DTrace script to show IOs coming and going on a downstairs as well as the
work task geting new work, performing the work and completing the work.  This
script takes the PID of a downstairs process to track and prints IO counts
and task work counts at a 4 second interval.

The columns show counts in the last 4 seconds of:
F>  Flush coming in from the upstairs
F<  Flush completed message being sent back to the upstairs.
W>  Write coming in from the upstairs
W<  Write completed message being sent back to the upstairs.
R>  Read coming in from the upstairs
R<  Read completed message being sent back to the upstairs.
WS  An IO has been submitted to the work task in the downstairs
WIP An IO is taken off the work queue by the downstairs work task.
WD  An IO is completed by the downstairs work task.

Here is some sample output:
```
alan@atrium:dsdrop$ pfexec dtrace -Z -s ./tools/dtrace/downstairs_count.d 56784
  F>   F<   W>   W<    R>    R<    WS   WIP    WD
  47   47  135  135   204   204   386   386   386
  39   39  130  131   193   193   362   362   363
  42   41  141  141   176   176   359   359   358
  40   40  120  120   155   155   315   315   315
   6    6   11   11    22    22    39    39    39
   4    4    8    8    10    10    22    22    22
   4    4   10   10     6     6    20    20    20
   5    5   10   10    13    12    28    27    27
  25   25   78   78   118   119   221   222   222
  31   32  149  148   183   183   363   363   363
  38   38  121  122   209   209   368   368   369
  39   39  138  137   160   160   337   337   336
  29   28  138  139   148   148   315   315   315
  42   42  149  149   178   178   369   369   369
  34   34  156  156   172   172   362   362   362
```

## get-ds-state.sh
This script is typically run on a sled where there are Propolis zones.
It will print the zone, the PID, and the state of each downstairs for
the propolis-server process.

Example output would look like this:
```
root@BRM42220014:~# /tmp/get-ds-state.sh
oxz_propolis-server_34aa5f9c-d933-49fa-b38e-fc4607933d50  28022            active            active            active
oxz_propolis-server_3cef2a36-c2c3-49a6-bf38-f3d340264b1c   7737            active            active            active
oxz_propolis-server_3a1b039a-e41f-4968-a672-0d6900830219   8766            active            active            active
oxz_propolis-server_373e09ac-abcc-418d-a989-5184429042da   9984            active            active            active
oxz_propolis-server_491d90a6-813e-4b83-b158-7bf708dd91f5  10335            active            active            active
oxz_propolis-server_4d78774c-6818-4a2f-bceb-35ca5d9affa4  11016            active            active            active
oxz_propolis-server_f14ba802-0d1a-4320-b4a2-192f7a6d0c5e  11817            active            active            active
```

## get-lr-state.sh
This script is typically run on a sled where there are Propolis zones.
It will print the zone, the PID, and counters for live repair completed on
each of the three downstairs, then counters for live repair aborted on each
of the three downstairs.

Example output would look like this:
```
root@BRM42220014:~# /tmp/get-lr-state.sh
oxz_propolis-server_34aa5f9c-d933-49fa-b38e-fc4607933d50  28022 0 0 0 0 0 0
oxz_propolis-server_3cef2a36-c2c3-49a6-bf38-f3d340264b1c   7737 0 0 0 0 0 0
oxz_propolis-server_3a1b039a-e41f-4968-a672-0d6900830219   8766 0 0 0 0 0 0
oxz_propolis-server_373e09ac-abcc-418d-a989-5184429042da   9984 0 0 0 0 0 0
oxz_propolis-server_491d90a6-813e-4b83-b158-7bf708dd91f5  10335 0 0 0 0 0 0
oxz_propolis-server_4d78774c-6818-4a2f-bceb-35ca5d9affa4  11016 0 0 0 0 0 0
oxz_propolis-server_f14ba802-0d1a-4320-b4a2-192f7a6d0c5e  11817 0 0 0 0 0 0
```

## perf-upstairs-rw.d
A DTrace script to track writes and flushes through the upstairs.
The shows the time in three parts:
1: From IO received in the upstairs to IO being submitted to the
queue of work for the three downstairs.
2: From IO on downstairs queue, to enough downstairs completing the
IO that it is ready to ack.
3: From the IO being ready to ack, to that ack being sent.

## perf-ds-client.d
A DTrace script that records the time in the Upstairs from when a Message
is sent to a client task to when that client task returns the response.

## perf-ds-net.d
This is a simple DTrace script that measures latency times for when a r/w/f
job is sent over the network to each downstairs to when the ACK for that job
is returned to the upstairs. Jobs are sorted by type (r/w/f) and by each
downstairs client ID.
```
sudo dtrace -s perf-net-ds.d
```

Here is an example of how it might look:
```
final:crucible alan$ sudo sudo dtrace -Z -s perf-net-ds.d
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
This is a simple DTrace script that measures latency times for when a r/w/f
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

## perf-reqwest.d
This is a simple dtrace script that measures latency times for reads
to a volume having a read only parent.  The time is from when the
volume read only parent (ReqwestBlockIO) layer receives a read to when
that read has been completed.
```
pfexec dtrace -s perf-reqwest.d
```

## perf-vol.d
This dtrace script measures latency times for IOs at the volume layer.
This is essentially where an IO first lands in crucible and is measured
to when that IO is completed by the volume layer. IO is grouped by UUID and: read,
write, or flush.
```
pfexec dtrace -s perfvol.d
```
Example output:
```
dtrace: script 'tools/dtrace/perfvol.d' matched 19 probes
CPU     ID                    FUNCTION:NAME
 12  84104                         :tick-5s

 12  84104                         :tick-5s
  a416a597-5ec5-417f-a913-e2ee78bff1dc                volume-write-done
           value  ------------- Distribution ------------- count
          524288 |                                         0
         1048576 |@@@                                      2
         2097152 |@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@     27
         4194304 |@                                        1
         8388608 |                                         0

  a416a597-5ec5-417f-a913-e2ee78bff1dc                volume-flush-done
           value  ------------- Distribution ------------- count
          131072 |                                         0
          262144 |@@@                                      1
          524288 |@@@@@@@@@@@                              4
         1048576 |@@@                                      1
         2097152 |                                         0
         4194304 |@@@@@@@@@                                3
         8388608 |@@@@@@@@@                                3
        16777216 |@@@@@@                                   2
        33554432 |                                         0

  a416a597-5ec5-417f-a913-e2ee78bff1dc                volume-read-done
           value  ------------- Distribution ------------- count
          262144 |                                         0
          524288 |@                                        2
         1048576 |@@@@@@                                   9
         2097152 |@@@@@@@@@@@@@@@                          21
         4194304 |@@@@@@@@@@@@@@                           20
         8388608 |@@@                                      4
        16777216 |                                         0
```

## perf-downstairs-d
Trace all IOs from when the downstairs received them to when the downstairs
has completed them and is about to ack to the upstairs.  Grouped by IO
type (R/W/F).

## perf-downstairs-os.d
Trace all IOs on a downstairs from when the downstairs sent them to the OS for
servicing (almost, sort of, see the code) to when the downstairs receives
an answer back from the OS. Grouped by PID and IO type (R/W/F).

## perf-downstairs-three.d
Trace a downstairs IO and measure time for in in the following three parts:
* 1st report is time for IO received (from upstairs) to sending it to the OS.
* 2nd report is OS time (for flush, to flush all extents)
* 3rd report is OS done to downstairs sending the ACK back to upstairs

## upstairs_action.d
This is a dtrace script for printing the counts of the upstairs main action
loop.
```
pfexec dtrace -s upstairs_action.d
```

You start crucible, then run the above script.  Output should start appearing
within a few seconds.

The output has several columns.  The first column is the total count of time
the upstairs apply() was called in the main run loop.
The other columns indicate counters for each UpstairsAction the apply loop
has received.

Here is how some sample output might look:
```
    APPLY    DOWN_S     GUEST   DFR_BLK   DFR_MSG  LEAK_CHK FLUSH_CHK  STAT_CHK  REPR_CHK  CTRL_CHK      NOOP
    19533      8829      2945      1417      3792         0         1         1         0         0      2548
    39372     17769      5924      2791      7752         2         3         2         0         0      5129
    59638     26823      8941      4214     11870         3         5         3         0         0      7779
    78887     35580     11859      5545     15599         4         7         4         0         1     10288
    98570     44556     14849      6918     19395         5         9         5         0         2     12831
   117642     53205     17731      8259     23104         6        11         6         0         3     15317
   137393     62142     20709      9660     26965         6        13         7         0         4     17887
   157578     71220     23734     11043     31032         8        15         8         0         5     20513
   176640     79788     26590     12371     34814         9        17         9         0         6     23036
   195661     88512     29496     13719     38399        10        19        10         0         6     25490
   215616     97539     32503     15120     42307        11        21        11         0         6     28098
   234292    106008     35324     16515     45826        12        23        12         0         6     30566
```

## upstairs_info.d
This is a dtrace script for printing upstairs state and work queue info.
If the upstairs is not yet running, add the -Z flag to dtrace so it will
wait to find the matching probe.
```
pfexec dtrace -s upstairs_info.d
```

You start crucible, then run the above script.  Output should start appearing
within a few seconds.

The output has several columns.  The first is the PID of the process where
we collected the DTrace states.  The next three will list the state of each
of the three downstairs. 

Following that the remaining columns all indicate
various work queues and job state for work internal to the Upstairs.  Before we
describe the columns, a bit of detail about the internal structure of Crucible
upstairs is needed.

Inside the Upstairs there are two work queues.
* The upstairs (or guest) side
* The downstairs side.

For the upstairs side, this holds jobs the upstairs has received from the
guest that we have not Acked yet.  For every upstairs side, there is one or
more corresponding downstairs jobs that track the progress of the work required
to complete this job.  For each downstairs job, there is a state for that job
on each of the three downstairs.

Now, back to the columns.
Columns five and six show the count of active jobs for the guest side (UPW)
and for the downstairs side (DSW).

DELTA column is the difference in JobID since the last stat was gathered.
This is roughly the number of new jobs that have been sent to the downstairs.
BAKPR column shows the delay applied to guest write operations.
WRITE_BO column is the number of write bytes that haven't finished.

The remaining columns show the count of job states on each downstairs.
Jobs for the downstairs can be in one of five possible states:
 `New`, `In Progress`, `Done`, `Skipped`, `Error`.
For each state/downstairs, we print a count of jobs in that state, except
for the `Error` state which we don't print.

Here is an example of how it might look:
```
alan@cat:crucible$ pfexec dtrace -s upstairs_info.d
   PID  DS STATE 0  DS STATE 1  DS STATE 2  UPW   DSW DELTA BAKPR  WRITE_BO  NEW0 NEW1 NEW2  IP0 IP1 IP2   D0  D1  D2  S0 S1 S2
  1546      active      active      active    9   165   303    81  82837504     0    0    0   26  17 152  139 148  13   0  0  0
  1546      active      active      active    7   158   314    33  72351744     0    0    0   32  15 137  126 143  21   0  0  0
  1546      active      active      active    7   177   327     3  59768832     0    0    0   28  12 113  149 165  64   0  0  0
  1546      active      active      active    5   154   315    20  68157440     0    0    0   21  11 121  133 143  33   0  0  0
  1546      active      active      active    7   184   364     0  55574528     0    0    0   15  12 101  169 172  83   0  0  0
  1546      active      active      active    7   192   362     0  46155264     0    0    0   26  10  95  166 182  97   0  0  0
```

## upstairs_count.d
This is a dtrace script similar to the upstairs_info, but here we are
printing various upstairs counters.
If the upstairs is not yet running, add the -Z flag to dtrace so it will
wait to find the matching probe.
```
pfexec dtrace -s upstairs_count.d
```

You start crucible, then run the above script.  Output should start appearing
within a few seconds.

The output has several columns.  The first three will list the state of each
of the three downstairs.  Following that the remaining columns all indicate
various internal counters.  The upstairs records values for these counters
at an interval defined in the up_listen() function.

The remaining columns 4-15 are all groups of three where there is a counter
for each downstairs client.

`CON` The number of times the upstairs has connected to downstairs.
`LRC` The number of times this downstairs has completed a LiveRepair.
`LRA` The number of times this downstairs aborted a LiveRepair.
`REP` The number of times this downstairs was replaced.

Here is an example of how it might look:
```
alan@cat:crucible$ pfexec dtrace -s upstairs_count.d
       DS STATE 0        DS STATE 1        DS STATE 2  CON0 CON1 CON2 LRC0 LRC1 LRC2 LRA0 LRA1 LRA2 REP0 REP1 REP2
              new               new               new     1    1    1    0    0    0    0    0    0    0    0    0
              new               new               new     1    1    0    0    0    0    0    0    0    0    0    0
      wait_quorum       wait_quorum               new     1    1    0    0    0    0    0    0    0    0    0    0
           repair            repair            repair     1    1    1    0    0    0    0    0    0    0    0    0
           repair            repair            repair     1    1    1    0    0    0    0    0    0    0    0    0
```
## upstairs_raw.d
This is a dtrace script that just dumps the `Arg` structure in json format.
The output of this can be sent to other commands for additional processing.

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

## trace-vol.d
This is a dtrace script that will count and report the volume IOs of each type
and group by UUID.  Run the script then hit Control-C to see the results.
 An example of running it would look like this:
```
alan@atrium:prescrub$ pfexec dtrace -Z -s tools/dtrace/trace-vol.d
^C

5d8b2d34-40e3-4166-84c6-6094ec201d19      volume-flush-done        12
5d8b2d34-40e3-4166-84c6-6094ec201d19      volume-flush-start       12
5d8b2d34-40e3-4166-84c6-6094ec201d19      volume-write-done        39
5d8b2d34-40e3-4166-84c6-6094ec201d19      volume-write-start       39
5d8b2d34-40e3-4166-84c6-6094ec201d19      volume-read-done         49
5d8b2d34-40e3-4166-84c6-6094ec201d19      volume-read-start        49
```

