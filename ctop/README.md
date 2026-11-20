# ctop

A curses display of what every crucible upstairs on the system is
doing, built using the `up-status` DTrace probe.

This is Similar to what `cmon dtrace` shows, but all wrapped into a
single tool that updates in the same window.  DTrace needs privileges,
so ctop has to be started with them or ran as root.

## The table

An example output:
```
ctop - Unix timestamp: 1790785550

     PID  SESSION DS0 DS1 DS2    NEXTJOB DELTA EXTL RECD RECN
>   2100 aaaa1111 ACT ACT ACT      30223   710    0    0    0▇▇▇▆▅▄▃▂▁▁▁▁▁▂▃▄▅
    2101 bbbb2222 ACT ACT ACT      11600    40    0    0    0▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁
    2102 cccc3333 ACT ACT ACT      11095     5    0    0    0▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁

[up/down: Move | 's': Scale | 'q': Quit]  scale: all  * = stale (5s)     [1/3]
```

We show one row per upstairs session, sorted by pid.
The first two columns tell us
| | |
|---|---|
| `>` | the cursor is on this row |
| `*` | nothing has been heard from this session for five seconds |

A session that goes quiet for thirty seconds is dropped from the
table.

The sparkline on the right is that session's recent job rate, one
column per sample, newest on the right.  It takes whatever width after
we have printed all our columns.

## Control Keys

| key | |
|---|---|
| up, down | move the cursor |
| `s` | switch what the sparklines are measured against |
| `d` | show the selected session's history full screen, and back |
| Esc | back from the detail view |
| `q`, Ctrl-C | quit |

## Sparkline scale

`s` toggles how we scale every sparkline.  Both start at zero and
differ in what a full height bar means:

- `scale: all` measures every row against the busiest sample on
  screen, so the rows can be compared with each other.  A quiet
  session next to a busy one reads as flat.
- `scale: self` measures each row against its own largest sample, so
  every row fills its height and shows its shape.  Nothing can be read
  across rows: a session doing ten jobs a second looks like one doing
  ten thousand, and a session at a steady rate draws as a solid bar
  because every sample is its own maximum.

The footer indicates which choice is in effect.

## Detail view

`d` gives the selected session's job rate the whole screen:

```
   PID  SESSION DS0 DS1 DS2    NEXTJOB DELTA EXTL RECD RECN
  2100 aaaa1111 ACT ACT ACT      30223   710    0    0    0
┌ Job rate - PID 2100 - Session aaaa1111 ──────────────────────────────┐
│997   ⢀⠤⠒⠉⠉⠑⠢⢄                        ⢠⠒⠉⠉⠉⠑⠢⡀                        │
│747 ⢀⠔⠁       ⠱⡀                    ⢀⠔⠁      ⠈⢢                       │
│   ⠠⠊          ⠈⢆                  ⡔⠁          ⠑⡄                 ⢀⠎  │
│498             ⠈⠢⡀              ⢀⠜             ⠘⢄               ⢠⠊   │
│                  ⠑⢄            ⢀⠎                ⠣⡀            ⡰⠁    │
│249                ⠈⢆          ⡰⠁                  ⠘⢄         ⢀⠜      │
│                     ⠣⣀      ⢀⠎                      ⠣⡀      ⡠⠃       │
│0                      ⠉⠢⢄⡠⠤⠒⠁                        ⠈⠑⠤⣀⡠⠔⠊         │
└ Samples: 39 | Min: 0 | Max: 997 | Avg: 505 | Current: 710 ───────────┘
['d'/Esc: Back | 'q': Quit]
```

This graph is scaled to this session's own range, since there is only
one session on screen to compare.  The `s` setting still applies to
the table you come back to.

## Replaying captured output

`--dtrace-cmd` replaces the command ctop runs.  This can aid in testing
without a live system, or when replaying specific situations:

```
pfexec dtrace -s tools/dtrace/upstairs_raw.d > /tmp/capture.json
ctop --dtrace-cmd "cat /tmp/capture.json"
```

The records are read as fast as the file can be, rather than at the
rate they were produced, so the sparklines fill immediately.  ctop
stays up after the command finishes so the final state can be read

You can also use this to have ctop read in the output from a different
dtrace script, but note that it expects a specific output.
