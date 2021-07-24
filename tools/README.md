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

dtrace: script 'tools/tracegw.d' matched 5 probes
^C
CPU     ID                    FUNCTION:NAME
  8      2                             :END gw_start:3000   gw_end:3000


```
That's all for now!
