/*
 * Dump the up-status probe (the DtraceInfo structure) in json format,
 * one line per upstairs each time the probe fires.
 *
 * Each line is wrapped in an object that adds the pid of the upstairs
 * that produced it:
 *     {"pid":<pid>,"status":{ ...DtraceInfo... }}
 *
 * This script matches every crucible upstairs on the system, so lines
 * from different processes arrive interleaved.  The pid, along with the
 * session_id inside the status, is what tells them apart.
 *
 * The output is meant to be piped to a command that will display
 * whatever fields of the structure you wish; `cmon dtrace` does this.
 */
#pragma D option quiet
#pragma D option strsize=2k
crucible_upstairs*:::up-status
{
    printf("{\"pid\":%d,\"status\":%s}\n", pid, json(copyinstr(arg1), "ok"));
}
