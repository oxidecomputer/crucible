/*
 * Dump the dtrace up-status in json format
 * This can be used to pipe to other command that
 * can display whatever fields of the structure you wish.
 */
#pragma D option quiet
#pragma D option strsize=2k
crucible_upstairs*:::up-status
{
    printf("{\"pid\":%d,\"status\":%s}\n", pid, json(copyinstr(arg1), "ok"));
}
