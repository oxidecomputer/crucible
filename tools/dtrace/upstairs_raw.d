/*
 * Dump the dtrace up-status in json format
 * This can be used to pipe to other command that
 * can display whatever fields of the structure you wish.
 */
#pragma D option quiet
#pragma D option strsize=1k
crucible_upstairs*:::up-status
{
    trace(json(copyinstr(arg1), "ok"));
    printf("\n");
}
