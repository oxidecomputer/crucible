/*
 * Display internal Upstairs live repair status.
 */
#pragma D option quiet
#pragma D option strsize=1k
/*
 * Print the header right away
 */
dtrace:::BEGIN
{
    show = 21;
}

/*
 * Every second, check and see if we have printed enough that it is
 * time to print the header again
 */
tick-1s
/show > 20/
{
    printf("%17s %17s %17s", "DS STATE 0", "DS STATE 1", "DS STATE 2");
    printf("  %4s %4s %4s %4s %4s %4s",
        "CON0", "CON1", "CON2", "LRC0", "LRC1", "LRC2");
    printf(" %4s %4s %4s %4s %4s %4s",
        "LRA0", "LRA1", "LRA2", "REP0", "REP1", "REP2");
    printf("\n");
    show = 0;
}

crucible_upstairs*:::up-status
{
    show = show + 1;
    /*
     * State for the three downstiars
     */
    printf("%17s", json(copyinstr(arg1), "ok.ds_state[0]"));
    printf(" %17s", json(copyinstr(arg1), "ok.ds_state[1]"));
    printf(" %17s", json(copyinstr(arg1), "ok.ds_state[2]"));

    /*
     * Repair counts for the downstairs
     */
    printf(" ");
    printf(" %4s", json(copyinstr(arg1), "ok.ds_connected[0]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_connected[1]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_connected[2]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_live_repair_completed[0]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_live_repair_completed[1]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_live_repair_completed[2]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_live_repair_aborted[0]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_live_repair_aborted[1]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_live_repair_aborted[2]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_replaced[0]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_replaced[1]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_replaced[2]"));

    printf("\n");
}
