/*
 * Print a live repair status line for all matching probes.
 * Exit after 5 seconds.
 */
#pragma D option quiet
#pragma D option strsize=1k

crucible_upstairs*:::up-status
{
    my_sesh = json(copyinstr(arg1), "ok.session_id");

    printf("%6d %8s %s %s %s %s %s %s\n",
        pid,
        substr(my_sesh, 0, 8),
        json(copyinstr(arg1), "ok.ds_live_repair_completed[0]"),
        json(copyinstr(arg1), "ok.ds_live_repair_completed[1]"),
        json(copyinstr(arg1), "ok.ds_live_repair_completed[2]"),
        json(copyinstr(arg1), "ok.ds_live_repair_aborted[0]"),
        json(copyinstr(arg1), "ok.ds_live_repair_aborted[1]"),
        json(copyinstr(arg1), "ok.ds_live_repair_aborted[2]"));
}

tick-5s
{
    exit(0);
}
