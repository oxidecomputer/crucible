/*
 * Print a status line for all matching probes.
 * Exit after 5 seconds.
 */
#pragma D option quiet
#pragma D option strsize=1k

crucible_upstairs*:::up-status
{
    my_sesh = json(copyinstr(arg1), "ok.session_id");

    printf("%6d %8s %17s %17s %17s\n",
        pid,
        substr(my_sesh, 0, 8),
        json(copyinstr(arg1), "ok.ds_state[0]"),
        json(copyinstr(arg1), "ok.ds_state[1]"),
        json(copyinstr(arg1), "ok.ds_state[2]"));
}

tick-5s
{
    exit(0);
}
