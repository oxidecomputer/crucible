/*
 * Display internal Upstairs status.
 */
#pragma D option quiet
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
    printf("  DS STATE 0   DS STATE 1   DS STATE 2");
    printf("   UPW  DSW");
    printf("  NEW0 NEW1 NEW2");
    printf("   IP0  IP1  IP2");
    printf("    D0   D1   D2");
    printf("    S0   S1   S2");
    printf("  E0 E1 E2");
    printf("\n");
    show = 0;
}

crucible_upstairs*:::up-status
{
    show = show + 1;
    /*
     * State for the three downstiars
     */
    printf("%12s", json(copyinstr(arg1), "ok.ds_state[0]"));
    printf(" %12s", json(copyinstr(arg1), "ok.ds_state[1]"));
    printf(" %12s", json(copyinstr(arg1), "ok.ds_state[2]"));

    /*
     * Work queue counts for Upstairs and Downstairs
     */
    printf(" ");
    printf(" %4s", json(copyinstr(arg1), "ok.up_count"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_count"));

    /*
     * New jobs on the work list for each downstairs
     */
    printf(" ");
    printf(" %4s", json(copyinstr(arg1), "ok.ds_io_count.new[0]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_io_count.new[1]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_io_count.new[2]"));

    /*
     * In progress jobs on the work list for each downstairs
     */
    printf(" ");
    printf(" %4s", json(copyinstr(arg1), "ok.ds_io_count.in_progress[0]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_io_count.in_progress[1]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_io_count.in_progress[2]"));

    /*
     * Completed (done) jobs on the work list for each downstairs
     */
    printf(" ");
    printf(" %4s", json(copyinstr(arg1), "ok.ds_io_count.done[0]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_io_count.done[1]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_io_count.done[2]"));

    /*
     * Skipped jobs on the work list for each downstairs
     */
    printf(" ");
    printf(" %4s", json(copyinstr(arg1), "ok.ds_io_count.skipped[0]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_io_count.skipped[1]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_io_count.skipped[2]"));

    /*
     * Jobs that are done with errors on the work list for each downstairs
     */
    printf(" ");
    printf(" %2s", json(copyinstr(arg1), "ok.ds_io_count.error[0]"));
    printf(" %2s", json(copyinstr(arg1), "ok.ds_io_count.error[1]"));
    printf(" %2s", json(copyinstr(arg1), "ok.ds_io_count.error[2]"));

    printf("\n");
}
