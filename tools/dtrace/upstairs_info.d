/*
 * Display internal Upstairs status.
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
    printf("%6s ", "PID");
    printf("%17s %17s %17s", "DS STATE 0", "DS STATE 1", "DS STATE 2");
    printf("  %5s %5s %9s", "UPW", "DSW", "JOBID");
    printf(" %10s", "WRITE_BO");
    printf("  %5s %5s %5s", "IP0", "IP1", "IP2");
    printf("  %5s %5s %5s", "D0", "D1", "D2");
    printf("  %5s %5s %5s", "S0", "S1", "S2");
    printf("\n");
    show = 0;
}

crucible_upstairs*:::up-status
{
    show = show + 1;
    printf("%6d ", pid);
    /*
     * State for the three downstairs
     */
    printf("%17s", json(copyinstr(arg1), "ok.ds_state[0]"));
    printf(" %17s", json(copyinstr(arg1), "ok.ds_state[1]"));
    printf(" %17s", json(copyinstr(arg1), "ok.ds_state[2]"));

    /*
     * Work queue counts for Upstairs and Downstairs
     */
    printf(" ");
    printf(" %5s", json(copyinstr(arg1), "ok.up_count"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_count"));

    /*
     * Job ID and outstanding bytes
     */
    printf(" %9s", json(copyinstr(arg1), "ok.next_job_id"));
    printf(" %10s", json(copyinstr(arg1), "ok.write_bytes_out"));

    /*
     * In progress jobs on the work list for each downstairs
     */
    printf(" ");
    printf(" %5s", json(copyinstr(arg1), "ok.ds_io_count.in_progress[0]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_io_count.in_progress[1]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_io_count.in_progress[2]"));

    /*
     * Completed (done) jobs on the work list for each downstairs
     */
    printf(" ");
    printf(" %5s", json(copyinstr(arg1), "ok.ds_io_count.done[0]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_io_count.done[1]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_io_count.done[2]"));

    /*
     * Skipped jobs on the work list for each downstairs
     */
    printf(" ");
    printf(" %5s", json(copyinstr(arg1), "ok.ds_io_count.skipped[0]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_io_count.skipped[1]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_io_count.skipped[2]"));

    printf("\n");
}
