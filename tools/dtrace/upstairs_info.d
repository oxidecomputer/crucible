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
    last = 0;
}

/*
 * Every second, check and see if we have printed enough that it is
 * time to print the header again
 */
tick-1s
/show > 20/
{
    printf("%17s %17s %17s", "DS STATE 0", "DS STATE 1", "DS STATE 2");
    printf("  %5s %5s %5s %5s", "UPW", "DSW", "DELTA", "BAKPR");
    printf("  %5s %5s %5s", "NEW0", "NEW1", "NEW2");
    printf("  %5s %5s %5s", "IP0", "IP1", "IP2");
    printf("  %5s %5s %5s", "D0", "D1", "D2");
    printf("  %5s %5s %5s", "S0", "S1", "S2");
    printf("\n");
    show = 0;
}

crucible_upstairs*:::up-status
{
    show = show + 1;
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
     * Job ID delta and backpressure
     */
    current_str = json(copyinstr(arg1), "ok.next_job");
    current = strtoll(current_str, 10);
    if (last == 0)
        delta = current;
    else
        delta = current - last;

    last = current;
    printf(" %5d", delta);
    printf(" %5s", json(copyinstr(arg1), "ok.up_backpressure"));

    /*
     * New jobs on the work list for each downstairs
     */
    printf(" ");
    printf(" %5s", json(copyinstr(arg1), "ok.ds_io_count.new[0]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_io_count.new[1]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_io_count.new[2]"));

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
