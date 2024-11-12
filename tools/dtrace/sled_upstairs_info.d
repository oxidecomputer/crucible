/*
 * Display internal Upstairs status.
 * This is an ease of use script that can be run on a sled and will
 * output stats for all propolis-server or pantry process (anything
 * that has an upstairs).  The PID and SESSION will be unique for
 * an upstairs. Multiple disks attached to a single propolis server
 * will share the PID, but have unique SESSIONs.
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
    printf("%5s %8s ", "PID", "SESSION");
    printf("%17s %17s %17s", "DS STATE 0", "DS STATE 1", "DS STATE 2");
    printf(" %5s %5s %9s %5s", "UPW", "DSW", "NEXT_JOB", "BAKPR");
    printf(" %10s", "WRITE_BO");
    printf("  %5s %5s %5s", "IP0", "IP1", "IP2");
    printf("  %5s %5s %5s", "D0", "D1", "D2");
    printf("  %5s %5s %5s", "S0", "S1", "S2");
    printf("  %5s %5s %5s", "ER0", "ER1", "ER2");
    printf("  %5s %5s %5s", "EC0", "EC1", "EC2");
    printf("\n");
    show = 0;
}

crucible_upstairs*:::up-status
{
    show = show + 1;
    session_id = json(copyinstr(arg1), "ok.session_id");

    /*
     * I'm not very happy about this very long muli-line printf, but if
     * we don't print it all on one line, then multiple sessions will
     * clobber each others output.
     */
    printf("%5d %8s %17s %17s %17s %5s %5s %5s %10s  %5s %5s %5s  %5s %5s %5s  %5s %5s %5s  %5s %5s %5s  %5s %5s %5s  %5s %5s %5s\n",

    pid,
    substr(session_id, 0, 8),

    /* State for the three downstairs */
    json(copyinstr(arg1), "ok.ds_state[0]"),
    json(copyinstr(arg1), "ok.ds_state[1]"),
    json(copyinstr(arg1), "ok.ds_state[2]"),

    /* Work queue counts for Upstairs and Downstairs */
    json(copyinstr(arg1), "ok.up_count"),
    json(copyinstr(arg1), "ok.ds_count"),

    /* Job ID and backpressure */
    json(copyinstr(arg1), "ok.next_job_id"),
    json(copyinstr(arg1), "ok.write_bytes_out"),

    /* In progress jobs on the work list for each downstairs */
    json(copyinstr(arg1), "ok.ds_io_count.in_progress[0]"),
    json(copyinstr(arg1), "ok.ds_io_count.in_progress[1]"),
    json(copyinstr(arg1), "ok.ds_io_count.in_progress[2]"),

    /* Completed (done) jobs on the work list for each downstairs */
    json(copyinstr(arg1), "ok.ds_io_count.done[0]"),
    json(copyinstr(arg1), "ok.ds_io_count.done[1]"),
    json(copyinstr(arg1), "ok.ds_io_count.done[2]"),

    /* Skipped jobs on the work list for each downstairs */
    json(copyinstr(arg1), "ok.ds_io_count.skipped[0]"),
    json(copyinstr(arg1), "ok.ds_io_count.skipped[1]"),
    json(copyinstr(arg1), "ok.ds_io_count.skipped[2]"),

    /* Extents Repaired */
    json(copyinstr(arg1), "ok.ds_extents_repaired[0]"),
    json(copyinstr(arg1), "ok.ds_extents_repaired[1]"),
    json(copyinstr(arg1), "ok.ds_extents_repaired[2]"),

    /* Extents Confirmed */
    json(copyinstr(arg1), "ok.ds_extents_confirmed[0]"),
    json(copyinstr(arg1), "ok.ds_extents_confirmed[1]"),
    json(copyinstr(arg1), "ok.ds_extents_confirmed[2]"));
}
