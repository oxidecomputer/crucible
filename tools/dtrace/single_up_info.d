/*
 * Display internal Upstairs status for the PID provided as $1
 */
#pragma D option quiet
#pragma D option strsize=1k
/*
 * Print the header right away
 */
dtrace:::BEGIN
{
    show = 21;
    /*
     * We have to init something for last_id so we can use the
     * default values for all the session IDs that we don't yet have.
     */
    last_id["string"] = (int64_t)1;
}

/*
 * Every second, check and see if we have printed enough that it is
 * time to print the header again
 */
dtrace:::BEGIN, tick-1s
/show > 20/
{
    printf("%8s ", "SESSION");
    printf("%3s %3s %3s", "DS0", "DS1", "DS2");
    printf(" %5s %5s %10s %6s", "UPW", "DSW", "NEXT_JOB", "DELTA");
    printf(" %10s", "WRITE_BO");
    printf("  %5s %5s %5s", "IP0", "IP1", "IP2");
    printf("  %5s %5s %5s", "D0", "D1", "D2");
    printf("  %5s %5s %5s", "S0", "S1", "S2");
    printf("  %5s %5s %5s", "ER0", "ER1", "ER2");
    printf("  %5s %5s %5s", "EC0", "EC1", "EC2");
    printf("\n");
    show = 0;
}

/*
 * Translate the longer state string into a shorter version
 */
inline string short_state[string ss] =
    ss == "active" ? "ACT" :
    ss == "new" ? "NEW" :
    ss == "live_repair_ready" ? "LRR" :
    ss == "live_repair" ? "LR" :
    ss == "faulted" ? "FLT" :
    ss == "offline" ? "OFL" :
    ss == "reconcile" ? "REC" :
    ss == "wait_quorum" ? "WQ" :
    ss == "wait_active" ? "WA" :
    ss == "replaced" ? "RPL" :
    ss == "connecting" ? "CON" :
    ss;

crucible_upstairs*:::up-status
/pid==$1/
{
    show = show + 1;
    /*
     * All these local variables require the "this->" so the probe firing
     * from different sessions don't collide with each other.
     */
    this->ds0state = json(copyinstr(arg1), "ok.ds_state[0].type");
    this->d0 = short_state[this->ds0state];

    this->ds1state = json(copyinstr(arg1), "ok.ds_state[1].type");
    this->d1 = short_state[this->ds1state];

    this->ds2state = json(copyinstr(arg1), "ok.ds_state[2].type");
    this->d2 = short_state[this->ds2state];

    this->full_session_id = json(copyinstr(arg1), "ok.session_id");
    this->session_id = substr(this->full_session_id, 0, 8);

    this->next_id_str = json(copyinstr(arg1), "ok.next_job_id");
    this->next_id_value = strtoll(this->next_id_str);

    this->delta = this->next_id_value - last_id[this->session_id];

    /* Set this for the next loop */
    last_id[this->session_id] = this->next_id_value;
    /*
     * I'm not very happy about this, but if we don't print it all on one
     * line, then multiple sessions will clobber each others output.
     */
    printf("%8s %3s %3s %3s %5s %5s %10s %6d %10s  %5s %5s %5s  %5s %5s %5s  %5s %5s %5s  %5s %5s %5s  %5s %5s %5s\n",

    this->session_id,
    /*
     * State for the three downstairs
     */
    this->d0, this->d1, this->d2,

    /*
     * Work queue counts for Upstairs and Downstairs
     */
    json(copyinstr(arg1), "ok.up_count"),
    json(copyinstr(arg1), "ok.ds_count"),

    /*
     * Job ID, job delta and write bytes outstanding
     */
    json(copyinstr(arg1), "ok.next_job_id"),
    this->delta,
    json(copyinstr(arg1), "ok.write_bytes_out"),

    /*
     * In progress jobs on the work list for each downstairs
     */
    json(copyinstr(arg1), "ok.ds_io_count.in_progress[0]"),
    json(copyinstr(arg1), "ok.ds_io_count.in_progress[1]"),
    json(copyinstr(arg1), "ok.ds_io_count.in_progress[2]"),

    /*
     * Completed (done) jobs on the work list for each downstairs
     */
    json(copyinstr(arg1), "ok.ds_io_count.done[0]"),
    json(copyinstr(arg1), "ok.ds_io_count.done[1]"),
    json(copyinstr(arg1), "ok.ds_io_count.done[2]"),

    /*
     * Skipped jobs on the work list for each downstairs
     */
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
