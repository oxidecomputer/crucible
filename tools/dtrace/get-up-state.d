/*
 * Display Upstairs status for all matching processes
 */
#pragma D option quiet
#pragma D option strsize=1k

/*
 * Print the header right away
 */
dtrace:::BEGIN
{
    /*
     * We have to init something for last_id so we can use the
     * default values for all the session IDs that we don't yet have.
     */
    last_id["string"] = (int64_t)1;
    printf("%5s %8s ", "PID", "SESSION");
    printf("%3s %3s %3s", "DS0", "DS1", "DS2");
    printf(" %10s %5s %4s", "NEXT_JOB", "DELTA", "CONN");
    printf(" %5s %5s", "ELR", "ELC");
    printf(" %5s %5s", "ERR", "ERN");
    printf("\n");
}

/*
 * After reporting for 10 seconds, exit
 */
tick-10s
{
    exit(0);
}

/*
 * All variables should be this->
 * Otherwise, there is a chance another probe will fire and
 * clobber the contents.
 */
crucible_upstairs*:::up-status
{
    this->ds0state = json(copyinstr(arg1), "ok.ds_state[0]");
    if (this->ds0state == "active") {
        this->d0 = "ACT";
    } else if (this->ds0state == "new") {
        this->d0 = "NEW";
    } else if (this->ds0state == "live_repair_ready") {
        this->d0 = "LRR";
    } else if (this->ds0state == "live_repair") {
        this->d0 = " LR";
    } else if (this->ds0state == "faulted") {
        this->d0 = "FLT";
    } else if (this->ds0state == "offline") {
        this->d0 = "OFL";
    } else {
        this->d0 = this->ds0state;
    }

    this->ds1state = json(copyinstr(arg1), "ok.ds_state[1]");
    if (this->ds1state == "active") {
        this->d1 = "ACT";
    } else if (this->ds1state == "new") {
        this->d1 = "NEW";
    } else if (this->ds1state == "live_repair_ready") {
        this->d1 = "LRR";
    } else if (this->ds1state == "live_repair") {
        this->d1 = " LR";
    } else if (this->ds1state == "faulted") {
        this->d1 = "FLT";
    } else if (this->ds1state == "offline") {
        this->d1 = "OFL";
    } else {
        this->d1 = this->ds1state;
    }

    this->ds2state = json(copyinstr(arg1), "ok.ds_state[2]");
    if (this->ds2state == "active") {
        this->d2 = "ACT";
    } else if (this->ds2state == "new") {
        this->d2 = "NEW";
    } else if (this->ds2state == "live_repair_ready") {
        this->d2 = "LRR";
    } else if (this->ds2state == "live_repair") {
        this->d2 = " LR";
    } else if (this->ds2state == "faulted") {
        this->d2 = "FLT";
    } else if (this->ds2state == "offline") {
        this->d2 = "OFL";
    } else {
        this->d2 = this->ds2state;
    }

    /*
     * All these local variables require the "this->" so the probe firing
     * from different sessions don't collide with each other.
     */
    this->full_session_id = json(copyinstr(arg1), "ok.session_id");
    this->session_id = substr(this->full_session_id, 0, 8);

    this->next_id_str = json(copyinstr(arg1), "ok.next_job_id");
    this->next_id_value = strtoll(this->next_id_str);

    if (last_id[this->session_id] == 0) {
        this->delta = 0;
        last_id[this->session_id] = this->next_id_value;
    } else {
        this->delta = this->next_id_value - last_id[this->session_id];
    }

    /* Total of extents live repaired */
    this->elr = strtoll(json(copyinstr(arg1), "ok.ds_extents_repaired[0]")) +
        strtoll(json(copyinstr(arg1), "ok.ds_extents_repaired[1]")) +
        strtoll(json(copyinstr(arg1), "ok.ds_extents_repaired[2]"));
    /* Total of extents not needing repair during live repair */
    this->elc = strtoll(json(copyinstr(arg1), "ok.ds_extents_confirmed[0]")) +
        strtoll(json(copyinstr(arg1), "ok.ds_extents_confirmed[1]")) +
        strtoll(json(copyinstr(arg1), "ok.ds_extents_confirmed[2]"));

    this->connections = strtoll(json(copyinstr(arg1), "ok.ds_connected[0]")) +
        strtoll(json(copyinstr(arg1), "ok.ds_connected[1]")) +
        strtoll(json(copyinstr(arg1), "ok.ds_connected[2]"));

    printf("%5d %8s %3s %3s %3s %10s %5d %4d %5d %5d %5s %5s\n",
        pid,
        this->session_id,
        /*
         * State for the three downstairs
         */
        this->d0,
        this->d1,
        this->d2,

        /*
         * Job ID, job delta and write bytes outstanding
         */
        json(copyinstr(arg1), "ok.next_job_id"),
        this->delta,
        this->connections,
        this->elr,
        this->elc,
        json(copyinstr(arg1), "ok.ds_reconciled"),
        json(copyinstr(arg1), "ok.ds_reconcile_needed"));
}
