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
    printf(" %10s %6s %4s", "NEXT_JOB", "DELTA", "CONN");
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
 * Translate the longer state string into a shorter version
 */
inline string short_state[string ss] =
    ss == "Active" ? "ACT" :
    ss == "WaitQuorum" ? "WQ" :
    ss == "Reconcile" ? "REC" :
    ss == "LiveRepairReady" ? "LRR" :
    ss == "New" ? "NEW" :
    ss == "Faulted" ? "FLT" :
    ss == "Offline" ? "OFL" :
    ss == "LiveRepair" ? "LR" :
    ss == "Replacing" ? "RPC" :
    ss == "Disabled" ? "DIS" :
    ss == "Deactivated" ? "DAV" :
    ss == "NegotiationFailed" ? "NF" :
    ss == "Fault" ? "FLT" :
    ss;

/*
 * All variables should be this->
 * Otherwise, there is a chance another probe will fire and
 * clobber the contents.
 */
crucible_upstairs*:::up-status
{
    this->ds0state = json(copyinstr(arg1), "ok.ds_state[0]");
    this->d0 = short_state[this->ds0state];

    this->ds1state = json(copyinstr(arg1), "ok.ds_state[1]");
    this->d1 = short_state[this->ds1state];

    this->ds2state = json(copyinstr(arg1), "ok.ds_state[2]");
    this->d2 = short_state[this->ds2state];

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

    printf("%5d %8s %3s %3s %3s %10s %6d %4d %5d %5d %5s %5s\n",
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
