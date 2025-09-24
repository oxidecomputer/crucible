/*
 * Example code from the 2024 DTrace.conf talk
 */
#pragma D option quiet
#pragma D option strsize=4k

dtrace:::BEGIN
{
    /*
     * We have to init something for the associative array last_id.
     * This means it will be created and later, when we have a
     * session ID, we can add that element.
     */
    last_id["string"] = (int64_t)1;

    /*
     * Bump the show counter to print the header right away
     */
    show = 21;
}

/*
 * On startup and on some interval, print the header.
 */
dtrace:::BEGIN,
tick-1s
/show > 20/
{
    printf("%5s %8s %8s ", "PID", "UUID", "SESSION");
    printf("%3s %3s %3s", "DS0", "DS1", "DS2");
    printf(" %10s %6s %4s", "NEXT_JOB", "DELTA", "CONN");
    printf(" %5s %5s", "ELR", "ELC");
    printf(" %5s %5s", "ERR", "ERN");
    printf("\n");
    show = 0;
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
    ss == "Replaced" ? "RPL" :
    ss == "Disabled" ? "DIS" :
    ss == "Deactivated" ? "DAV" :
    ss == "NegotiationFailed" ? "NF" :
    ss == "Fault" ? "FLT" :
    ss;

/*
 * All variables should be this->
 * Otherwise, there is a chance another probe will fire and
 * clobber the contents.
 *
 * $target is zero if it's not set on the command line.  So we
 * either always print when the probe is fired and no PID is provided
 * or we only print when the probe fires for the requested PID.
 */
crucible_upstairs*:::up-status
/$target == 0 | $target == pid/
{
    show = show + 1;
    this->ds0state = json(copyinstr(arg1), "ok.ds_state[0]");
    this->d0 = short_state[this->ds0state];

    this->ds1state = json(copyinstr(arg1), "ok.ds_state[1]");
    this->d1 = short_state[this->ds1state];

    this->ds2state = json(copyinstr(arg1), "ok.ds_state[2]");
    this->d2 = short_state[this->ds2state];

    this->full_upstairs_id = json(copyinstr(arg1), "ok.upstairs_id");
    this->upstairs_id = substr(this->full_upstairs_id, 0, 8);

    this->full_session_id = json(copyinstr(arg1), "ok.session_id");
    this->session_id = substr(this->full_session_id, 0, 8);

    this->next_id_str = json(copyinstr(arg1), "ok.next_job_id");
    this->next_id_value = strtoll(this->next_id_str);

    /*
     * The first time through, we don't know delta, so start with 0.
     */
    if (last_id[this->session_id] == 0) {
        this->delta = 0;
        last_id[this->session_id] = this->next_id_value;
    } else {
        this->delta = this->next_id_value - last_id[this->session_id];
        last_id[this->session_id] = this->next_id_value;
    }

    this->connections = strtoll(json(copyinstr(arg1), "ok.ds_connected[0]")) +
        strtoll(json(copyinstr(arg1), "ok.ds_connected[1]")) +
        strtoll(json(copyinstr(arg1), "ok.ds_connected[2]"));

    /* Total of extents live repaired */
    this->elr = strtoll(json(copyinstr(arg1), "ok.ds_extents_repaired[0]")) +
        strtoll(json(copyinstr(arg1), "ok.ds_extents_repaired[1]")) +
        strtoll(json(copyinstr(arg1), "ok.ds_extents_repaired[2]"));

    /* Total of extents not needing repair during live repair */
    this->elc = strtoll(json(copyinstr(arg1), "ok.ds_extents_confirmed[0]")) +
        strtoll(json(copyinstr(arg1), "ok.ds_extents_confirmed[1]")) +
        strtoll(json(copyinstr(arg1), "ok.ds_extents_confirmed[2]"));

    printf("%5d %8s %8s %3s %3s %3s %10d %6d %4d %5d %5d %5s %5s\n",
        pid,
        this->upstairs_id,
        this->session_id,
        this->d0,
        this->d1,
        this->d2,
        this->next_id_value,
        this->delta,
        this->connections,
        this->elr,
        this->elc,
        json(copyinstr(arg1), "ok.ds_reconciled"),
        json(copyinstr(arg1), "ok.ds_reconcile_needed"));
}
