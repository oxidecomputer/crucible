/*
 * Print a status line for all matching probes.
 * Exit after 5 seconds.
 */
#pragma D option quiet
#pragma D option strsize=1k

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

crucible_upstairs*:::up-status
{
    my_id = json(copyinstr(arg1), "ok.upstairs_id");
    my_sesh = json(copyinstr(arg1), "ok.session_id");

    this->ds0state = json(copyinstr(arg1), "ok.ds_state[0]");
    this->d0 = short_state[this->ds0state];

    this->ds1state = json(copyinstr(arg1), "ok.ds_state[1]");
    this->d1 = short_state[this->ds1state];

    this->ds2state = json(copyinstr(arg1), "ok.ds_state[2]");
    this->d2 = short_state[this->ds2state];

    printf("%6d %8s %8s %3s %3s %3s\n",
        pid,
        substr(my_id, 0, 8),
        substr(my_sesh, 0, 8),
        this->d0,
        this->d1,
        this->d2);
}

tick-5s
{
    exit(0);
}
