/*
 * Display internal Upstairs live repair status.
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
dtrace:::BEGIN, tick-1s
/show > 20/
{
    printf("%3s %3s %3s", "DS0", "DS1", "DS2");
    printf(" ");

    /* Header width, three downstairs, space between: 5+1+5+1+5 = 17 */
    printf(" %17s %17s",
        "LR_COMPLETED", "LR_ABORTED");
    printf(" %17s %17s",
        "CONNECTED", "REPLACED");
    printf(" %17s %17s",
        "EXTENTS_REPAIRED", "EXTENTS_CONFIRMED");
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

crucible_upstairs*:::up-status
{
    show = show + 1;
    /*
     * State for the three downstairs
     */

    this->ds0state = json(copyinstr(arg1), "ok.ds_state[0]");
    this->d0 = short_state[this->ds0state];

    this->ds1state = json(copyinstr(arg1), "ok.ds_state[1]");
    this->d1 = short_state[this->ds1state];

    this->ds2state = json(copyinstr(arg1), "ok.ds_state[2]");
    this->d2 = short_state[this->ds2state];

    printf("%3s", this->d0);
    printf(" %3s", this->d1);
    printf(" %3s", this->d2);

    printf(" ");
    printf(" %5s", json(copyinstr(arg1), "ok.ds_live_repair_completed[0]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_live_repair_completed[1]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_live_repair_completed[2]"));

    printf(" %5s", json(copyinstr(arg1), "ok.ds_live_repair_aborted[0]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_live_repair_aborted[1]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_live_repair_aborted[2]"));

    printf(" %5s", json(copyinstr(arg1), "ok.ds_connected[0]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_connected[1]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_connected[2]"));

    printf(" %5s", json(copyinstr(arg1), "ok.ds_replaced[0]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_replaced[1]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_replaced[2]"));

    printf(" %5s", json(copyinstr(arg1), "ok.ds_extents_repaired[0]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_extents_repaired[1]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_extents_repaired[2]"));

    printf(" %5s", json(copyinstr(arg1), "ok.ds_extents_confirmed[0]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_extents_confirmed[1]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_extents_confirmed[2]"));

    printf("\n");
}
