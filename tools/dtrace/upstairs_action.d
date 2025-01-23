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
    printf("%9s %9s %9s", "APPLY", "DOWN_S", "GUEST");
    printf(" %9s %9s %9s", "DFR_BLK", "DFR_MSG", "LEAK_CHK");
    printf(" %9s %9s", "FLUSH_CHK", "STAT_CHK");
    printf(" %9s %9s", "CTRL_CHK", "NOOP");
    printf("\n");
    show = 0;
}

crucible_upstairs*:::up-status
{
    show = show + 1;
    printf("%9s", json(copyinstr(arg1), "ok.up_counters.apply"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_downstairs"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_guest"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_deferred_block"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_deferred_message"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_leak_check"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_flush_check"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_stat_check"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_control_check"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_noop"));
    printf("\n");
}
