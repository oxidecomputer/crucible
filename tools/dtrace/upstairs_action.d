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
    printf(" %9s %9s %9s", "DEFER", "LEAK_CHK", "FLUSH_CHK");
    printf(" %9s %9s %9s", "STAT_CHK", "REPR_CHK", "CTRL_CHK");
    printf(" %9s", "NOOP");
    printf("\n");
    show = 0;
}

crucible_upstairs*:::up-status
{
    show = show + 1;
    printf("%9s", json(copyinstr(arg1), "ok.up_counters.apply"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_downstairs"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_guest"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_deferred"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_leak_check"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_flush_check"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_stat_check"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_repair_check"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_control_check"));
    printf(" %9s", json(copyinstr(arg1), "ok.up_counters.action_noop"));
    printf("\n");
}
