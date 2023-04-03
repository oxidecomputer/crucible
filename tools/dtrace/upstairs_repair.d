/*
 * Display internal Upstairs online repair status.
 */
#pragma D option quiet
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
    printf("   DS STATE 0    DS STATE 1    DS STATE 2");
    printf(" CONF0 CONF1 CONF2 RPAR0 RPAR1 RPAR2");
    printf("\n");
    show = 0;
}

crucible_upstairs*:::up-status
{
    show = show + 1;
    /*
     * State for the three downstiars
     */
    printf("%13s", json(copyinstr(arg1), "ok.ds_state[0]"));
    printf(" %13s", json(copyinstr(arg1), "ok.ds_state[1]"));
    printf(" %13s", json(copyinstr(arg1), "ok.ds_state[2]"));

    /*
     * Repair counts for the downstairs
     */
    printf(" ");
    printf(" %5s", json(copyinstr(arg1), "ok.ds_confirm[0]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_confirm[1]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_confirm[2]"));
    printf(" %4s", json(copyinstr(arg1), "ok.ds_repair[0]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_repair[1]"));
    printf(" %5s", json(copyinstr(arg1), "ok.ds_repair[2]"));

    printf("\n");
}
