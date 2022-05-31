/*
 * Display internal Upstairs status.
 */
#pragma D option quiet
crucible_upstairs*:::up-status
{
    printf("%s ", json(copyinstr(arg1), "ok.ds_state"));
    printf("Upstairs:%4s ", json(copyinstr(arg1), "ok.up_count"));
    printf("Downstairs:%4s\n", json(copyinstr(arg1), "ok.ds_count"));
}
