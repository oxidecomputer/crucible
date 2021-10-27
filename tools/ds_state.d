/*
 * Display internal Upstairs status.
 * Note, the way dtrace works with Rust means you have to start crucible
 * running before you can run this.
 */
#pragma D option quiet
cdt*:::up_status
{
    printf("%s ", json(copyinstr(arg1), "ok.buffer"));
    printf("Upstairs:%4s ", json(copyinstr(arg1), "ok.up_count"));
    printf("Downstairs:%4s\n", json(copyinstr(arg1), "ok.ds_count"));
}
