/*
 * Trace all the volume submitted and completed IOs.
 */
#pragma D option quiet
crucible_upstairs*:::volume-*-start
{
    uuid = json(copyinstr(arg1), "ok");
    @io_count[uuid, probename] = count();
}
crucible_upstairs*:::volume-*-done
{
    uuid = json(copyinstr(arg1), "ok");
    @io_count[uuid, probename] = count();
}

END
{
    printa(@io_count);
}
