/*
 * Trace all the guest submitted and completed IOs.
 */
#pragma D option quiet
crucible_upstairs*:::gw-read-start
{
    @read_start = count();
}
crucible_upstairs*:::gw-read-done
{
    @read_done = count();
}
crucible_upstairs*:::gw-write-start
{
    @write_start = count();
}
crucible_upstairs*:::gw-write-done
{
    @write_done = count();
}
crucible_upstairs*:::gw-flush-start
{
    @flush_start = count();
}
crucible_upstairs*:::gw-flush-done
{
    @flush_done = count();
}

END
{
    printa(" read-start:%@d    read-done:%@d\n", @read_start, @read_done);
    printa("write-start:%@d   write-done:%@d\n", @write_start, @write_done);
    printa("flush-start:%@d   flush-done:%@d\n", @flush_start, @flush_done);
}
