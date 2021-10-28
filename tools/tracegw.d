/*
 * Trace all the guest submitted and completed IOs.
 */
#pragma D option quiet
cdt*:::gw_read_start
{
    @read_start = count();
}
cdt*:::gw_read_end
{
    @read_end = count();
}
cdt*:::gw_write_start
{
    @write_start = count();
}
cdt*:::gw_write_end
{
    @write_end = count();
}
cdt*:::gw_flush_start
{
    @flush_start = count();
}
cdt*:::gw_flush_end
{
    @flush_end = count();
}

END
{
    printa(" read_start:%@d    read_end:%@d\n", @read_start, @read_end);
    printa("write_start:%@d   write_end:%@d\n", @write_start, @write_end);
    printa("flush_start:%@d   flush_end:%@d\n", @flush_start, @flush_end);
}
