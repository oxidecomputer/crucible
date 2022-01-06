/*
 * Trace all the guest submitted and completed IOs.
 */
#pragma D option quiet
cdt*:::gw-read-start
{
    @read_start = count();
}
cdt*:::gw-read-end
{
    @read_end = count();
}
cdt*:::gw-write-start
{
    @write_start = count();
}
cdt*:::gw-write-end
{
    @write_end = count();
}
cdt*:::gw-flush-start
{
    @flush_start = count();
}
cdt*:::gw-flush-end
{
    @flush_end = count();
}

END
{
    printa(" read-start:%@d    read-end:%@d\n", @read_start, @read_end);
    printa("write-start:%@d   write-end:%@d\n", @write_start, @write_end);
    printa("flush-start:%@d   flush-end:%@d\n", @flush_start, @flush_end);
}
