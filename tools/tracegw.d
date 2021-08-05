// Trace all the guest submitted and completed IOs.
// Note, the way dtrace works with Rust means you have to start crucible
// running before you can run this.
#pragma D option quiet
crutrace*:::gw_read_start
{
    @read_start = count();
}
crutrace*:::gw_read_end
{
    @read_end = count();
}
crutrace*:::gw_write_start
{
    @write_start = count();
}
crutrace*:::gw_write_end
{
    @write_end = count();
}
crutrace*:::gw_flush_start
{
    @flush_start = count();
}
crutrace*:::gw_flush_end
{
    @flush_end = count();
}

END
{
    printa(" read_start:%@d    read_end:%@d\n", @read_start, @read_end);
    printa("write_start:%@d   write_end:%@d\n", @write_start, @write_end);
    printa("flush_start:%@d   flush_end:%@d\n", @flush_start, @flush_end);
}
