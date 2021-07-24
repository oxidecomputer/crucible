// Trace all the guest submitted and completed IOs.
// Note, the way dtrace works with Rust means you have to start crucible
// running before you can run this.
crutrace*:::gw_start
{
    @start = count();
}
crutrace*:::gw_end
{
    @end = count();
}

END
{
    printa("gw_start:%@d   gw_end:%@d", @start, @end);
}
