// Trace all the guest submitted and completed IOs.
// Note, the way dtrace works with Rust means you have to start crucible
// running before you can run this.
crutrace*:::gw_read_start,
crutrace*:::gw_write_start,
crutrace*:::gw_flush_start
{
    start[arg0] = timestamp;
}
crutrace*:::gw_read_end
{
    @time["read"] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}
crutrace*:::gw_write_end
{
    @time["write"] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}
crutrace*:::gw_flush_end
{
    @time["flush"] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}
