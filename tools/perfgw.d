/*
 * Trace all the guest submitted and completed IOs.
 * Note, the way dtrace works with Rust means you have to start crucible
 * running before you can run this.
 */
cdt*:::gw_read_start,
cdt*:::gw_write_start,
cdt*:::gw_flush_start
{
    start[arg0] = timestamp;
}
cdt*:::gw_read_end
{
    @time["read"] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}
cdt*:::gw_write_end
{
    @time["write"] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}
cdt*:::gw_flush_end
{
    @time["flush"] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}
