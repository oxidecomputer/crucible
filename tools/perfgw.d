/*
 * Trace all the guest submitted and completed IOs.
 */
cdt*:::gw_read_start,
cdt*:::gw_write_start,
cdt*:::gw_flush_start
{
    start[arg0] = timestamp;
}
cdt*:::gw_read_end
/start[arg0] != 0/
{
    @time["read"] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}
cdt*:::gw_write_end
/start[arg0] != 0/
{
    @time["write"] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}
cdt*:::gw_flush_end
/start[arg0] != 0/
{
    @time["flush"] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}
