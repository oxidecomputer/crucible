/*
 * Trace all the guest submitted and completed IOs.
 */
cdt*:::gw-read-start,
cdt*:::gw-write-start,
cdt*:::gw-flush-start
{
    start[arg0] = timestamp;
}
cdt*:::gw-read-end
/start[arg0] != 0/
{
    @time["read"] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}
cdt*:::gw-write-end
/start[arg0] != 0/
{
    @time["write"] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}
cdt*:::gw-flush-end
/start[arg0] != 0/
{
    @time["flush"] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}
