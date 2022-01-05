/*
 * Trace all IOs for each downstairs from the time they are sent over
 * the network socket to when the ack for an IO is received.
 * Group by IO type (R/W/F) and client ID (Which downstairs).
 *
 * arg0 is the job ID number.
 * arg1 is the client ID
 */
cdt*:::gw_read_submit_start,
cdt*:::gw_write_submit_start,
cdt*:::gw_flush_submit_start
/arg1 == 0/
{
    start0[arg0] = timestamp;
}
cdt*:::gw_read_submit_start,
cdt*:::gw_write_submit_start,
cdt*:::gw_flush_submit_start
/arg1 == 1/
{
    start1[arg0] = timestamp;
}
cdt*:::gw_read_submit_start,
cdt*:::gw_write_submit_start,
cdt*:::gw_flush_submit_start
/arg1 == 2/
{
    start2[arg0] = timestamp;
}

/*
 * When a read ACK comes back, calculate the delta and store it
 */
cdt*:::gw_read_submit_end
/start0[arg0] != 0 && arg1 == 0/
{
    @time["read downstairs client 0"] = quantize(timestamp - start0[arg0]);
    start0[arg0] = 0;
}
cdt*:::gw_read_submit_end
/start1[arg0] != 0 && arg1 == 1/
{
    @time["read downstairs client 1"] = quantize(timestamp - start1[arg0]);
    start1[arg0] = 0;
}
cdt*:::gw_read_submit_end
/start2[arg0] != 0 && arg1 == 2/
{
    @time["read downstairs client 2"] = quantize(timestamp - start2[arg0]);
    start2[arg0] = 0;
}

/*
 * When a write ACK comes back, calculate the delta and store it
 */
cdt*:::gw_write_submit_end
/start0[arg0] != 0 && arg1 == 0/
{
    @time["write downstairs client 0"] = quantize(timestamp - start0[arg0]);
    start0[arg0] = 0;
}
cdt*:::gw_write_submit_end
/start1[arg0] != 0 && arg1 == 1/
{
    @time["write downstairs client 1"] = quantize(timestamp - start1[arg0]);
    start1[arg0] = 0;
}
cdt*:::gw_write_submit_end
/start2[arg0] != 0 && arg1 == 2/
{
    @time["write downstairs client 2"] = quantize(timestamp - start2[arg0]);
    start2[arg0] = 0;
}

/*
 * When a flush ACK comes back, calculate the delta and store it
 */
cdt*:::gw_flush_submit_end
/start0[arg0] != 0 && arg1 == 0/
{
    @time["flush downstairs client 0"] = quantize(timestamp - start0[arg0]);
    start0[arg0] = 0;
}
cdt*:::gw_flush_submit_end
/start1[arg0] != 0 && arg1 == 1/
{
    @time["flush downstairs client 1"] = quantize(timestamp - start1[arg0]);
    start1[arg0] = 0;
}
cdt*:::gw_flush_submit_end
/start2[arg0] != 0 && arg1 == 2/
{
    @time["flush downstairs client 2"] = quantize(timestamp - start2[arg0]);
    start2[arg0] = 0;
}
