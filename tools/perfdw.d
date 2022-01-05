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
{
    start[arg0, arg1] = timestamp;
}

/*
 * When a read ACK comes back, calculate the delta and store it
 */
cdt*:::gw_read_submit_end
/start[arg0, arg1] != 0/
{
    @time["read", arg1] = quantize(timestamp - start[arg0, arg1]);
    start[arg0, arg1] = 0;
}

/*
 * When a write ACK comes back, calculate the delta and store it
 */
cdt*:::gw_write_submit_end
/start[arg0, arg1] != 0/
{
    @time["write", arg1] = quantize(timestamp - start[arg0, arg1]);
    start[arg0, arg1] = 0;
}

/*
 * When a flush ACK comes back, calculate the delta and store it
 */
cdt*:::gw_flush_submit_end
/start[arg0, arg1] != 0/
{
    @time["flush", arg1] = quantize(timestamp - start[arg0, arg1]);
    start[arg0, arg1] = 0;
}
