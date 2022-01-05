/*
 * Trace all IOs for each downstairs from the time they are sent over
 * the network socket to when the ack for an IO is received.
 * Group by IO type (R/W/F) and client ID (Which downstairs).
 *
 * arg0 is the job ID number.
 * arg1 is the client ID
 */
cdt*:::gw-read-submit-start,
cdt*:::gw-write-submit-start,
cdt*:::gw-flush-submit-start
{
    start[arg0, arg1] = timestamp;
}

/*
 * When a read ACK comes back, calculate the delta and store it
 */
cdt*:::gw-read-submit-end
/start[arg0, arg1] != 0/
{
    @time["read", arg1] = quantize(timestamp - start[arg0, arg1]);
    start[arg0, arg1] = 0;
}

/*
 * When a write ACK comes back, calculate the delta and store it
 */
cdt*:::gw-write-submit-end
/start[arg0, arg1] != 0/
{
    @time["write", arg1] = quantize(timestamp - start[arg0, arg1]);
    start[arg0, arg1] = 0;
}

/*
 * When a flush ACK comes back, calculate the delta and store it
 */
cdt*:::gw-flush-submit-end
/start[arg0, arg1] != 0/
{
    @time["flush", arg1] = quantize(timestamp - start[arg0, arg1]);
    start[arg0, arg1] = 0;
}
