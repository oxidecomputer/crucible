/*
 * Watch upstairs flush and write IOs.
 * Report on:
 * 1: From IO received in the upstairs to IO being submitted to the
 * queue of work for the three downstairs.
 * 2: From IO on downstairs queue, to enough downstairs completing the
 * IO that it is ready to ack.
 * 3: From the IO being ready to ack, to that ack being sent.
 *
 * arg0 is the job ID number.
 */
crucible_upstairs*:::gw-flush-start,
crucible_upstairs*:::gw-write-start
{
    start[arg0] = timestamp;
}

crucible_upstairs*:::up-to-ds-flush-start,
crucible_upstairs*:::up-to-ds-write-start
/start[arg0]/
{
    @[probename] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
    substart[arg0] = timestamp;
}

crucible_upstairs*:::gw-flush-done,
crucible_upstairs*:::gw-write-done
/substart[arg0]/
{
    @[probename] = quantize(timestamp - substart[arg0]);
    substart[arg0] = 0;
}
