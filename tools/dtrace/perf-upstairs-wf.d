/*
 * Watch a upstairs flush and write IOs.
 * 1st report is gw to ds sending it to the OS.
 * 2nd report is OS time (for flush, to flush all extents)
 * 3rd report is OS done to sending ACK back to upstairs/
 *
 * arg0 is the job ID number.
 */
crucible_upstairs*:::gw-flush-start
{
    start[arg0] = timestamp;
}

crucible_upstairs*:::up-to-ds-flush-start
/start[arg0]/
{
    @timeone["flush guest-upds"] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
    substart[arg0] = timestamp;
}

crucible_upstairs*:::up-to-ds-flush-done
/substart[arg0]/
{
    @timetwo["flush upds-ds-upds"] = quantize(timestamp - substart[arg0]);
    substart[arg0] = 0;
    final[arg0] = timestamp;
}

crucible_upstairs*:::gw-flush-done
/final[arg0]/
{
    @timethree["flush upds-guest"] = quantize(timestamp - final[arg0]);
    final[arg0] = 0;
}

/*
 * Now the same, but for writes
 */
crucible_upstairs*:::gw-write-start
{
    wstart[arg0] = timestamp;
}

crucible_upstairs*:::up-to-ds-write-start
/wstart[arg0]/
{
    @wtimeone["write guest-upds"] = quantize(timestamp - wstart[arg0]);
    wstart[arg0] = 0;
    wsubstart[arg0] = timestamp;
}

crucible_upstairs*:::up-to-ds-write-done
/wsubstart[arg0]/
{
    @wtimetwo["write upds-upds"] = quantize(timestamp - wsubstart[arg0]);
    wsubstart[arg0] = 0;
    wfinal[arg0] = timestamp;
}

crucible_upstairs*:::gw-write-done
/wfinal[arg0]/
{
    @wtimethree["write upds-guest"] = quantize(timestamp - wfinal[arg0]);
    wfinal[arg0] = 0;
}
