/*
 * Watch a downstairs IO.
 * 1st report is submit to sending it to the OS.
 * 2nd report is OS time (for flush, to flush all extents)
 * 3rd report is OS done to sending ACK back to upstairs
 *
 * arg0 is the job ID number.
 */
crucible_downstairs*:::submit-flush-start
{
    start[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-flush-start
/start[pid,arg0]/
{
    @timeone[pid,"flush submit-OS"] = quantize(timestamp - start[pid,arg0]);
    start[pid,arg0] = 0;
    substart[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-flush-done
/substart[pid,arg0]/
{
    @timetwo[pid,"flush OS"] = quantize(timestamp - substart[pid,arg0]);
    substart[pid,arg0] = 0;
    final[pid,arg0] = timestamp;
}

crucible_downstairs*:::submit-flush-done
/final[pid,arg0]/
{
    @timethree[pid,"flush OS-done"] = quantize(timestamp - final[pid,arg0]);
    final[pid,arg0] = 0;
}

/*
 * Now the same, but for writes
 */
crucible_downstairs*:::submit-write-start
{
    start[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-write-start
/start[pid,arg0]/
{
    @timeone[pid,"write submit-OS"] = quantize(timestamp - start[pid,arg0]);
    start[pid,arg0] = 0;
    substart[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-write-done
/substart[pid,arg0]/
{
    @timetwo[pid,"write OS"] = quantize(timestamp - substart[pid,arg0]);
    substart[pid,arg0] = 0;
    final[pid,arg0] = timestamp;
}

crucible_downstairs*:::submit-write-done
/final[pid,arg0]/
{
    @timethree[pid,"write OS-done"] = quantize(timestamp - final[pid,arg0]);
    final[pid,arg0] = 0;
}

/*
 * Now the same, but for reads
 */
crucible_downstairs*:::submit-read-start
{
    start[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-read-start
/start[pid,arg0]/
{
    @timeone[pid,"read submit-OS"] = quantize(timestamp - start[pid,arg0]);
    start[pid,arg0] = 0;
    substart[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-read-done
/substart[pid,arg0]/
{
    @timetwo[pid,"read OS"] = quantize(timestamp - substart[pid,arg0]);
    substart[pid,arg0] = 0;
    final[pid,arg0] = timestamp;
}

crucible_downstairs*:::submit-read-done
/final[pid,arg0]/
{
    @timethree[pid,"read OS-done"] = quantize(timestamp - final[pid,arg0]);
    final[pid,arg0] = 0;
}

/*
 * Now the same, but for write unwritten
 */
crucible_downstairs*:::submit-writeunwritten-start
{
    start[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-writeunwritten-start
/start[pid,arg0]/
{
    @timeone[pid,"read submit-OS"] = quantize(timestamp - start[pid,arg0]);
    start[pid,arg0] = 0;
    substart[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-writeunwritten-done
/substart[pid,arg0]/
{
    @timetwo[pid,"read OS"] = quantize(timestamp - substart[pid,arg0]);
    substart[pid,arg0] = 0;
    final[pid,arg0] = timestamp;
}

crucible_downstairs*:::submit-writeunwritten-done
/final[pid,arg0]/
{
    @timethree[pid,"read OS-done"] = quantize(timestamp - final[pid,arg0]);
    final[pid,arg0] = 0;
}

tick-60s
{
    printa(@timeone)
}

tick-60s
{
    printa(@timetwo)
}

tick-60s
{
    printa(@timethree)
}
