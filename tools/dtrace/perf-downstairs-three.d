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
    wstart[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-write-start
/wstart[pid,arg0]/
{
    @wtimeone[pid,"write submit-OS"] = quantize(timestamp - wstart[pid,arg0]);
    wstart[pid,arg0] = 0;
    wsubstart[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-write-done
/wsubstart[pid,arg0]/
{
    @wtimetwo[pid,"write OS"] = quantize(timestamp - wsubstart[pid,arg0]);
    wsubstart[pid,arg0] = 0;
    wfinal[pid,arg0] = timestamp;
}

crucible_downstairs*:::submit-write-done
/wfinal[pid,arg0]/
{
    @wtimethree[pid,"write OS-done"] = quantize(timestamp - wfinal[pid,arg0]);
    wfinal[pid,arg0] = 0;
}

/*
 * Now the same, but for reads
 */
crucible_downstairs*:::submit-read-start
{
    wstart[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-read-start
/wstart[pid,arg0]/
{
    @wtimeone[pid,"read submit-OS"] = quantize(timestamp - wstart[pid,arg0]);
    wstart[pid,arg0] = 0;
    wsubstart[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-read-done
/wsubstart[pid,arg0]/
{
    @wtimetwo[pid,"read OS"] = quantize(timestamp - wsubstart[pid,arg0]);
    wsubstart[pid,arg0] = 0;
    wfinal[pid,arg0] = timestamp;
}

crucible_downstairs*:::submit-read-done
/wfinal[pid,arg0]/
{
    @wtimethree[pid,"read OS-done"] = quantize(timestamp - wfinal[pid,arg0]);
    wfinal[pid,arg0] = 0;
}

/*
 * Now the same, but for write unwritten
 */
crucible_downstairs*:::submit-writeunwritten-start
{
    wstart[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-writeunwritten-start
/wstart[pid,arg0]/
{
    @wtimeone[pid,"read submit-OS"] = quantize(timestamp - wstart[pid,arg0]);
    wstart[pid,arg0] = 0;
    wsubstart[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-writeunwritten-done
/wsubstart[pid,arg0]/
{
    @wtimetwo[pid,"read OS"] = quantize(timestamp - wsubstart[pid,arg0]);
    wsubstart[pid,arg0] = 0;
    wfinal[pid,arg0] = timestamp;
}

crucible_downstairs*:::submit-writeunwritten-done
/wfinal[pid,arg0]/
{
    @wtimethree[pid,"read OS-done"] = quantize(timestamp - wfinal[pid,arg0]);
    wfinal[pid,arg0] = 0;
}
