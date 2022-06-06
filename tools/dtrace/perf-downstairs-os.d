/*
 * Trace all IOs from when the downstairs sent them to the OS for
 * servicing (almost, sort of, see the code) to when we got an
 * answer back from the OS.
 * Group by IO type (R/W/F).
 *
 * arg0 is the job ID number.
 */
crucible_downstairs*:::os-*-start
{
    start[pid,arg0] = timestamp;
}

crucible_downstairs*:::os-*-done
/start[pid,arg0]/
{
    strtok(probename, "-");
    this->cmd = strtok(NULL, "-");

    @time[pid,this->cmd] = quantize(timestamp - start[pid,arg0]);
    start[pid,arg0] = 0;
}
