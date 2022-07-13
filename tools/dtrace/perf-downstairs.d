/*
 * Trace all IOs from when the downstairs received them to when the
 * downstairs has completed them and is about to ack to the upstairs.
 * Group by IO type (R/W/F).
 *
 * arg0 is the job ID number.
 */
crucible_downstairs*:::submit-*-start
{
    start[pid,arg0] = timestamp;
}

crucible_downstairs*:::submit-*-done
/start[pid,arg0]/
{
    strtok(probename, "-");
    this->cmd = strtok(NULL, "-");

    @time[pid,this->cmd] = quantize(timestamp - start[pid,arg0]);
    start[pid,arg0] = 0;
}

