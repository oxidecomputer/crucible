/*
 * Trace all IOs from the Upstairs for each Downstairs from the time they
 * are sent over the network socket to when the ack for an IO is received
 * back in the Upstairs.
 * Group by IO type (R/W/F) and client ID (Which downstairs).
 *
 * arg0 is the job ID number.
 * arg1 is the client ID
 */
cdt*:::ds-*-io-start
{
    start[arg0, arg1] = timestamp;
}

cdt*:::ds-*-io-done
/start[arg0, arg1]/
{
    strtok(probename, "-");
    this->cmd = strtok(NULL, "-");

    @time[strjoin(this->cmd, " for downstairs client"), arg1] =
            quantize(timestamp - start[arg0, arg1]);
    start[arg0, arg1] = 0;
}

