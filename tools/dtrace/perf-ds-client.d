/*
 * Trace all IOs from the Upstairs for each Downstairs from the time they
 * are sent to the client task who handles the network tranmission to the
 * time the result message is returned to the main task and processing
 * is about to begin.
 * Group by IO type (R/W/F) and client ID (Which downstairs).
 *
 * arg0 is the job ID number.
 * arg1 is the client ID
 */
crucible_upstairs*:::ds-*-client-start
{
    start[arg0, arg1] = timestamp;
}

crucible_upstairs*:::ds-*-client-done
/start[arg0, arg1]/
{
    strtok(probename, "-");
    this->cmd = strtok(NULL, "-");

    @time[strjoin(this->cmd, " for downstairs client"), arg1] =
            quantize(timestamp - start[arg0, arg1]);
    start[arg0, arg1] = 0;
}
