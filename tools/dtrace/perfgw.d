/*
 * Trace all the guest submitted and completed IOs.
 */
crucible_upstairs*:::gw-*-start
{
    start[arg0] = timestamp;
}

crucible_upstairs*:::gw-*-done
/start[arg0]/
{
    strtok(probename, "-");
    this->cmd = strtok(NULL, "-");
    @time[this->cmd] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}
