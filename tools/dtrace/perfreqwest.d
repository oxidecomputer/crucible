/*
 * Trace all the guest submitted and completed IOs.
 */
crucible_upstairs*:::reqwest-read-start
{
    start[arg0] = timestamp;
}

crucible_upstairs*:::reqwest-read-done
/start[arg0]/
{
    strtok(probename, "-");
    this->cmd = strtok(NULL, "-");
    @time[this->cmd] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}


tick-5s
{
    printa(@time)
}
