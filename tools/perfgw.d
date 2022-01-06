/*
 * Trace all the guest submitted and completed IOs.
 */
cdt*:::gw-*-start
{
    start[arg0] = timestamp;
}

cdt*:::gw-*-done
/start[arg0]/
{
    strtok(probename, "-");
    this->cmd = strtok(NULL, "-");
    @time[this->cmd] = quantize(timestamp - start[arg0]);
    start[arg0] = 0;
}
