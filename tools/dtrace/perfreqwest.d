/*
 * Trace read ReqwestBlockIO.
 */
crucible_upstairs*:::reqwest-read-start
{
    start[arg0, json(copyinstr(arg1), "ok")] = timestamp;
}

crucible_upstairs*:::reqwest-read-done
/start[arg0, json(copyinstr(arg1), "ok")]/
{
    this->uuid = json(copyinstr(arg1), "ok");
    this->cmd = strtok(NULL, "-");
    @time[this->uuid, probename] = quantize(timestamp - start[arg0, this->uuid]);
    start[arg0, this->uuid] = 0;
}

tick-5s
{
    printa(@time)
}
