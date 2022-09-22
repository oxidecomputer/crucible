/*
 * Trace and time all the volume IOs.
 *
 * If you want a specific UUID only, do something like
 * this to the volume-*-start:
 *
 * /json(copyinstr(arg1), "ok")] == "SOME_UUID_YOU_WANT"/
 */
crucible_upstairs*:::volume-*-start
{
    start[arg0, json(copyinstr(arg1), "ok")] = timestamp;
}

crucible_upstairs*:::volume-*-done
/start[arg0, json(copyinstr(arg1), "ok")]/
{
    strtok(probename, "-");
    this->cmd = strtok(NULL, "-");
    this->uuid = json(copyinstr(arg1), "ok");
    @time[this->uuid, this->cmd] = quantize(timestamp - start[arg0, this->uuid]);
    start[arg0, this->uuid] = 0;
}

tick-5s
{
    printa(@time)
}
