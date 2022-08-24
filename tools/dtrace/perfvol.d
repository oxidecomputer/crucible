/*
 * Trace and time all the volume IOs.
 */
crucible_upstairs*:::volume-*-start
{
    start[arg0, json(copyinstr(arg1), "ok")] = timestamp;
}

crucible_upstairs*:::volume-*-done
/start[arg0, json(copyinstr(arg1), "ok")]/
{
    this->uuid = json(copyinstr(arg1), "ok");
    @time[this->uuid, probename] = quantize(timestamp - start[arg0, this->uuid]);
    start[arg0, this->uuid] = 0;
}

tick-5s
{
    printa(@time)
}
