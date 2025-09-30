#pragma D option quiet
/*
 * Print IO counters for all running downstairs.
 */
crucible_downstairs*:::submit-flush-start
{
    @sf_start[pid] = count();
}

crucible_downstairs*:::submit-flush-done
{
    @sf_done[pid] = count();
}

crucible_downstairs*:::submit-write-start
{
    @sw_start[pid] = count();
}

crucible_downstairs*:::submit-write-done
{
    @sw_done[pid] = count();
}

crucible_downstairs*:::submit-read-start
{
    @sr_start[pid] = count();
}

crucible_downstairs*:::submit-read-done
{
    @sr_done[pid] = count();
}

crucible_downstairs*:::submit-writeunwritten-start
{
    @swu_start[pid] = count();
}

crucible_downstairs*:::submit-writeunwritten-done
{
    @swu_done[pid] = count();
}
crucible_downstairs*:::work-start
{
    @work_start[pid] = count();
}
crucible_downstairs*:::work-process
{
    @work_process[pid] = count();
}
crucible_downstairs*:::work-done
{
    @work_done[pid] = count();
}


tick-4s
{
    printf("%5s %5s %5s %5s %5s %5s %5s %5s %5s %5s\n",
        "PID", "F>", "F<", "W>", "W<", "R>", "R<", "WS", "WIP", "WD");
    printa("%05d %@5u %@5u %@5u %@5u %@5u %@5u %@5u %@5u %@5u\n",
        @sf_start, @sf_done, @sw_start, @sw_done, @sr_start, @sr_done,
        @work_start, @work_process, @work_done
    );
    clear(@sf_start);
    clear(@sf_done);
    clear(@sw_start);
    clear(@sw_done);
    clear(@sr_start);
    clear(@sr_done);
    clear(@swu_start);
    clear(@swu_done);
    clear(@work_start);
    clear(@work_process);
    clear(@work_done);
}
