#pragma D option quiet
/*
 * IO counters for downstairs.
 */

/*
 * Print the header right away
 */
dtrace:::BEGIN
{
    show = 21;
}

crucible_downstairs*:::submit-flush-start
/pid == $1/
{
    @sf_start = count();
}

crucible_downstairs*:::submit-flush-done
/pid == $1/
{
    @sf_done = count();
}

crucible_downstairs*:::submit-write-start
/pid == $1/
{
    @sw_start = count();
}

crucible_downstairs*:::submit-write-done
/pid == $1/
{
    @sw_done = count();
}

crucible_downstairs*:::submit-read-start
/pid == $1/
{
    @sr_start = count();
}

crucible_downstairs*:::submit-read-done
/pid == $1/
{
    @sr_done = count();
}

crucible_downstairs*:::submit-writeunwritten-start
/pid == $1/
{
    @swu_start = count();
}

crucible_downstairs*:::submit-writeunwritten-done
/pid == $1/
{
    @swu_done = count();
}
crucible_downstairs*:::work-start
/pid == $1/
{
    @work_start = count();
}
crucible_downstairs*:::work-process
/pid == $1/
{
    @work_process = count();
}
crucible_downstairs*:::work-done
/pid == $1/
{
    @work_done = count();
}

/*
 * Every second, check and see if we have printed enough that it is
 * time to print the header again
 */
tick-1s
/show > 20/
{
    printf("%4s %4s %4s %4s %5s %5s", "F>", "F<", "W>", "W<", "R>", "R<");
    printf(" %5s %5s %5s", "WS", "WIP", "WD");
    printf("\n");
    show = 0;
}

tick-2s
{
    printa("%@4u %@4u %@4u %@4u %@5u %@5u %@5u %@5u %@5u",
        @sf_start, @sf_done, @sw_start, @sw_done, @sr_start, @sr_done,
        @work_start, @work_process, @work_done
    );
    printf("\n");
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
    show = show + 1;
}
