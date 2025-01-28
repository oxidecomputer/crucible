#pragma D option quiet
/*
 * IO counters for upstairs.
 */

/*
 * Print the header right away
 */
dtrace:::BEGIN
{
    show = 21;
}

crucible_upstairs*:::gw-flush-start
/pid == $1/
{
    @flush_start = count();
}

crucible_upstairs*:::gw-flush-done
/pid == $1/
{
    @flush_done = count();
}

crucible_upstairs*:::gw-write-start
/pid == $1/
{
    @write_start = count();
}

crucible_upstairs*:::gw-write-done
/pid == $1/
{
    @write_done = count();
}

crucible_upstairs*:::gw-read-start
/pid == $1/
{
    @read_start = count();
}

crucible_upstairs*:::gw-read-done
/pid == $1/
{
    @read_done = count();
}

crucible_upstairs*:::gw-write-unwritten-start
/pid == $1/
{
    @write_unwritten_start = count();
}

crucible_upstairs*:::gw-write-unwritten-done
/pid == $1/
{
    @write_unwritten_done = count();
}

crucible_upstairs*:::gw-barrier-start
/pid == $1/
{
    @barrier_start = count();
}

crucible_upstairs*:::gw-barrier-done
/pid == $1/
{
    @barrier_done = count();
}

/*
 * Every second, check and see if we have printed enough that it is
 * time to print the header again
 */
tick-1s
/show > 20/
{
    printf("%5s %5s %5s %5s %5s %5s %5s %5s %5s %5s",
        "F>", "F<", "W>", "W<", "R>", "R<", "WU>", "WU<", "B>", "B<");
    printf("\n");
    show = 0;
}

tick-1s
{
    printa("%@4u %@4u %@4u %@4u %@5u %@5u %@4u %@4u %@4u %@4u",
        @flush_start, @flush_done, @write_start, @write_done,
        @read_start, @read_done, @write_unwritten_start, @write_unwritten_done,
        @barrier_start, @barrier_done
    );
    printf("\n");
    clear(@flush_start);
    clear(@flush_done);
    clear(@write_start);
    clear(@write_done);
    clear(@read_start);
    clear(@read_done);
    clear(@write_unwritten_start);
    clear(@write_unwritten_done);
    clear(@barrier_start);
    clear(@barrier_done);
    show = show + 1;
}
