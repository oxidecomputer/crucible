/*
 * Trace how long extent IO takes.
 * Measure
 * - Total time to flush
 *   - Time to flush the OS file handle
 *   - Time to re-read the file from disk to re-hash (may be removed later)
 *   - Time to insert new hashes into sqlite DB
 */



/*
 * arg0 is job ID
 * arg1 is extent number
 * arg2 is number of blocks, when relevant (writes/reads)
 */

/*
 * flushes
 */
crucible_downstairs*:::extent-flush-start
{
    extent_flush_start[pid,arg0,arg1] = timestamp;
}

crucible_downstairs*:::extent-flush-file-start
{
    extent_flush_file_start[pid,arg0,arg1] = timestamp;
}

crucible_downstairs*:::extent-flush-rehash-start
{
    extent_flush_rehash_start[pid,arg0,arg1] = timestamp;
}

crucible_downstairs*:::extent-flush-sqlite-insert-start
{
    extent_flush_sqlite_insert_start[pid,arg0,arg1] = timestamp;
}


/* and collections */
crucible_downstairs*:::extent-flush-done
/extent_flush_start[pid,arg0,arg1]/
{
    @time["flush"] = quantize((timestamp - extent_flush_start[pid,arg0,arg1]) / arg2);
    extent_flush_start[pid,arg0,arg1] = 0;
}

crucible_downstairs*:::extent-flush-file-done
/extent_flush_file_start[pid,arg0,arg1]/
{
    @time["flush_file"] = quantize((timestamp - extent_flush_file_start[pid,arg0,arg1]) / arg2);
    extent_flush_file_start[pid,arg0,arg1] = 0;
}

crucible_downstairs*:::extent-flush-rehash-done
/extent_flush_rehash_start[pid,arg0,arg1]/
{
    @time["flush_rehash"] = quantize((timestamp - extent_flush_rehash_start[pid,arg0,arg1]) / arg2);
    extent_flush_rehash_start[pid,arg0,arg1] = 0;
}

crucible_downstairs*:::extent-flush-sqlite-insert-done
/extent_flush_sqlite_insert_start[pid,arg0,arg1]/
{
    @time["flush_sqlite_insert"] = quantize((timestamp - extent_flush_sqlite_insert_start[pid,arg0,arg1]) / arg2);
    extent_flush_sqlite_insert_start[pid,arg0,arg1] = 0;
}


/*
 * writes
 */
crucible_downstairs*:::extent-write-start
{
    extent_write_start[pid,arg0,arg1] = timestamp;
}

crucible_downstairs*:::extent-write-file-start
{
    extent_write_file_start[pid,arg0,arg1] = timestamp;
}

crucible_downstairs*:::extent-write-get-hashes-start
{
    extent_write_get_hashes_start[pid,arg0,arg1] = timestamp;
}

crucible_downstairs*:::extent-write-sqlite-insert-start
{
    extent_write_sqlite_insert_start[pid,arg0,arg1] = timestamp;
}


/* and collections */
crucible_downstairs*:::extent-write-done
/extent_write_start[pid,arg0,arg1]/
{
    @time["write"] = quantize((timestamp - extent_write_start[pid,arg0,arg1]) / arg2);
    extent_write_start[pid,arg0,arg1] = 0;
}

crucible_downstairs*:::extent-write-file-done
/extent_write_file_start[pid,arg0,arg1]/
{
    @time["write_file"] = quantize((timestamp - extent_write_file_start[pid,arg0,arg1]) / arg2);
    extent_write_file_start[pid,arg0,arg1] = 0;
}

crucible_downstairs*:::extent-write-get-hashes-done
/extent_write_get_hashes_start[pid,arg0,arg1]/
{
    @time["write_get_hashes"] = quantize((timestamp - extent_write_get_hashes_start[pid,arg0,arg1]) / arg2);
    extent_write_get_hashes_start[pid,arg0,arg1] = 0;
}

crucible_downstairs*:::extent-write-sqlite-insert-done
/extent_write_sqlite_insert_start[pid,arg0,arg1]/
{
    @time["write_sqlite_insert"] = quantize((timestamp - extent_write_sqlite_insert_start[pid,arg0,arg1]) / arg2);
    extent_write_sqlite_insert_start[pid,arg0,arg1] = 0;
}




/*
 * reads
 */
crucible_downstairs*:::extent-read-start
{
    extent_read_start[pid,arg0,arg1] = timestamp;
}

crucible_downstairs*:::extent-read-file-start
{
    extent_read_file_start[pid,arg0,arg1] = timestamp;
}

crucible_downstairs*:::extent-read-get-contexts-start
{
    extent_read_get_contexts_start[pid,arg0,arg1] = timestamp;
}


/* and collections */
crucible_downstairs*:::extent-read-done
/extent_read_start[pid,arg0,arg1]/
{
    @time["read"] = quantize((timestamp - extent_read_start[pid,arg0,arg1]) / arg2);
    extent_read_start[pid,arg0,arg1] = 0;
}

crucible_downstairs*:::extent-read-file-done
/extent_read_file_start[pid,arg0,arg1]/
{
    @time["read_file"] = quantize((timestamp - extent_read_file_start[pid,arg0,arg1]) / arg2);
    extent_read_file_start[pid,arg0,arg1] = 0;
}

crucible_downstairs*:::extent-read-get-contexts-done
/extent_read_get_contexts_start[pid,arg0,arg1]/
{
    @time["read_get_contexts"] = quantize((timestamp - extent_read_get_contexts_start[pid,arg0,arg1]) / arg2);
    extent_read_get_contexts_start[pid,arg0,arg1] = 0;
}

