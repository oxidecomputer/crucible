/*
 * one906-panic.d: guest side panic trigger for reproducing
 * crucible#1906 (see tools/test-for-1906-notes.md).
 *
 * Run inside the VM hosting a downstairs while the crutest "one906"
 * workload is running against it:
 *
 *     dtrace -w -s one906-panic.d <N>
 *
 * Once a dmu_sync() is seen (a zil_commit is committing a WR_INDIRECT
 * log record, the vulnerable setup), panic the guest after the Nth
 * subsequent ZIL log block write completes.  Later log blocks from the
 * same commit may not be on disk yet, so ZIL replay on the next boot
 * stops partway through the commit.  If the cut lands between a data
 * write's log record and the log record holding the following write's
 * context slots, the extent comes back with data that matches neither
 * context slot and the downstairs fails to open the region.
 *
 * The harness (tools/test_one906.sh) arms this with a randomized N
 * (roughly 2-9) to vary where the ZIL chain is cut.  Panicking only
 * chooses the moment the guest dies; a real power loss at the same
 * instant would leave the same state on disk.
 */
#pragma D option destructive
#pragma D option quiet
#pragma D option defaultargs

dtrace:::BEGIN
{
	target = $1 != 0 ? $1 : 3;
	synced = 0;
	lwb = 0;
	printf("one906: armed, panic on lwb write %d after dmu_sync\n",
	    target);
}

fbt::dmu_sync:entry
{
	synced = 1;
}

fbt::zil_lwb_write_done:entry
/synced/
{
	lwb++;
}

fbt::zil_lwb_write_done:entry
/synced && lwb >= target/
{
	printf("one906: panicking on lwb write %d\n", lwb);
	panic();
}
