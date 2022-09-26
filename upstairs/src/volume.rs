// Copyright 2021 Oxide Computer Company

use super::*;
use async_recursion::async_recursion;
use oximeter::types::ProducerRegistry;
use std::ops::Range;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crucible_client_types::VolumeConstructionRequest;

#[derive(Debug)]
pub struct Volume {
    uuid: Uuid,

    sub_volumes: Vec<SubVolume>,
    read_only_parent: Option<Arc<SubVolume>>,

    /*
     * The block below which the scrubber has written
     */
    scrub_point: Arc<AtomicU64>,

    /*
     * Each sub volume should be the same block size (unit is bytes)
     */
    block_size: u64,
    count: AtomicU32,
}

pub struct SubVolume {
    lba_range: Range<u64>,
    block_io: Arc<dyn BlockIO + Send + Sync>,
}

impl Debug for SubVolume {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("SubVolume")
            .field("lba_range", &self.lba_range)
            .finish()
    }
}

impl Volume {
    pub fn new_with_id(block_size: u64, uuid: Uuid) -> Volume {
        Self {
            uuid,
            sub_volumes: vec![],
            read_only_parent: None,
            scrub_point: Arc::new(AtomicU64::new(0)),
            block_size,
            count: AtomicU32::new(0),
        }
    }

    pub fn new(block_size: u64) -> Volume {
        Volume::new_with_id(block_size, Uuid::new_v4())
    }

    // Increment the counter to allow all IOs to have a unique number
    // for dtrace probes.
    pub fn next_count(&self) -> u32 {
        self.count.fetch_add(1, Ordering::Relaxed)
    }

    // Create a simple Volume from a single BlockIO
    pub async fn from_block_io(
        block_io: Arc<dyn BlockIO + Sync + Send>,
    ) -> Result<Volume, CrucibleError> {
        let block_size = block_io.get_block_size().await?;
        let uuid = block_io.get_uuid().await?;

        let sub_volume = SubVolume {
            lba_range: Range {
                start: 0,
                end: block_io.total_size().await? / block_size,
            },
            block_io,
        };

        Ok(Self {
            uuid,
            sub_volumes: vec![sub_volume],
            read_only_parent: None,
            scrub_point: Arc::new(AtomicU64::new(0)),
            block_size,
            count: AtomicU32::new(0),
        })
    }

    fn compute_next_lba_range(&self, number_of_blocks: u64) -> Range<u64> {
        if self.sub_volumes.is_empty() {
            Range {
                start: 0,
                end: number_of_blocks,
            }
        } else {
            let last_sub_volume_end =
                self.sub_volumes.last().unwrap().lba_range.end;

            Range {
                start: last_sub_volume_end,
                end: last_sub_volume_end + number_of_blocks,
            }
        }
    }

    pub async fn add_subvolume(
        &mut self,
        block_io: Arc<dyn BlockIO + Send + Sync>,
    ) -> Result<(), CrucibleError> {
        let block_size = block_io.get_block_size().await?;

        if block_size != self.block_size {
            crucible_bail!(BlockSizeMismatch);
        }

        let number_of_blocks = block_io.total_size().await? / block_size;

        self.sub_volumes.push(SubVolume {
            lba_range: self.compute_next_lba_range(number_of_blocks),
            block_io,
        });

        Ok(())
    }

    pub async fn add_subvolume_create_guest(
        &mut self,
        opts: CrucibleOpts,
        gen: u64,
        producer_registry: Option<ProducerRegistry>,
    ) -> Result<(), CrucibleError> {
        let guest = Arc::new(Guest::new());

        // Spawn crucible tasks
        let guest_clone = guest.clone();
        tokio::spawn(async move {
            // XXX result eaten here!
            let _ = up_main(opts, gen, guest_clone, producer_registry).await;
        });

        guest.activate(gen).await?;

        self.add_subvolume(guest).await
    }

    // Add a "parent" source for blocks.
    //
    // Imagine three sub volumes:
    //
    //   sub volumes:    |------------|------------|------------|
    //
    // This function adds the ability to "get" blocks from a read only source
    // that starts at LBA 0 and whose size is less than or equal to :
    //
    //   sub volumes:    |------------|------------|------------|
    //   read-only src:  |xxxxxxxxxxxxxxxxxxx|
    //
    // This will be used to construct volumes from snapshots. Among other
    // things.
    //
    pub async fn add_read_only_parent(
        &mut self,
        block_io: Arc<dyn BlockIO + Send + Sync>,
    ) -> Result<(), CrucibleError> {
        let block_size = block_io.get_block_size().await?;

        if block_size != self.block_size {
            crucible_bail!(BlockSizeMismatch);
        }

        let number_of_blocks = block_io.total_size().await? / block_size;

        // TODO migration task to pull blocks in from parent, then set parent to
        // None when done - read block by block, don't overwrite if owned by sub
        // volume

        self.read_only_parent = Some(Arc::new(SubVolume {
            // Read only parent LBA range always starts from 0
            lba_range: Range {
                start: 0,
                end: number_of_blocks,
            },
            block_io,
        }));

        Ok(())
    }

    // Imagine three sub volumes:
    //
    //  0 -> Range(0, a)
    //  1 -> Range(a, b)
    //  2 -> Range(b, c)
    //
    // Read or write requests could come in and require one, two, or three:
    //
    //                   0            a            b            c
    //   sub volumes:    |------------|------------|------------|
    //   read request 1:     |------|
    //   read request 2:                   |------------|
    //   read request 3:    |-------------------------------|
    //
    // For request 1, it affects sub volume 0, and the offset that the user
    // wants maps directly to the offset of the sub volume.
    //
    // For request 2, it affects sub volumes 1 and 2. The logical block address
    // of the volume has to be translated to the logical block address of
    // the sub volume.
    //
    // For request 3, it affects all sub volumes.
    //
    // Sort that out here. Note: start and length are in blocks!
    //
    pub fn sub_volumes_for_lba_range(
        &self,
        start: u64,
        length: u64,
    ) -> Vec<(Range<u64>, &SubVolume)> {
        let mut sv_vec = vec![];

        for sub_volume in &self.sub_volumes {
            let coverage = sub_volume.lba_range_coverage(start, length);
            if let Some(coverage) = coverage {
                sv_vec.push((coverage, sub_volume));
            }
        }

        sv_vec
    }

    pub fn read_only_parent_for_lba_range(
        &self,
        start: u64,
        length: u64,
    ) -> Option<Range<u64>> {
        if let Some(ref read_only_parent) = self.read_only_parent {
            // Check if the scrubber has passed this offset
            let scrub_point = self.scrub_point.load(Ordering::SeqCst);
            if start + length <= scrub_point {
                None
            } else {
                read_only_parent.lba_range_coverage(start, length)
            }
        } else {
            None
        }
    }

    // Scrub a volume.
    // If a volume has a read only parent, we do the work to read from
    // the read only side, and write_unwritten to the LBA of the SubVolume
    // that is "below" it.
    //
    // Still TODO: here
    // If there is an error, how to we tell Nexus?
    // If we finish the scrub, how do we tell Nexus?
    pub async fn scrub(&self) -> Result<(), CrucibleError> {
        println!("Scrub check for {}", self.uuid);
        // XXX Can we assert volume is activated?

        if let Some(ref read_only_parent) = self.read_only_parent {
            let ts = read_only_parent.total_size().await?;
            let bs = read_only_parent.get_block_size().await? as usize;

            let scrub_start = Instant::now();
            println!("Scrub for {} begins", self.uuid);

            println!("Scrub with total_size:{:?} block_size:{:?}", ts, bs,);
            let start = read_only_parent.lba_range.start;
            let end = read_only_parent.lba_range.end;

            // Based on some seat of the pants measurements, we are doing
            // 256 KiB IOs during the scrub. Here we select how many blocks
            // this is.
            // TODO: Determine if this value should be adjusted.
            let mut block_count = 262144 / bs;
            println!(
                "Scrub from block {:?} to {:?} in ({}) {:?} size IOs",
                start,
                end,
                block_count,
                block_count * bs,
            );

            let mut retries = 0;
            let showstep = (end - start) / 25;
            let mut showat = start + showstep;
            let mut offset = start;
            while offset < end {
                if offset + block_count as u64 > end {
                    block_count = (end - offset) as usize;
                    println!(
                        "Adjust block_count to {} at offset {}",
                        block_count, offset
                    );
                }
                assert!(offset + block_count as u64 <= end);
                let block = Block::new(offset, bs.trailing_zeros());
                let buffer = Buffer::new(block_count * bs);

                // The read will first try to establish a connection to
                // the remote side before returning something we can await
                // on. If that initial connection fails, we can retry.
                let mut retry_needed = true;
                let mut retry_count = 0;
                while retry_needed {
                    match read_only_parent.read(block, buffer.clone()).await {
                        Ok(_) => {
                            retry_needed = false;
                            if retry_count > 0 {
                                // This counter indicates a retry was
                                // eventually successful.
                                retries += 1;
                            }
                        }
                        Err(e) => {
                            println!("scrub {}, offset {}", e, offset);
                            retry_count += 1;
                        }
                    }
                    if retry_count > 5 {
                        // TODO: Nexus needs to know the scrub had problems.
                        crucible_bail!(
                            IoError,
                            "Scrub failed to read at offset {}",
                            offset
                        );
                    }
                }

                // TODO: Nexus needs to know about this failure.
                self.write_unwritten(
                    Block::new(offset, bs.trailing_zeros()),
                    Bytes::from(buffer.as_vec().await.clone()),
                )
                .await?;

                offset += block_count as u64;

                // Set the scrub high water mark
                self.scrub_point.store(offset, Ordering::SeqCst);

                if offset > showat {
                    println!(
                        "Scrub at offset {}/{} sp:{:?}",
                        offset, end, self.scrub_point
                    );
                    showat += showstep;
                }
            }

            let total_time = scrub_start.elapsed();
            println!(
                "Scrub {} done in {} seconds. Retries:{}",
                self.uuid,
                total_time.as_secs(),
                retries,
            );
            self.flush(None).await?;
        } else {
            println!("Scrub for {} not required", self.uuid);
        }

        Ok(())
    }

    // This method is called by both write and write_unwritten and
    // provides a single place so both can share common code.
    async fn volume_write_op(
        &self,
        offset: Block,
        data: Bytes,
        is_write_unwritten: bool,
    ) -> Result<(), CrucibleError> {
        // In the case that this volume only has a read only parent,
        // return an error.
        if self.sub_volumes.is_empty() {
            crucible_bail!(CannotReceiveBlocks, "No sub volumes!");
        }
        let cc = self.next_count();
        if is_write_unwritten {
            cdt::volume__writeunwritten__start!(|| (cc, self.uuid));
        } else {
            cdt::volume__write__start!(|| (cc, self.uuid));
        }

        let affected_sub_volumes = self.sub_volumes_for_lba_range(
            offset.value,
            data.len() as u64 / self.block_size,
        );

        // TODO parallel dispatch!
        let mut data_index = 0;
        for (coverage, sub_volume) in affected_sub_volumes {
            let sub_offset = Block::new(
                sub_volume.compute_sub_volume_lba(coverage.start),
                offset.shift,
            );
            let sz = (coverage.end - coverage.start) as usize
                * self.block_size as usize;
            let slice_range = Range::<usize> {
                start: data_index,
                end: data_index + sz,
            };
            let slice = data.slice(slice_range);

            // Take the write or write_unwritten path here.
            if is_write_unwritten {
                sub_volume
                    .write_unwritten(sub_offset, slice.clone())
                    .await?;
            } else {
                sub_volume.write(sub_offset, slice.clone()).await?;
            }

            data_index += sz;
        }

        if is_write_unwritten {
            cdt::volume__writeunwritten__done!(|| (cc, self.uuid));
        } else {
            cdt::volume__write__done!(|| (cc, self.uuid));
        }

        Ok(())
    }
}

#[async_trait]
impl BlockIO for Volume {
    async fn activate(&self, gen: u64) -> Result<(), CrucibleError> {
        for sub_volume in &self.sub_volumes {
            sub_volume.conditional_activate(gen).await?;

            let sub_volume_computed_size = self.block_size
                * (sub_volume.lba_range.end - sub_volume.lba_range.start);

            if sub_volume.total_size().await? != sub_volume_computed_size {
                crucible_bail!(SubvolumeSizeMismatch);
            }
        }

        if let Some(ref read_only_parent) = &self.read_only_parent {
            read_only_parent.conditional_activate(gen).await?;
        }

        Ok(())
    }

    async fn deactivate(&self) -> Result<(), CrucibleError> {
        for sub_volume in &self.sub_volumes {
            sub_volume.deactivate().await?;
        }

        if let Some(ref read_only_parent) = &self.read_only_parent {
            read_only_parent.deactivate().await?;
        }

        Ok(())
    }

    async fn query_is_active(&self) -> Result<bool, CrucibleError> {
        for sub_volume in &self.sub_volumes {
            if !sub_volume.query_is_active().await? {
                return Ok(false);
            }
        }

        if let Some(ref read_only_parent) = &self.read_only_parent {
            if !read_only_parent.query_is_active().await? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    async fn total_size(&self) -> Result<u64, CrucibleError> {
        if !self.sub_volumes.is_empty() {
            // If this volume has sub volumes, compute total size based on those
            let mut total_blocks = 0;

            for sub_volume in &self.sub_volumes {
                // Range is [start, end), meaning 0..10 is 10
                total_blocks += (sub_volume.lba_range.end
                    - sub_volume.lba_range.start)
                    as u64;
            }

            Ok(total_blocks * self.block_size)
        } else if let Some(ref read_only_parent) = &self.read_only_parent {
            // If this volume only has a read only parent, report that size for
            // total size
            let total_blocks = read_only_parent.lba_range.end
                - read_only_parent.lba_range.start;
            Ok(total_blocks * self.block_size)
        } else {
            // If this volume has neither, then total size is 0
            Ok(0)
        }
    }

    async fn get_block_size(&self) -> Result<u64, CrucibleError> {
        Ok(self.block_size)
    }

    async fn get_uuid(&self) -> Result<Uuid, CrucibleError> {
        Ok(self.uuid)
    }

    async fn read(
        &self,
        offset: Block,
        data: Buffer,
    ) -> Result<(), CrucibleError> {
        // In the case that this volume only has a read only parent, serve
        // reads directly from that
        let cc = self.next_count();
        cdt::volume__read__start!(|| (cc, self.uuid));
        if self.sub_volumes.is_empty() {
            if let Some(ref read_only_parent) = &self.read_only_parent {
                let res = read_only_parent.read(offset, data).await;
                cdt::volume__read__done!(|| (cc, self.uuid));
                return res;
            } else {
                crucible_bail!(
                    CannotServeBlocks,
                    "No read only parent, no sub volumes!",
                );
            }
        }

        let affected_sub_volumes = self.sub_volumes_for_lba_range(
            offset.value,
            data.len().await as u64 / self.block_size,
        );

        // TODO parallel dispatch!
        let mut data_index = 0;
        for (coverage, sub_volume) in affected_sub_volumes {
            let sub_offset = Block::new(
                sub_volume.compute_sub_volume_lba(coverage.start),
                offset.shift,
            );
            // 0..10 would be size 10
            let sz = (coverage.end - coverage.start) as usize
                * self.block_size as usize;
            let sub_buffer = Buffer::new(sz);

            sub_volume.read(sub_offset, sub_buffer.clone()).await?;

            // When performing a read, check the parent coverage: if it's
            // Some(range), then we have to perform multiple reads:
            //
            // - if the sub volume block is "owned", then return it
            // - otherwise, return the read only parent block
            let parent_coverage = self.read_only_parent_for_lba_range(
                coverage.start,
                coverage.end - coverage.start,
            );

            if let Some(parent_coverage) = parent_coverage {
                let parent_offset = Block::new(coverage.start, offset.shift);
                let sub_sz = self.block_size as usize
                    * (parent_coverage.end - parent_coverage.start) as usize;

                let parent_buffer = Buffer::new(sub_sz);
                self.read_only_parent
                    .as_ref()
                    .unwrap()
                    .read(parent_offset, parent_buffer.clone())
                    .await?;

                let mut data_vec = data.as_vec().await;
                let mut owned_vec = data.owned_vec().await;

                let sub_data_vec = sub_buffer.as_vec().await;
                let sub_owned_vec = sub_buffer.owned_vec().await;
                let parent_data_vec = parent_buffer.as_vec().await;

                // "ownership" comes back from the downstairs per byte but all
                // writes occur per block. Iterate over the blocks that both the
                // read-only parent and subvolume cover here.
                //
                // This layer of the volume "owns" a block if the subvolume
                // "owns" the block, *not* if the read only parent does.
                for i in 0..(sub_sz as u64 / self.block_size) {
                    let start_of_block = (i * self.block_size) as usize;

                    if sub_owned_vec[start_of_block] {
                        // sub volume has written to this block, use it
                        for block_offset in 0..self.block_size {
                            let inside_block =
                                start_of_block + block_offset as usize;

                            data_vec[data_index + inside_block] =
                                sub_data_vec[inside_block];

                            // this layer of the volume does own this
                            // block, it was written to the subvolume
                            owned_vec[data_index + inside_block] = true;
                        }
                    } else {
                        // sub volume hasn't written to this block, use the
                        // parent
                        for block_offset in 0..self.block_size {
                            let inside_block =
                                start_of_block + block_offset as usize;

                            data_vec[data_index + inside_block] =
                                parent_data_vec[inside_block];

                            // this layer of the volume doesn't own this
                            // block, it came from the read only parent. Note
                            // this doesn't depend if the block was owned by
                            // the read only parent, just that the subvolume
                            // doesn't own it.
                            owned_vec[data_index + inside_block] = false;
                        }
                    }
                }

                // Iterate over all the blocks that only come from the subvolume
                // here.
                for i in (sub_sz as u64 / self.block_size)
                    ..(sz as u64 / self.block_size)
                {
                    let start_of_block = (i * self.block_size) as usize;
                    for block_offset in 0..self.block_size {
                        let inside_block =
                            start_of_block + block_offset as usize;

                        data_vec[data_index + inside_block] =
                            sub_data_vec[inside_block];

                        // this layer of the volume does own this block, it came
                        // from the subvolume's LBA - the read-only parent
                        // cannot own this block because it's outside their LBA.
                        owned_vec[data_index + inside_block] = true;
                    }
                }
            } else {
                let mut data_vec = data.as_vec().await;
                let mut owned_vec = data.owned_vec().await;

                // In the case where this volume has no read only parent,
                // propagate the sub volume information up.
                data_vec[data_index..(data_index + sz)]
                    .copy_from_slice(&sub_buffer.as_vec().await);
                owned_vec[data_index..(data_index + sz)]
                    .copy_from_slice(&sub_buffer.owned_vec().await);
            }

            data_index += sz;
        }

        assert_eq!(data.len().await, data_index);

        cdt::volume__read__done!(|| (cc, self.uuid));
        Ok(())
    }

    async fn write(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError> {
        // Make use of the volume specific write_op method to avoid
        // code duplication with volume.write_unwritten.
        self.volume_write_op(offset, data, false).await
    }

    async fn write_unwritten(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError> {
        // Make use of the volume specific write_op method to avoid
        // code duplication with volume.write.
        self.volume_write_op(offset, data, true).await
    }

    async fn flush(
        &self,
        snapshot_details: Option<SnapshotDetails>,
    ) -> Result<(), CrucibleError> {
        let cc = self.next_count();
        cdt::volume__flush__start!(|| (cc, self.uuid));
        for sub_volume in &self.sub_volumes {
            sub_volume.flush(snapshot_details.clone()).await?;
        }

        // No need to flush read only parent. We assume that read only parents
        // are already consistent, because we can't write to them (they may be
        // served out of a ZFS snapshot and be read only at the filesystem
        // level)

        cdt::volume__flush__done!(|| (cc, self.uuid));
        Ok(())
    }

    async fn show_work(&self) -> Result<WQCounts, CrucibleError> {
        let mut wq_counts = WQCounts {
            up_count: 0,
            ds_count: 0,
        };

        for sub_volume in &self.sub_volumes {
            let sub_wq_counts = sub_volume.show_work().await?;

            wq_counts.up_count += sub_wq_counts.up_count;
            wq_counts.ds_count += sub_wq_counts.ds_count;
        }

        if let Some(ref read_only_parent) = &self.read_only_parent {
            let sub_wq_counts = read_only_parent.show_work().await?;

            wq_counts.up_count += sub_wq_counts.up_count;
            wq_counts.ds_count += sub_wq_counts.ds_count;
        }

        Ok(wq_counts)
    }
}

// Traditional subvolume is just one region set
impl SubVolume {
    // Compute sub volume LBA from total volume LBA.
    //
    // Total volume address:                    x
    //     Total volume LBA:   |----------------x------------------|
    //       Sub volume LBA:             |------x---------|
    //   Sub volume address:                    x
    //
    // For example, if total volume address is 1234, and this sub volume's range
    // is [1024,2048), then the sub volume address is
    //
    //     total address - sub volume start
    //     = 1234 - 1024
    //     = 210
    //
    pub fn compute_sub_volume_lba(&self, address: u64) -> u64 {
        assert!(self.lba_range.contains(&address));
        address - self.lba_range.start
    }

    pub fn lba_range_coverage(
        &self,
        start: u64,
        length: u64,
    ) -> Option<Range<u64>> {
        assert!(length >= 1);

        let end = start + length - 1;

        // No coverage:
        //
        // lba_range:                  |-------------|
        // argument range:  |-------|
        // argument range:                                  |--------|
        //

        if end < self.lba_range.start {
            return None;
        }

        if start >= self.lba_range.end {
            return None;
        }

        // Total coverage:
        //
        // lba_range:                  |-------------|
        // argument range:              |-------|
        // argument range:                 |--------|

        if self.lba_range.contains(&start) && self.lba_range.contains(&end) {
            return Some(start..(start + length));
        }

        // Partial coverage:

        if self.lba_range.contains(&start) {
            assert!(!self.lba_range.contains(&end));

            // lba_range:                  |-------------|
            // argument range:                         |--------|
            // coverage:                               ^^^

            Some(start..self.lba_range.end)
        } else if self.lba_range.contains(&end) {
            assert!(!self.lba_range.contains(&start));

            // lba_range:                  |-------------|
            // argument range:          |-------|
            // coverage:                   ^^^^^^
            Some(self.lba_range.start..(end + 1))
        } else if start < self.lba_range.start && end > self.lba_range.end {
            // lba_range:                  |-------------|
            // argument range:          |--------------------|
            // coverage:                   ^^^^^^^^^^^^^^^
            Some(self.lba_range.clone())
        } else {
            println!("{:?} {} {}", self.lba_range, start, length);
            panic!("should never get here!");
        }
    }
}

#[async_trait]
impl BlockIO for SubVolume {
    async fn activate(&self, gen: u64) -> Result<(), CrucibleError> {
        self.block_io.activate(gen).await
    }

    async fn deactivate(&self) -> Result<(), CrucibleError> {
        self.block_io.deactivate().await
    }

    async fn query_is_active(&self) -> Result<bool, CrucibleError> {
        self.block_io.query_is_active().await
    }

    async fn total_size(&self) -> Result<u64, CrucibleError> {
        self.block_io.total_size().await
    }

    async fn get_block_size(&self) -> Result<u64, CrucibleError> {
        self.block_io.get_block_size().await
    }

    async fn get_uuid(&self) -> Result<Uuid, CrucibleError> {
        self.block_io.get_uuid().await
    }

    async fn read(
        &self,
        offset: Block,
        data: Buffer,
    ) -> Result<(), CrucibleError> {
        self.block_io.read(offset, data).await
    }

    async fn write(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError> {
        self.block_io.write(offset, data).await
    }

    async fn write_unwritten(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<(), CrucibleError> {
        self.block_io.write_unwritten(offset, data).await
    }

    async fn flush(
        &self,
        snapshot_details: Option<SnapshotDetails>,
    ) -> Result<(), CrucibleError> {
        self.block_io.flush(snapshot_details).await
    }

    async fn show_work(&self) -> Result<WQCounts, CrucibleError> {
        self.block_io.show_work().await
    }
}

impl Volume {
    #[async_recursion]
    pub async fn construct(
        request: VolumeConstructionRequest,
        producer_registry: Option<ProducerRegistry>,
    ) -> Result<Volume> {
        match request {
            VolumeConstructionRequest::Volume {
                id,
                block_size,
                sub_volumes,
                read_only_parent,
            } => {
                let mut vol = Volume::new_with_id(block_size, id);

                for subreq in sub_volumes {
                    vol.add_subvolume(Arc::new(
                        Volume::construct(subreq, producer_registry.clone())
                            .await?,
                    ))
                    .await?;
                }

                if let Some(read_only_parent) = read_only_parent {
                    vol.add_read_only_parent(Arc::new(
                        Volume::construct(*read_only_parent, producer_registry)
                            .await?,
                    ))
                    .await?;
                }

                Ok(vol)
            }

            VolumeConstructionRequest::Url {
                id,
                block_size,
                url,
            } => {
                let mut vol = Volume::new(block_size);
                vol.add_subvolume(Arc::new(
                    ReqwestBlockIO::new(id, block_size, url).await?,
                ))
                .await?;
                Ok(vol)
            }

            VolumeConstructionRequest::Region {
                block_size,
                opts,
                gen,
            } => {
                let mut vol = Volume::new(block_size);
                vol.add_subvolume_create_guest(opts, gen, producer_registry)
                    .await?;
                Ok(vol)
            }

            VolumeConstructionRequest::File {
                id,
                block_size,
                path,
            } => {
                let mut vol = Volume::new(block_size);
                vol.add_subvolume(Arc::new(FileBlockIO::new(
                    id, block_size, path,
                )?))
                .await?;
                Ok(vol)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_single_block() -> Result<()> {
        let sub_volume = SubVolume {
            lba_range: 0..10,
            block_io: Arc::new(Guest::new()),
        };

        // Coverage inside region
        assert_eq!(sub_volume.lba_range_coverage(0, 1), Some(0..1));

        Ok(())
    }

    #[test]
    fn test_single_sub_volume_lba_coverage() -> Result<()> {
        let sub_volume = SubVolume {
            lba_range: 0..2048,
            block_io: Arc::new(Guest::new()),
        };

        // Coverage inside region
        assert_eq!(sub_volume.lba_range_coverage(0, 1), Some(0..1),);
        assert_eq!(sub_volume.lba_range_coverage(1, 4), Some(1..5),);
        assert_eq!(sub_volume.lba_range_coverage(512, 512), Some(512..1024),);
        assert_eq!(sub_volume.lba_range_coverage(0, 2048), Some(0..2048),);

        // Coverage over region
        assert_eq!(sub_volume.lba_range_coverage(1024, 4096), Some(1024..2048),);

        // Coverage outside region
        assert_eq!(sub_volume.lba_range_coverage(4096, 512), None,);

        Ok(())
    }

    #[test]
    fn test_single_sub_volume_lba_coverage_with_offset() -> Result<()> {
        let sub_volume = SubVolume {
            lba_range: 1024..2048,
            block_io: Arc::new(Guest::new()),
        };

        // No coverage before region
        assert_eq!(sub_volume.lba_range_coverage(0, 512), None,);
        assert_eq!(sub_volume.lba_range_coverage(0, 1024), None,);
        assert_eq!(sub_volume.lba_range_coverage(512, 512), None,);

        // Coverage from before to region
        assert_eq!(sub_volume.lba_range_coverage(512, 1024), Some(1024..1536),);
        assert_eq!(sub_volume.lba_range_coverage(512, 2048), Some(1024..2048),);
        assert_eq!(sub_volume.lba_range_coverage(512, 2560), Some(1024..2048),);

        // Coverage from inside region to beyond region
        assert_eq!(sub_volume.lba_range_coverage(1024, 4096), Some(1024..2048),);
        assert_eq!(sub_volume.lba_range_coverage(1536, 4096), Some(1536..2048),);
        assert_eq!(sub_volume.lba_range_coverage(2047, 4096), Some(2047..2048),);

        // Coverage outside region
        assert_eq!(sub_volume.lba_range_coverage(2048, 4096), None,);
        assert_eq!(sub_volume.lba_range_coverage(4096, 512), None,);

        Ok(())
    }

    #[tokio::test]
    async fn test_volume_size() -> Result<()> {
        let volume = Volume {
            uuid: Uuid::new_v4(),
            sub_volumes: vec![
                SubVolume {
                    lba_range: Range { start: 0, end: 512 },
                    block_io: Arc::new(InMemoryBlockIO::new(
                        Uuid::new_v4(),
                        512,
                        512 * 512,
                    )),
                },
                SubVolume {
                    lba_range: Range {
                        start: 512,
                        end: 1024,
                    },
                    block_io: Arc::new(InMemoryBlockIO::new(
                        Uuid::new_v4(),
                        512,
                        512 * 512,
                    )),
                },
            ],
            read_only_parent: None,
            scrub_point: Arc::new(AtomicU64::new(0)),
            block_size: 512,
            count: AtomicU32::new(0),
        };

        assert_eq!(volume.total_size().await?, 512 * 1024);

        Ok(())
    }

    #[test]
    fn test_affected_subvolumes() -> Result<()> {
        // volume:       |--------|--------|--------|
        //               0        512      1024
        //
        // each sub volume LBA coverage is 512 blocks
        let volume = Volume {
            uuid: Uuid::new_v4(),
            sub_volumes: vec![
                SubVolume {
                    lba_range: Range { start: 0, end: 512 },
                    block_io: Arc::new(InMemoryBlockIO::new(
                        Uuid::new_v4(),
                        512,
                        512 * 512,
                    )),
                },
                SubVolume {
                    lba_range: Range {
                        start: 512,
                        end: 1024,
                    },
                    block_io: Arc::new(InMemoryBlockIO::new(
                        Uuid::new_v4(),
                        512,
                        512 * 512,
                    )),
                },
                SubVolume {
                    lba_range: Range {
                        start: 1024,
                        end: 1536,
                    },
                    block_io: Arc::new(InMemoryBlockIO::new(
                        Uuid::new_v4(),
                        512,
                        512 * 512,
                    )),
                },
            ],
            read_only_parent: None,
            scrub_point: Arc::new(AtomicU64::new(0)),
            block_size: 512,
            count: AtomicU32::new(0),
        };

        // volume:       |--------|--------|--------|
        // request:      |----|
        assert_eq!(volume.sub_volumes_for_lba_range(0, 256).len(), 1,);

        // volume:       |--------|--------|--------|
        // request:      |--------|
        assert_eq!(volume.sub_volumes_for_lba_range(0, 512).len(), 1,);

        // volume:       |--------|--------|--------|
        // request:      |---------|
        assert_eq!(volume.sub_volumes_for_lba_range(0, 513).len(), 2);

        // volume:       |--------|--------|--------|
        // request:          |--|
        assert_eq!(volume.sub_volumes_for_lba_range(256, 16).len(), 1,);

        // volume:       |--------|--------|--------|
        // request:          |----|
        assert_eq!(volume.sub_volumes_for_lba_range(256, 256).len(), 1,);

        // volume:       |--------|--------|--------|
        // request:          |--------|
        assert_eq!(volume.sub_volumes_for_lba_range(256, 512).len(), 2,);

        // volume:       |--------|--------|--------|
        // request:          |-------------|
        assert_eq!(volume.sub_volumes_for_lba_range(256, 256 + 512).len(), 2,);

        // volume:       |--------|--------|--------|
        // request:          |------------------|
        assert_eq!(
            volume.sub_volumes_for_lba_range(256, 256 + 512 + 256).len(),
            3,
        );

        // volume:       |--------|--------|--------|
        // request:          |----------------------|
        assert_eq!(
            volume.sub_volumes_for_lba_range(256, 256 + 512 + 512).len(),
            3,
        );

        // volume:       |--------|--------|--------|
        // request:               |-----------------|
        assert_eq!(volume.sub_volumes_for_lba_range(512, 512 + 512).len(), 2,);

        Ok(())
    }

    #[test]
    fn test_no_read_only_parent_for_lba_range() -> Result<()> {
        // sub volume:  |-------------------|
        let volume = Volume {
            uuid: Uuid::new_v4(),
            sub_volumes: vec![SubVolume {
                lba_range: Range { start: 0, end: 512 },
                block_io: Arc::new(InMemoryBlockIO::new(
                    Uuid::new_v4(),
                    512,
                    512 * 512,
                )),
            }],
            read_only_parent: None,
            scrub_point: Arc::new(AtomicU64::new(0)),
            block_size: 512,
            count: AtomicU32::new(0),
        };

        assert!(volume.read_only_parent_for_lba_range(0, 512).is_none());

        Ok(())
    }

    #[test]
    fn test_read_only_parent_for_lba_range() -> Result<()> {
        // sub volume:  |-------------------|
        // parent:      |xxxxxxxxx|
        let volume = Volume {
            uuid: Uuid::new_v4(),
            sub_volumes: vec![SubVolume {
                lba_range: Range { start: 0, end: 512 },
                block_io: Arc::new(InMemoryBlockIO::new(
                    Uuid::new_v4(),
                    512,
                    512 * 512,
                )),
            }],
            read_only_parent: Some(Arc::new(SubVolume {
                lba_range: Range { start: 0, end: 256 },
                block_io: Arc::new(InMemoryBlockIO::new(
                    Uuid::new_v4(),
                    512,
                    256 * 512,
                )),
            })),
            scrub_point: Arc::new(AtomicU64::new(0)),
            block_size: 512,
            count: AtomicU32::new(0),
        };

        assert!(volume.read_only_parent_for_lba_range(0, 512).is_some());

        // Inside, starting at 0
        assert_eq!(volume.read_only_parent_for_lba_range(0, 128), Some(0..128),);
        assert_eq!(volume.read_only_parent_for_lba_range(0, 256), Some(0..256),);
        assert_eq!(volume.read_only_parent_for_lba_range(0, 512), Some(0..256),);

        // Inside, starting at offset
        assert_eq!(
            volume.read_only_parent_for_lba_range(128, 512),
            Some(128..256),
        );

        // Outside
        assert_eq!(volume.read_only_parent_for_lba_range(256, 512), None,);

        Ok(())
    }

    #[tokio::test]
    async fn test_in_memory_block_io() -> Result<()> {
        const BLOCK_SIZE: u64 = 512;
        let disk = InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 4096);

        // Initial read should come back all zeroes
        let buffer = Buffer::new(4096);
        disk.read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;
        assert_eq!(*buffer.as_vec().await, vec![0; 4096]);

        // Write ones to second block
        disk.write(
            Block::new(1, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![1; 512]),
        )
        .await?;

        // Write unwritten twos to the second block.
        // This should not change the block.
        disk.write_unwritten(
            Block::new(1, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![2; 512]),
        )
        .await?;

        // Read and verify the data is from the first write
        let buffer = Buffer::new(4096);
        disk.read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        let mut expected = vec![0; 512];
        expected.extend(vec![1; 512]);
        expected.extend(vec![0; 4096 - 1024]);
        assert_eq!(*buffer.as_vec().await, expected);

        // Write twos to first block
        disk.write(
            Block::new(0, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![2; 512]),
        )
        .await?;

        // Read and verify
        let buffer = Buffer::new(4096);
        disk.read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        let mut expected = vec![2; 512];
        expected.extend(vec![1; 512]);
        expected.extend(vec![0; 4096 - 1024]);
        assert_eq!(*buffer.as_vec().await, expected);

        // Write sevens to third and fourth blocks
        disk.write(
            Block::new(2, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![7; 1024]),
        )
        .await?;

        // Write_unwritten eights to fifth and six blocks
        disk.write_unwritten(
            Block::new(4, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![8; 1024]),
        )
        .await?;

        // Read and verify
        let buffer = Buffer::new(4096);
        disk.read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        let mut expected = vec![2; 512];
        expected.extend(vec![1; 512]);
        expected.extend(vec![7; 1024]);
        expected.extend(vec![8; 1024]);
        expected.extend(vec![0; 1024]);
        assert_eq!(*buffer.as_vec().await, expected);

        Ok(())
    }

    // Accept an initialization value so that we can test when the read only
    // parent is uninitialized, and is initialized with a value
    async fn test_parent_read_only_region(
        block_size: u64,
        parent: Arc<dyn BlockIO + Send + Sync>,
        mut volume: Volume,
        read_only_parent_init_value: u8,
    ) -> Result<()> {
        volume.activate(0).await?;
        assert_eq!(volume.get_block_size().await?, 512);
        assert_eq!(block_size, 512);
        assert_eq!(volume.total_size().await?, 4096);

        // Volume is set up like this:
        //
        // parent blocks: n n n n
        //    sub volume: 0 0 0 0 0 0 0 0
        //
        // where "n" is read_only_parent_init_value (or "-" if the read only
        // parent was uninitialized).
        //
        // blocks that have not been written to will be marked with "-" and will
        // assumed to be all zeros.

        // The first 2048b of the initial read should come only from the parent
        // (aka read_only_parent_init_value), and the second 2048b should come
        // from the subvolume(s) (aka be uninitialized).
        let buffer = Buffer::new(4096);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())
            .await?;

        let mut expected = vec![read_only_parent_init_value; 512 * 4];
        expected.extend(vec![0x00; 512 * 4]);

        assert_eq!(*buffer.as_vec().await, expected);

        // If the parent volume has data, it should be returned. Write ones to
        // the first block of the parent:
        //
        // parent blocks: 1 n n n
        //    sub volume: - - - - - - - -
        //
        parent
            .write(
                Block::new(0, block_size.trailing_zeros()),
                Bytes::from(vec![1; 512]),
            )
            .await?;

        // Read whole volume and verify
        let buffer = Buffer::new(4096);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())
            .await?;

        let mut expected = vec![1; 512];
        expected.extend(vec![read_only_parent_init_value; 512 * 3]);
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(*buffer.as_vec().await, expected);

        // If the volume is written to and it doesn't overlap, still return the
        // parent data. Write twos to the volume:
        //
        // parent blocks: 1 n n n
        //    sub volume: - 2 2 - - - - -
        volume
            .write(
                Block::new(1, block_size.trailing_zeros()),
                Bytes::from(vec![2; 1024]),
            )
            .await?;

        // Make sure the parent data hasn't changed!
        {
            let buffer = Buffer::new(2048);
            parent
                .read(
                    Block::new(0, block_size.trailing_zeros()),
                    buffer.clone(),
                )
                .await?;

            let mut expected = vec![1; 512];
            expected.extend(vec![read_only_parent_init_value; 2048 - 512]);

            assert_eq!(*buffer.as_vec().await, expected);
        }

        // Read whole volume and verify
        let buffer = Buffer::new(4096);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())
            .await?;

        let mut expected = vec![1; 512];
        expected.extend(vec![2; 512 * 2]);
        expected.extend(vec![read_only_parent_init_value; 512]);
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(*buffer.as_vec().await, expected);

        // If the volume is written to and it does overlap, return the volume
        // data. Write threes to the volume:
        //
        // parent blocks: 1 n n n
        //    sub volume: 3 3 2 - - - - -
        volume
            .write(
                Block::new(0, block_size.trailing_zeros()),
                Bytes::from(vec![3; 512 * 2]),
            )
            .await?;

        // Make sure the parent data hasn't changed!
        {
            let buffer = Buffer::new(2048);
            parent
                .read(
                    Block::new(0, block_size.trailing_zeros()),
                    buffer.clone(),
                )
                .await?;

            let mut expected = vec![1; 512];
            expected.extend(vec![read_only_parent_init_value; 2048 - 512]);

            assert_eq!(*buffer.as_vec().await, expected);
        }

        // Read whole volume and verify
        let buffer = Buffer::new(4096);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())
            .await?;

        let mut expected = vec![3; 512 * 2];
        expected.extend(vec![2; 512]);
        expected.extend(vec![read_only_parent_init_value; 512]);
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(*buffer.as_vec().await, expected);

        // If the whole parent is now written to, only the last block should be
        // returned. Write fours to the parent
        //
        // parent blocks: 4 4 4 4
        //    sub volume: 3 3 2 - - - - -
        parent
            .write(
                Block::new(0, block_size.trailing_zeros()),
                Bytes::from(vec![4; 2048]),
            )
            .await?;

        // Read whole volume and verify
        let buffer = Buffer::new(4096);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())
            .await?;

        let mut expected = vec![3; 512 * 2];
        expected.extend(vec![2; 512]);
        expected.extend(vec![4; 512]);
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(*buffer.as_vec().await, expected);

        // If the parent goes away, then the sub volume data should still be
        // readable.
        volume.read_only_parent = None;

        // Read whole volume and verify
        let buffer = Buffer::new(4096);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())
            .await?;

        let mut expected = vec![3; 512 * 2];
        expected.extend(vec![2; 512]);
        expected.extend(vec![0; 512]); // <- was previously from parent, now "-"
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(*buffer.as_vec().await, expected);

        // Write to the whole volume. There's no more read-only parent (it was
        // dropped above)
        volume
            .write(
                Block::new(0, block_size.trailing_zeros()),
                Bytes::from(vec![9; 4096]),
            )
            .await?;

        let buffer = Buffer::new(4096);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(*buffer.as_vec().await, vec![9; 4096]);

        Ok(())
    }

    #[tokio::test]
    async fn test_parent_initialized_read_only_region_one_subvolume(
    ) -> Result<()> {
        const BLOCK_SIZE: u64 = 512;

        // test initializing the read only parent with all byte values
        for i in 0x00..0xff {
            let parent = Arc::new(InMemoryBlockIO::new(
                Uuid::new_v4(),
                BLOCK_SIZE,
                2048,
            ));
            let disk = Arc::new(InMemoryBlockIO::new(
                Uuid::new_v4(),
                BLOCK_SIZE,
                4096,
            ));

            parent
                .write(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    Bytes::from(vec![i; 2048]),
                )
                .await?;

            // this layout has one volume that the parent lba range overlaps:
            //
            // volumes: 0 0 0 0 0 0 0 0
            //  parent: P P P P
            //
            // the total volume size is 4096b

            let mut volume = Volume::new(BLOCK_SIZE);
            volume.add_subvolume(disk).await?;
            volume.add_read_only_parent(parent.clone()).await?;

            test_parent_read_only_region(BLOCK_SIZE, parent, volume, i).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_parent_uninitialized_read_only_region_one_subvolume(
    ) -> Result<()> {
        const BLOCK_SIZE: u64 = 512;

        // test an uninitialized read only parent
        let parent =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));
        let disk =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 4096));

        // this layout has one volume that the parent lba range overlaps:
        //
        // volumes: 0 0 0 0 0 0 0 0
        //  parent: P P P P
        //
        // the total volume size is 4096b

        let mut volume = Volume::new(BLOCK_SIZE);
        volume.add_subvolume(disk).await?;
        volume.add_read_only_parent(parent.clone()).await?;

        test_parent_read_only_region(BLOCK_SIZE, parent, volume, 0x00).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_parent_initialized_read_only_region_with_multiple_sub_volumes_1(
    ) -> Result<()> {
        const BLOCK_SIZE: u64 = 512;

        for i in 0x00..0xFF {
            let parent = Arc::new(InMemoryBlockIO::new(
                Uuid::new_v4(),
                BLOCK_SIZE,
                2048,
            ));

            parent
                .write(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    Bytes::from(vec![i; 2048]),
                )
                .await?;

            let subdisk1 = Arc::new(InMemoryBlockIO::new(
                Uuid::new_v4(),
                BLOCK_SIZE,
                1024,
            ));
            let subdisk2 = Arc::new(InMemoryBlockIO::new(
                Uuid::new_v4(),
                BLOCK_SIZE,
                3072,
            ));

            // this layout has two volumes that the parent lba range overlaps:
            //
            // volumes: 0 0
            //              1 1 1 1 1 1
            //  parent: P P P P
            //
            // the total volume size is the same as the previous test: 4096b

            let mut volume = Volume::new(BLOCK_SIZE);
            volume.add_subvolume(subdisk1).await?;
            volume.add_subvolume(subdisk2).await?;
            volume.add_read_only_parent(parent.clone()).await?;

            test_parent_read_only_region(BLOCK_SIZE, parent, volume, i).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_parent_uninitialized_read_only_region_with_multiple_sub_volumes_1(
    ) -> Result<()> {
        const BLOCK_SIZE: u64 = 512;

        let parent =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));
        let subdisk1 =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 1024));
        let subdisk2 =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 3072));

        // this layout has two volumes that the parent lba range overlaps:
        //
        // volumes: 0 0
        //              1 1 1 1 1 1
        //  parent: P P P P
        //
        // the total volume size is the same as the previous test: 4096b

        let mut volume = Volume::new(BLOCK_SIZE);
        volume.add_subvolume(subdisk1).await?;
        volume.add_subvolume(subdisk2).await?;
        volume.add_read_only_parent(parent.clone()).await?;

        test_parent_read_only_region(BLOCK_SIZE, parent, volume, 0x00).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_parent_initialized_read_only_region_with_multiple_sub_volumes_2(
    ) -> Result<()> {
        const BLOCK_SIZE: u64 = 512;

        for i in 0x00..0xFF {
            let parent = Arc::new(InMemoryBlockIO::new(
                Uuid::new_v4(),
                BLOCK_SIZE,
                2048,
            ));

            parent
                .write(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    Bytes::from(vec![i; 2048]),
                )
                .await?;

            let subdisk1 = Arc::new(InMemoryBlockIO::new(
                Uuid::new_v4(),
                BLOCK_SIZE,
                2048,
            ));
            let subdisk2 = Arc::new(InMemoryBlockIO::new(
                Uuid::new_v4(),
                BLOCK_SIZE,
                2048,
            ));

            // this layout has two volumes that the parent lba range overlaps:
            //
            // volumes: 0 0 0 0
            //                  1 1 1 1
            //  parent: P P P P
            //
            // the total volume size is the same as the previous test: 4096b

            let mut volume = Volume::new(BLOCK_SIZE);
            volume.add_subvolume(subdisk1).await?;
            volume.add_subvolume(subdisk2).await?;
            volume.add_read_only_parent(parent.clone()).await?;

            test_parent_read_only_region(BLOCK_SIZE, parent, volume, i).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_parent_uninitialized_read_only_region_with_multiple_sub_volumes_2(
    ) -> Result<()> {
        const BLOCK_SIZE: u64 = 512;

        let parent =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));

        let subdisk1 =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));
        let subdisk2 =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));

        // this layout has two volumes that the parent lba range overlaps:
        //
        // volumes: 0 0 0 0
        //                  1 1 1 1
        //  parent: P P P P
        //
        // the total volume size is the same as the previous test: 4096b

        let mut volume = Volume::new(BLOCK_SIZE);
        volume.add_subvolume(subdisk1).await?;
        volume.add_subvolume(subdisk2).await?;
        volume.add_read_only_parent(parent.clone()).await?;

        test_parent_read_only_region(BLOCK_SIZE, parent, volume, 0x00).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_volume_with_only_read_only_parent() -> Result<()> {
        const BLOCK_SIZE: u64 = 512;

        let parent =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));

        parent.activate(0).await?;

        // Write 0x80 into parent
        parent
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![128; 2048]),
            )
            .await?;

        let volume = Volume {
            uuid: Uuid::new_v4(),
            sub_volumes: vec![],
            read_only_parent: Some(Arc::new(SubVolume {
                lba_range: Range {
                    start: 0,
                    end: parent.total_size().await? / BLOCK_SIZE as u64,
                },
                block_io: parent.clone(),
            })),
            scrub_point: Arc::new(AtomicU64::new(0)),
            block_size: BLOCK_SIZE,
            count: AtomicU32::new(0),
        };

        volume.activate(0).await?;

        assert_eq!(volume.total_size().await?, 2048);

        // Read volume and verify contents
        let buffer = Buffer::new(2048);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        let expected = vec![128; 2048];

        assert_eq!(*buffer.as_vec().await, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_writing_to_volume_with_only_read_only_parent() {
        const BLOCK_SIZE: u64 = 512;

        let parent =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));

        let volume = Volume {
            uuid: Uuid::new_v4(),
            sub_volumes: vec![],
            read_only_parent: Some(Arc::new(SubVolume {
                lba_range: Range {
                    start: 0,
                    end: parent.total_size().await.unwrap() / BLOCK_SIZE as u64,
                },
                block_io: parent.clone(),
            })),
            scrub_point: Arc::new(AtomicU64::new(0)),
            block_size: BLOCK_SIZE,
            count: AtomicU32::new(0),
        };

        volume.activate(0).await.unwrap();

        // Write 0x80 into volume - this will error
        let res = volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![128; 2048]),
            )
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_write_unwritten_to_volume_with_only_read_only_parent() {
        const BLOCK_SIZE: u64 = 512;

        let parent =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));

        let volume = Volume {
            uuid: Uuid::new_v4(),
            sub_volumes: vec![],
            read_only_parent: Some(Arc::new(SubVolume {
                lba_range: Range {
                    start: 0,
                    end: parent.total_size().await.unwrap() / BLOCK_SIZE as u64,
                },
                block_io: parent.clone(),
            })),
            scrub_point: Arc::new(AtomicU64::new(0)),
            block_size: BLOCK_SIZE,
            count: AtomicU32::new(0),
        };

        volume.activate(0).await.unwrap();

        // Write 0x80 into volume - this will error
        let res = volume
            .write_unwritten(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![128; 2048]),
            )
            .await;

        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_drop_then_recreate_test() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // Write 0x55 into parent
        let parent = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 10,
        ));
        parent
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 10]),
            )
            .await?;

        let overlay = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 10,
        ));

        {
            // Make a volume, verify orignal data, write new data to it, then
            // let it fall out of scope
            let mut volume = Volume::new(BLOCK_SIZE as u64);
            volume.add_subvolume(overlay.clone()).await?;
            volume.add_read_only_parent(parent.clone()).await?;

            let buffer = Buffer::new(BLOCK_SIZE * 10);
            volume
                .read(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    buffer.clone(),
                )
                .await?;

            assert_eq!(vec![0x55; BLOCK_SIZE * 10], *buffer.as_vec().await);

            volume
                .write(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    Bytes::from(vec![0xFF; BLOCK_SIZE * 10]),
                )
                .await?;

            let buffer = Buffer::new(BLOCK_SIZE * 10);
            volume
                .read(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    buffer.clone(),
                )
                .await?;

            assert_eq!(vec![0xFF; BLOCK_SIZE * 10], *buffer.as_vec().await);
        }

        // Create the same volume, verify data was written
        // Note that add function order is reversed, it shouldn't matter
        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume.add_read_only_parent(parent).await?;
        volume.add_subvolume(overlay).await?;

        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0xFF; BLOCK_SIZE * 10], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
    async fn test_three_layers() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // Write 0x55 into parent
        let parent = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 10,
        ));
        parent
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0x55; BLOCK_SIZE * 10]),
            )
            .await?;

        // Read parent, verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        parent
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x55; BLOCK_SIZE * 10], *buffer.as_vec().await);

        let mut parent_volume = Volume::new(BLOCK_SIZE as u64);
        parent_volume.add_subvolume(parent).await?;

        let overlay = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 10,
        ));

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume.add_subvolume(overlay).await?;
        volume.add_read_only_parent(Arc::new(parent_volume)).await?;

        // Now:
        //
        // Volume {
        //   subvolumes: [
        //     overlay
        //   ]
        //   read_only_parent: Volume {
        //     subvolumes: [
        //       parent
        //    ]
        //   }
        // }

        // Read whole volume, verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0x55; BLOCK_SIZE * 10], *buffer.as_vec().await);

        // Write over whole volume
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0xFF; BLOCK_SIZE * 10]),
            )
            .await?;

        // Read whole volume, verify new contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![0xFF; BLOCK_SIZE * 10], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
    async fn construct_file_block_io() {
        const BLOCK_SIZE: usize = 512;

        let dir = tempdir().unwrap();
        let file_path = dir.path().join("disk.raw");

        let mut file = File::create(&file_path).unwrap();
        file.write_all(&vec![0u8; 512]).unwrap();
        file.write_all(&vec![5u8; 512]).unwrap();

        let request = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: 512,
            sub_volumes: vec![],
            read_only_parent: Some(Box::new(VolumeConstructionRequest::File {
                id: Uuid::new_v4(),
                block_size: 512,
                path: file_path.into_os_string().into_string().unwrap(),
            })),
        };
        let volume = Volume::construct(request, None).await.unwrap();

        let buffer = Buffer::new(BLOCK_SIZE);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await
            .unwrap();

        assert_eq!(vec![0x0; BLOCK_SIZE], *buffer.as_vec().await);

        let buffer = Buffer::new(BLOCK_SIZE);
        volume
            .read(Block::new(1, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .await
            .unwrap();

        assert_eq!(vec![0x5; BLOCK_SIZE], *buffer.as_vec().await);
    }

    // Test that blocks are correctly returned during read-only parent +
    // subvolume overlap.
    async fn test_correct_blocks_returned(
        block_size: usize,
        subvolumes: &[Arc<dyn BlockIO + Send + Sync>],
    ) -> Result<()> {
        // Create read only parent
        let parent = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            block_size as u64,
            block_size * 5,
        ));

        // Fill the parent with 1s
        parent
            .write(
                Block::new(0, block_size.trailing_zeros()),
                Bytes::from(vec![11; block_size * 5]),
            )
            .await?;

        // Read back parent, verify 1s
        let buffer = Buffer::new(block_size * 5);
        parent
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![11; block_size * 5], *buffer.as_vec().await);

        // Create a volume out of this parent and the argument subvolume parts
        let mut volume = Volume::new(block_size as u64);

        for subvolume in subvolumes {
            volume.add_subvolume(subvolume.clone()).await?;
        }

        volume.add_read_only_parent(parent).await?;

        volume.activate(0).await?;

        assert_eq!(volume.total_size().await?, block_size as u64 * 10);

        // Verify parent contents in one read
        let buffer = Buffer::new(block_size * 10);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())
            .await?;

        let mut expected = vec![11; block_size * 5];
        expected.extend(vec![0x00; block_size * 5]);
        assert_eq!(expected, *buffer.as_vec().await);

        // One big write!
        volume
            .write(
                Block::new(0, block_size.trailing_zeros()),
                Bytes::from(vec![55; block_size * 10]),
            )
            .await?;

        // Verify volume contents in one read
        let buffer = Buffer::new(block_size * 10);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())
            .await?;

        assert_eq!(vec![55; block_size * 10], *buffer.as_vec().await);

        Ok(())
    }

    #[tokio::test]
    async fn test_correct_blocks_returned_one_subvolume() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // this layout has one volume that the parent lba range overlaps:
        //
        // volumes: 0 0 0 0 0 0 0 0 0 0
        //  parent: P P P P P
        let subvolume = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 10,
        ));

        test_correct_blocks_returned(BLOCK_SIZE, &[subvolume]).await
    }

    #[tokio::test]
    async fn test_correct_blocks_returned_multiple_subvolumes_1() -> Result<()>
    {
        const BLOCK_SIZE: usize = 512;

        // this layout has two volumes that the parent lba range overlaps:
        //
        // volumes: 0 0
        //              1 1 1 1 1 1 1 1
        //  parent: P P P P P
        let subvolume_1 = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 2,
        ));
        let subvolume_2 = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 8,
        ));

        test_correct_blocks_returned(BLOCK_SIZE, &[subvolume_1, subvolume_2])
            .await
    }

    #[tokio::test]
    async fn test_correct_blocks_returned_multiple_subvolumes_2() -> Result<()>
    {
        const BLOCK_SIZE: usize = 512;

        // this layout has two volumes that the parent lba range overlaps:
        //
        // volumes: 0 0 0 0 0
        //                    1 1 1 1 1
        //  parent: P P P P P
        let subvolume_1 = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 5,
        ));
        let subvolume_2 = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 5,
        ));

        test_correct_blocks_returned(BLOCK_SIZE, &[subvolume_1, subvolume_2])
            .await
    }

    #[tokio::test]
    async fn test_correct_blocks_returned_multiple_subvolumes_3() -> Result<()>
    {
        const BLOCK_SIZE: usize = 512;

        // this layout has two volumes that the parent lba range overlaps:
        //
        // volumes: 0 0 0 0 0 0 0
        //                        1 1 1
        //  parent: P P P P P
        let subvolume_1 = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 7,
        ));
        let subvolume_2 = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 3,
        ));

        test_correct_blocks_returned(BLOCK_SIZE, &[subvolume_1, subvolume_2])
            .await
    }

    #[tokio::test]
    async fn test_correct_blocks_returned_three_subvolumes() -> Result<()> {
        const BLOCK_SIZE: usize = 512;

        // this layout has three volumes that the parent lba range overlaps:
        //
        // volumes: 0 0 0
        //                1 1 1
        //                      2 2 2 2
        //  parent: P P P P P
        let subvolume_1 = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 3,
        ));
        let subvolume_2 = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 3,
        ));
        let subvolume_3 = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 4,
        ));

        test_correct_blocks_returned(
            BLOCK_SIZE,
            &[subvolume_1, subvolume_2, subvolume_3],
        )
        .await
    }

    #[tokio::test]
    async fn test_scrub_point_subvolume_equal() -> Result<()> {
        // Test that scrub high water mark is honored.
        let block_size: usize = 512;

        // One volume with full read only parent overlap
        //
        // volumes: 0 0 0 0 0 0 0 0 0 0
        //  parent: P P P P P P P P P P

        test_volume_scrub_point_lba_range(block_size, 10, &[10]).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_scrub_point_subvolume_smaller() -> Result<()> {
        // Test that scrub high water mark is honored.
        let block_size: usize = 512;

        // One volume with partial read only parent overlap
        //
        // volumes: 0 0 0 0 0 0 0 0 0 0
        //  parent: P P P P P

        test_volume_scrub_point_lba_range(block_size, 5, &[10]).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_scrub_point_two_subvolume_smaller_1() -> Result<()> {
        // Test that scrub high water mark is honored.
        let block_size: usize = 512;

        // Two volumes with partial read only parent overlap
        //
        // volume1: 0 0 0 0 0 0 0 0 0 0
        // volume2:                     0 0 0 0 0
        // parent: P P P P P
        test_volume_scrub_point_lba_range(block_size, 5, &[10, 5]).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_scrub_point_two_subvolume_smaller_2() -> Result<()> {
        // Test that scrub high water mark is honored.
        let block_size: usize = 512;

        // Two volumes with partial read only parent overlap
        //
        // volume1: 0 0 0 0 0
        // volume2:           0 0 0 0 0
        // parent:  P P P P P
        test_volume_scrub_point_lba_range(block_size, 5, &[5, 5]).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_scrub_point_two_subvolume_smaller_3() -> Result<()> {
        // Test that scrub high water mark is honored.
        let block_size: usize = 512;

        // Two volumes with partial read only parent overlap
        //
        // volume1: 0 0 0 0 0
        // volume2:           0 0 0 0 0
        // parent:  P P P P P P P P
        test_volume_scrub_point_lba_range(block_size, 8, &[5, 5]).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_scrub_point_two_subvolume_equal() -> Result<()> {
        // Test that scrub high water mark is honored.
        let block_size: usize = 512;

        // Two volumes with full read only parent overlap
        //
        // volume1: 0 0 0 0 0
        // volume2:           0 0 0 0 0
        // parent:  P P P P P P P P P P
        test_volume_scrub_point_lba_range(block_size, 8, &[5, 5]).await?;

        Ok(())
    }

    // This test accepts a vec of sub-volume sizes, and a size for the read
    // only parent.  A volume is created using these inputs.  The test will
    // then walk all possible scrub points and IO sizes to test that the
    // correct LBA range is returned.
    //
    // All the possible IO offsets, sizes, and scrub points are checked.
    async fn test_volume_scrub_point_lba_range(
        block_size: usize,
        parent_blocks: usize,
        subvol_sizes: &[usize],
    ) -> Result<()> {
        // Create a volume
        let mut volume = Volume::new(block_size as u64);

        // Create the subvolume(s) of the requested size(s)
        for size in subvol_sizes {
            let subvolume = Arc::new(InMemoryBlockIO::new(
                Uuid::new_v4(),
                block_size as u64,
                block_size * size,
            ));
            volume.add_subvolume(subvolume.clone()).await?;
        }

        // Create the read only parent
        let parent = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            block_size as u64,
            block_size * parent_blocks,
        ));

        volume.add_read_only_parent(parent.clone()).await?;
        volume.activate(0).await?;

        // The total blocks in our volume
        let volume_blocks = volume.total_size().await? / block_size as u64;

        // Walk all the possible sizes (in blocks) for an IO.
        for size in 1..volume_blocks {
            // Walk the possible scrub points across the parent volume.
            for scrub_point in 0..=parent_blocks {
                volume
                    .scrub_point
                    .store(scrub_point as u64, Ordering::SeqCst);

                // Now walk the block offsets for the volume, verify the
                // returned lba_range is correct.
                //
                // Because we are testing all possible IO sizes, we only
                // need to walk the blocks up to the point where our
                // offset+size reaches the end of the volume.
                let final_volume_offset = volume_blocks - size;
                for block in 0..=final_volume_offset {
                    let r = volume.read_only_parent_for_lba_range(block, size);
                    if block >= parent_blocks as u64 {
                        // The block is past the end of the parent, this will
                        // not have a read_only_parent irregardless of the
                        // current scrub point location.
                        println!(
                            "block {} > parent {}. Go to SubVolume",
                            block, parent_blocks
                        );
                        assert!(r.is_none());
                    } else if block + size <= scrub_point as u64 {
                        // Our IO is fully under the scrub point, we should
                        // go directly to the SubVolume
                        println!(
                            "block {}+{} <= scrub_point {}. No parent check",
                            block, size, scrub_point,
                        );
                        assert!(r.is_none());
                    } else {
                        // Our IO is not fully under the scrub point, but we
                        // know it's still below the
                        // size of the parent, so we have
                        // to check.
                        println!(
                            "block {} < scrub_point {}. Check with your parent",
                            block, scrub_point,
                        );
                        assert!(r.is_some());
                    }
                }
            }
        }

        Ok(())
    }
}
