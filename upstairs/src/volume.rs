// Copyright 2021 Oxide Computer Company

use super::*;
use oximeter::types::ProducerRegistry;
use std::ops::Range;
use std::sync::atomic::{AtomicU32, Ordering};

use crucible_client_types::VolumeConstructionRequest;

#[derive(Debug)]
pub struct Volume {
    uuid: Uuid,

    sub_volumes: Vec<SubVolume>,
    read_only_parent: Option<SubVolume>,

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

    // Create a simple Volume from a single guest
    pub fn from_guest(guest: Arc<Guest>) -> Result<Volume, CrucibleError> {
        let block_size = guest.query_block_size()?;
        let uuid = guest.query_upstairs_uuid()?;

        let sub_volume = SubVolume {
            lba_range: Range {
                start: 0,
                end: guest.query_total_size()? / block_size,
            },
            block_io: guest,
        };

        Ok(Self {
            uuid,
            sub_volumes: vec![sub_volume],
            read_only_parent: None,
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

    pub fn add_subvolume(
        &mut self,
        block_io: Arc<dyn BlockIO + Send + Sync>,
    ) -> Result<(), CrucibleError> {
        let block_size = block_io.get_block_size()?;

        if block_size != self.block_size {
            crucible_bail!(BlockSizeMismatch);
        }

        let number_of_blocks = block_io.total_size()? / block_size;

        self.sub_volumes.push(SubVolume {
            lba_range: self.compute_next_lba_range(number_of_blocks),
            block_io,
        });

        Ok(())
    }

    pub fn add_subvolume_create_guest(
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

        guest.activate(gen)?;

        self.add_subvolume(guest)
    }

    // Add a potential "parent" source for blocks.
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
    pub fn add_read_only_parent(
        &mut self,
        block_io: Arc<dyn BlockIO + Send + Sync>,
    ) -> Result<(), CrucibleError> {
        let block_size = block_io.get_block_size()?;

        if block_size != self.block_size {
            crucible_bail!(BlockSizeMismatch);
        }

        let number_of_blocks = block_io.total_size()? / block_size;

        // TODO migration task to pull blocks in from parent, then set parent to
        // None when done - read block by block, don't overwrite if owned by sub
        // volume

        self.read_only_parent = Some(SubVolume {
            // Read only parent LBA range always starts from 0
            lba_range: Range {
                start: 0,
                end: number_of_blocks,
            },
            block_io,
        });

        Ok(())
    }

    // XXX When/Where is this used?
    pub fn add_read_only_parent_create_guest(
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

        guest.activate(gen)?;

        self.add_read_only_parent(guest)
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
        // Does the read only parent apply for this range
        if let Some(ref read_only_parent) = self.read_only_parent {
            read_only_parent.lba_range_coverage(start, length)
        } else {
            None
        }
    }

    // If a volume has a read only parent, do the work to read from
    // it and write_unwritten to the LBA it covers.
    pub fn scrub(&self) -> Result<(), CrucibleError> {
        println!("Scrub check for {}", self.uuid);
        // XXX Can we assert volume is activated?

        if let Some(ref read_only_parent) = self.read_only_parent {
            let ts = read_only_parent.total_size()?;
            let bs = read_only_parent.get_block_size()? as usize;

            println!("Scrub for total_size:{:?} block_size:{:?}", ts, bs);
            let start = read_only_parent.lba_range.start;
            let end = read_only_parent.lba_range.end;
            println!("Scrub will go from {:?} to {:?}", start, end);

            for offset in start..end {
                let block = Block::new(offset, bs.trailing_zeros());
                let buffer = Buffer::new(bs);

                let mut waiter =
                    read_only_parent.read(block, buffer.clone())?;
                waiter.block_wait()?;

                let mut waiter = self.write_unwritten(
                    Block::new(offset, bs.trailing_zeros()),
                    Bytes::from(buffer.as_vec().clone()),
                )?;
                waiter.block_wait()?;
            }
            println!("Scrub for {} has completed", self.uuid);
        }
        Ok(())
    }

    // This method is called by both write and write_unwritten and
    // provides a single place so both can share common code.
    fn volume_write_op(
        &self,
        offset: Block,
        data: Bytes,
        is_write_unwritten: bool,
    ) -> Result<BlockReqWaiter, CrucibleError> {
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
            let mut waiter = if is_write_unwritten {
                sub_volume.write_unwritten(sub_offset, slice.clone())?
            } else {
                sub_volume.write(sub_offset, slice.clone())?
            };

            waiter.block_wait()?;

            data_index += sz;
        }

        if is_write_unwritten {
            cdt::volume__writeunwritten__done!(|| (cc, self.uuid));
        } else {
            cdt::volume__write__done!(|| (cc, self.uuid));
        }
        BlockReqWaiter::immediate()
    }
}

impl BlockIO for Volume {
    fn activate(&self, gen: u64) -> Result<(), CrucibleError> {
        for sub_volume in &self.sub_volumes {
            sub_volume.conditional_activate(gen)?;

            let sub_volume_computed_size = self.block_size
                * (sub_volume.lba_range.end - sub_volume.lba_range.start);

            if sub_volume.total_size()? != sub_volume_computed_size {
                crucible_bail!(SubvolumeSizeMismatch);
            }
        }

        if let Some(ref read_only_parent) = &self.read_only_parent {
            read_only_parent.conditional_activate(gen)?;
            // TODO: Scrubber starts here?  Maybe?
        }

        Ok(())
    }

    fn query_is_active(&self) -> Result<bool, CrucibleError> {
        for sub_volume in &self.sub_volumes {
            if !sub_volume.query_is_active()? {
                return Ok(false);
            }
        }

        if let Some(ref read_only_parent) = &self.read_only_parent {
            if !read_only_parent.query_is_active()? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn total_size(&self) -> Result<u64, CrucibleError> {
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

    fn get_block_size(&self) -> Result<u64, CrucibleError> {
        Ok(self.block_size)
    }

    fn get_uuid(&self) -> Result<Uuid, CrucibleError> {
        Ok(self.uuid)
    }

    fn read(
        &self,
        offset: Block,
        data: Buffer,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        // In the case that this volume only has a read only parent, serve
        // reads directly from that
        let cc = self.next_count();
        cdt::volume__read__start!(|| (cc, self.uuid));
        if self.sub_volumes.is_empty() {
            if let Some(ref read_only_parent) = &self.read_only_parent {
                let res = read_only_parent.read(offset, data);
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
            data.len() as u64 / self.block_size,
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

            let mut waiter = sub_volume.read(sub_offset, sub_buffer.clone())?;
            waiter.block_wait()?;

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
                let mut waiter = self
                    .read_only_parent
                    .as_ref()
                    .unwrap()
                    .read(parent_offset, parent_buffer.clone())?;
                waiter.block_wait()?;

                let mut data_vec = data.as_vec();
                let mut owned_vec = data.owned_vec();

                let sub_data_vec = sub_buffer.as_vec();
                let sub_owned_vec = sub_buffer.owned_vec();
                let parent_data_vec = parent_buffer.as_vec();

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
                let mut data_vec = data.as_vec();
                let mut owned_vec = data.owned_vec();

                // In the case where this volume has no read only parent,
                // propagate the sub volume information up.
                data_vec[data_index..(data_index + sz)]
                    .copy_from_slice(&sub_buffer.as_vec());
                owned_vec[data_index..(data_index + sz)]
                    .copy_from_slice(&sub_buffer.owned_vec());
            }

            data_index += sz;
        }

        assert_eq!(data.len(), data_index);

        cdt::volume__read__done!(|| (cc, self.uuid));
        BlockReqWaiter::immediate()
    }

    fn write(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        // Make use of the volume specific write_op method to avoid
        // code duplication with volume.write_unwritten.
        self.volume_write_op(offset, data, false)
    }

    fn write_unwritten(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        // Make use of the volume specific write_op method to avoid
        // code duplication with volume.write.
        self.volume_write_op(offset, data, true)
    }

    fn flush(
        &self,
        snapshot_details: Option<SnapshotDetails>,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        let cc = self.next_count();
        cdt::volume__flush__start!(|| (cc, self.uuid));
        for sub_volume in &self.sub_volumes {
            let mut waiter = sub_volume.flush(snapshot_details.clone())?;
            waiter.block_wait()?;
        }

        // No need to flush read only parent. We assume that read only parents
        // are already consistent, because we can't write to them (they may be
        // served out of a ZFS snapshot and be read only at the filesystem
        // level)

        cdt::volume__flush__done!(|| (cc, self.uuid));
        BlockReqWaiter::immediate()
    }

    fn show_work(&self) -> Result<WQCounts, CrucibleError> {
        let mut wq_counts = WQCounts {
            up_count: 0,
            ds_count: 0,
        };

        for sub_volume in &self.sub_volumes {
            let sub_wq_counts = sub_volume.show_work()?;

            wq_counts.up_count += sub_wq_counts.up_count;
            wq_counts.ds_count += sub_wq_counts.ds_count;
        }

        if let Some(ref read_only_parent) = &self.read_only_parent {
            let sub_wq_counts = read_only_parent.show_work()?;

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

impl BlockIO for SubVolume {
    fn activate(&self, gen: u64) -> Result<(), CrucibleError> {
        self.block_io.activate(gen)
    }

    fn query_is_active(&self) -> Result<bool, CrucibleError> {
        self.block_io.query_is_active()
    }

    fn total_size(&self) -> Result<u64, CrucibleError> {
        self.block_io.total_size()
    }

    fn get_block_size(&self) -> Result<u64, CrucibleError> {
        self.block_io.get_block_size()
    }

    fn get_uuid(&self) -> Result<Uuid, CrucibleError> {
        self.block_io.get_uuid()
    }

    fn read(
        &self,
        offset: Block,
        data: Buffer,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        self.block_io.read(offset, data)
    }

    fn write(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        self.block_io.write(offset, data)
    }

    fn write_unwritten(
        &self,
        offset: Block,
        data: Bytes,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        self.block_io.write_unwritten(offset, data)
    }

    fn flush(
        &self,
        snapshot_details: Option<SnapshotDetails>,
    ) -> Result<BlockReqWaiter, CrucibleError> {
        self.block_io.flush(snapshot_details)
    }

    fn show_work(&self) -> Result<WQCounts, CrucibleError> {
        self.block_io.show_work()
    }
}

impl Volume {
    pub fn construct(
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
                    vol.add_subvolume(Arc::new(Volume::construct(
                        subreq,
                        producer_registry.clone(),
                    )?))?;
                }

                if let Some(read_only_parent) = read_only_parent {
                    vol.add_read_only_parent(Arc::new(Volume::construct(
                        *read_only_parent,
                        producer_registry,
                    )?))?;
                }

                Ok(vol)
            }

            VolumeConstructionRequest::Url {
                id,
                block_size,
                url,
            } => {
                let mut vol = Volume::new(block_size);
                vol.add_subvolume(Arc::new(ReqwestBlockIO::new(
                    id, block_size, url,
                )?))?;
                Ok(vol)
            }

            VolumeConstructionRequest::Region {
                block_size,
                opts,
                gen,
            } => {
                let mut vol = Volume::new(block_size);
                vol.add_subvolume_create_guest(opts, gen, producer_registry)?;
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
                )?))?;
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

    #[test]
    fn test_volume_size() -> Result<()> {
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
            block_size: 512,
            count: AtomicU32::new(0),
        };

        assert_eq!(volume.total_size()?, 512 * 1024);

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
            read_only_parent: Some(SubVolume {
                lba_range: Range { start: 0, end: 256 },
                block_io: Arc::new(InMemoryBlockIO::new(
                    Uuid::new_v4(),
                    512,
                    256 * 512,
                )),
            }),
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

    #[test]
    fn test_in_memory_block_io() -> Result<()> {
        const BLOCK_SIZE: u64 = 512;
        let disk = InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 4096);

        // Initial read should come back all zeroes
        let buffer = Buffer::new(4096);
        disk.read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;
        assert_eq!(*buffer.as_vec(), vec![0; 4096]);

        // Write ones to second block
        disk.write(
            Block::new(1, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![1; 512]),
        )?
        .block_wait()?;

        // Write unwritten twos to the second block.
        // This should not change the block.
        disk.write_unwritten(
            Block::new(1, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![2; 512]),
        )?
        .block_wait()?;

        // Read and verify the data is from the first write
        let buffer = Buffer::new(4096);
        disk.read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        let mut expected = vec![0; 512];
        expected.extend(vec![1; 512]);
        expected.extend(vec![0; 4096 - 1024]);
        assert_eq!(*buffer.as_vec(), expected);

        // Write twos to first block
        disk.write(
            Block::new(0, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![2; 512]),
        )?
        .block_wait()?;

        // Read and verify
        let buffer = Buffer::new(4096);
        disk.read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        let mut expected = vec![2; 512];
        expected.extend(vec![1; 512]);
        expected.extend(vec![0; 4096 - 1024]);
        assert_eq!(*buffer.as_vec(), expected);

        // Write sevens to third and fourth blocks
        disk.write(
            Block::new(2, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![7; 1024]),
        )?
        .block_wait()?;

        // Write_unwritten eights to fifth and six blocks
        disk.write_unwritten(
            Block::new(4, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![8; 1024]),
        )?
        .block_wait()?;

        // Read and verify
        let buffer = Buffer::new(4096);
        disk.read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        let mut expected = vec![2; 512];
        expected.extend(vec![1; 512]);
        expected.extend(vec![7; 1024]);
        expected.extend(vec![8; 1024]);
        expected.extend(vec![0; 1024]);
        assert_eq!(*buffer.as_vec(), expected);

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
        volume.activate(0)?;
        assert_eq!(volume.get_block_size()?, 512);
        assert_eq!(block_size, 512);
        assert_eq!(volume.total_size()?, 4096);

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
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        let mut expected = vec![read_only_parent_init_value; 512 * 4];
        expected.extend(vec![0x00; 512 * 4]);

        assert_eq!(*buffer.as_vec(), expected);

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
            )?
            .block_wait()?;

        // Read whole volume and verify
        let buffer = Buffer::new(4096);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        let mut expected = vec![1; 512];
        expected.extend(vec![read_only_parent_init_value; 512 * 3]);
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(*buffer.as_vec(), expected);

        // If the volume is written to and it doesn't overlap, still return the
        // parent data. Write twos to the volume:
        //
        // parent blocks: 1 n n n
        //    sub volume: - 2 2 - - - - -
        volume
            .write(
                Block::new(1, block_size.trailing_zeros()),
                Bytes::from(vec![2; 1024]),
            )?
            .block_wait()?;

        // Make sure the parent data hasn't changed!
        {
            let buffer = Buffer::new(2048);
            parent
                .read(
                    Block::new(0, block_size.trailing_zeros()),
                    buffer.clone(),
                )?
                .block_wait()?;

            let mut expected = vec![1; 512];
            expected.extend(vec![read_only_parent_init_value; 2048 - 512]);

            assert_eq!(*buffer.as_vec(), expected);
        }

        // Read whole volume and verify
        let buffer = Buffer::new(4096);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        let mut expected = vec![1; 512];
        expected.extend(vec![2; 512 * 2]);
        expected.extend(vec![read_only_parent_init_value; 512]);
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(*buffer.as_vec(), expected);

        // If the volume is written to and it does overlap, return the volume
        // data. Write threes to the volume:
        //
        // parent blocks: 1 n n n
        //    sub volume: 3 3 2 - - - - -
        volume
            .write(
                Block::new(0, block_size.trailing_zeros()),
                Bytes::from(vec![3; 512 * 2]),
            )?
            .block_wait()?;

        // Make sure the parent data hasn't changed!
        {
            let buffer = Buffer::new(2048);
            parent
                .read(
                    Block::new(0, block_size.trailing_zeros()),
                    buffer.clone(),
                )?
                .block_wait()?;

            let mut expected = vec![1; 512];
            expected.extend(vec![read_only_parent_init_value; 2048 - 512]);

            assert_eq!(*buffer.as_vec(), expected);
        }

        // Read whole volume and verify
        let buffer = Buffer::new(4096);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        let mut expected = vec![3; 512 * 2];
        expected.extend(vec![2; 512]);
        expected.extend(vec![read_only_parent_init_value; 512]);
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(*buffer.as_vec(), expected);

        // If the whole parent is now written to, only the last block should be
        // returned. Write fours to the parent
        //
        // parent blocks: 4 4 4 4
        //    sub volume: 3 3 2 - - - - -
        parent
            .write(
                Block::new(0, block_size.trailing_zeros()),
                Bytes::from(vec![4; 2048]),
            )?
            .block_wait()?;

        // Read whole volume and verify
        let buffer = Buffer::new(4096);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        let mut expected = vec![3; 512 * 2];
        expected.extend(vec![2; 512]);
        expected.extend(vec![4; 512]);
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(*buffer.as_vec(), expected);

        // If the parent goes away, then the sub volume data should still be
        // readable.
        volume.read_only_parent = None;

        // Read whole volume and verify
        let buffer = Buffer::new(4096);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        let mut expected = vec![3; 512 * 2];
        expected.extend(vec![2; 512]);
        expected.extend(vec![0; 512]); // <- was previously from parent, now "-"
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(*buffer.as_vec(), expected);

        // Write to the whole volume. There's no more read-only parent (it was
        // dropped above)
        volume
            .write(
                Block::new(0, block_size.trailing_zeros()),
                Bytes::from(vec![9; 4096]),
            )?
            .block_wait()?;

        let buffer = Buffer::new(4096);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(*buffer.as_vec(), vec![9; 4096]);

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
                )?
                .block_wait()?;

            // this layout has one volume that the parent lba range overlaps:
            //
            // volumes: 0 0 0 0 0 0 0 0
            //  parent: P P P P
            //
            // the total volume size is 4096b

            let mut volume = Volume::new(BLOCK_SIZE);
            volume.add_subvolume(disk)?;
            volume.add_read_only_parent(parent.clone())?;

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
        volume.add_subvolume(disk)?;
        volume.add_read_only_parent(parent.clone())?;

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
                )?
                .block_wait()?;

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
            volume.add_subvolume(subdisk1)?;
            volume.add_subvolume(subdisk2)?;
            volume.add_read_only_parent(parent.clone())?;

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
        volume.add_subvolume(subdisk1)?;
        volume.add_subvolume(subdisk2)?;
        volume.add_read_only_parent(parent.clone())?;

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
                )?
                .block_wait()?;

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
            volume.add_subvolume(subdisk1)?;
            volume.add_subvolume(subdisk2)?;
            volume.add_read_only_parent(parent.clone())?;

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
        volume.add_subvolume(subdisk1)?;
        volume.add_subvolume(subdisk2)?;
        volume.add_read_only_parent(parent.clone())?;

        test_parent_read_only_region(BLOCK_SIZE, parent, volume, 0x00).await?;

        Ok(())
    }

    #[test]
    fn test_volume_with_only_read_only_parent() -> Result<()> {
        const BLOCK_SIZE: u64 = 512;

        let parent =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));

        parent.activate(0)?;

        // Write 0x80 into parent
        parent
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![128; 2048]),
            )?
            .block_wait()?;

        let volume = Volume {
            uuid: Uuid::new_v4(),
            sub_volumes: vec![],
            read_only_parent: Some(SubVolume {
                lba_range: Range {
                    start: 0,
                    end: parent.total_size()? / BLOCK_SIZE as u64,
                },
                block_io: parent.clone(),
            }),
            block_size: BLOCK_SIZE,
            count: AtomicU32::new(0),
        };

        volume.activate(0)?;

        assert_eq!(volume.total_size()?, 2048);

        // Read volume and verify contents
        let buffer = Buffer::new(2048);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        let expected = vec![128; 2048];

        assert_eq!(*buffer.as_vec(), expected);

        Ok(())
    }

    #[test]
    fn test_writing_to_volume_with_only_read_only_parent() {
        const BLOCK_SIZE: u64 = 512;

        let parent =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));

        let volume = Volume {
            uuid: Uuid::new_v4(),
            sub_volumes: vec![],
            read_only_parent: Some(SubVolume {
                lba_range: Range {
                    start: 0,
                    end: parent.total_size().unwrap() / BLOCK_SIZE as u64,
                },
                block_io: parent.clone(),
            }),
            block_size: BLOCK_SIZE,
            count: AtomicU32::new(0),
        };

        volume.activate(0).unwrap();

        // Write 0x80 into volume - this will error
        let res = volume.write(
            Block::new(0, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![128; 2048]),
        );
        assert!(res.is_err());
    }

    #[test]
    fn test_write_unwritten_to_volume_with_only_read_only_parent() {
        const BLOCK_SIZE: u64 = 512;

        let parent =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));

        let volume = Volume {
            uuid: Uuid::new_v4(),
            sub_volumes: vec![],
            read_only_parent: Some(SubVolume {
                lba_range: Range {
                    start: 0,
                    end: parent.total_size().unwrap() / BLOCK_SIZE as u64,
                },
                block_io: parent.clone(),
            }),
            block_size: BLOCK_SIZE,
            count: AtomicU32::new(0),
        };

        volume.activate(0).unwrap();

        // Write 0x80 into volume - this will error
        let res = volume.write_unwritten(
            Block::new(0, BLOCK_SIZE.trailing_zeros()),
            Bytes::from(vec![128; 2048]),
        );

        assert!(res.is_err());
    }

    #[test]
    fn test_drop_then_recreate_test() -> Result<()> {
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
            )?
            .block_wait()?;

        let overlay = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 10,
        ));

        {
            // Make a volume, verify orignal data, write new data to it, then
            // let it fall out of scope
            let mut volume = Volume::new(BLOCK_SIZE as u64);
            volume.add_subvolume(overlay.clone())?;
            volume.add_read_only_parent(parent.clone())?;

            let buffer = Buffer::new(BLOCK_SIZE * 10);
            volume
                .read(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    buffer.clone(),
                )?
                .block_wait()?;

            assert_eq!(vec![0x55; BLOCK_SIZE * 10], *buffer.as_vec());

            volume
                .write(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    Bytes::from(vec![0xFF; BLOCK_SIZE * 10]),
                )?
                .block_wait()?;

            let buffer = Buffer::new(BLOCK_SIZE * 10);
            volume
                .read(
                    Block::new(0, BLOCK_SIZE.trailing_zeros()),
                    buffer.clone(),
                )?
                .block_wait()?;

            assert_eq!(vec![0xFF; BLOCK_SIZE * 10], *buffer.as_vec());
        }

        // Create the same volume, verify data was written
        // Note that add function order is reversed, it shouldn't matter
        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume.add_read_only_parent(parent)?;
        volume.add_subvolume(overlay)?;

        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0xFF; BLOCK_SIZE * 10], *buffer.as_vec());

        Ok(())
    }

    #[test]
    fn test_three_layers() -> Result<()> {
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
            )?
            .block_wait()?;

        // Read parent, verify contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        parent
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0x55; BLOCK_SIZE * 10], *buffer.as_vec());

        let mut parent_volume = Volume::new(BLOCK_SIZE as u64);
        parent_volume.add_subvolume(parent)?;

        let overlay = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 10,
        ));

        let mut volume = Volume::new(BLOCK_SIZE as u64);
        volume.add_subvolume(overlay)?;
        volume.add_read_only_parent(Arc::new(parent_volume))?;

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
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0x55; BLOCK_SIZE * 10], *buffer.as_vec());

        // Write over whole volume
        volume
            .write(
                Block::new(0, BLOCK_SIZE.trailing_zeros()),
                Bytes::from(vec![0xFF; BLOCK_SIZE * 10]),
            )?
            .block_wait()?;

        // Read whole volume, verify new contents
        let buffer = Buffer::new(BLOCK_SIZE * 10);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![0xFF; BLOCK_SIZE * 10], *buffer.as_vec());

        Ok(())
    }

    #[tokio::test]
    async fn construct_snapshot_backed_vol() {
        let _request = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: 512,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 512,
                opts: CrucibleOpts {
                    target: vec![
                        "127.0.0.1:123".parse().unwrap(),
                        "127.0.0.1:456".parse().unwrap(),
                        "127.0.0.1:789".parse().unwrap(),
                    ],
                    key: Some("key".to_string()),
                    ..Default::default()
                },
                gen: 0,
            }],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size: 512,
                    opts: CrucibleOpts {
                        target: vec![
                            "127.0.0.1:11111".parse().unwrap(),
                            "127.0.0.1:22222".parse().unwrap(),
                            "127.0.0.1:33333".parse().unwrap(),
                        ],
                        key: Some("key".to_string()),
                        ..Default::default()
                    },
                    gen: 0,
                },
            )),
        };

        // XXX can't test this without a running set of downstairs
        // let vol = Volume::construct(request).unwrap();
    }

    #[tokio::test]
    async fn construct_read_only_iso_volume() {
        let _request = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: 512,
            sub_volumes: vec![],
            read_only_parent: Some(Box::new(VolumeConstructionRequest::Url {
                id: Uuid::new_v4(),
                block_size: 512,
                // You can boot anything as long as it's Alpine
                url: "https://fake.test/alpine.iso".to_string(),
            })),
        };

        // XXX can't test this without a running set of downstairs
        // let vol = Volume::construct(request).unwrap();
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
        let volume = Volume::construct(request, None).unwrap();

        let buffer = Buffer::new(BLOCK_SIZE);
        volume
            .read(Block::new(0, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .unwrap()
            .block_wait()
            .unwrap();

        assert_eq!(vec![0x0; BLOCK_SIZE], *buffer.as_vec());

        let buffer = Buffer::new(BLOCK_SIZE);
        volume
            .read(Block::new(1, BLOCK_SIZE.trailing_zeros()), buffer.clone())
            .unwrap()
            .block_wait()
            .unwrap();

        assert_eq!(vec![0x5; BLOCK_SIZE], *buffer.as_vec());
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
            )?
            .block_wait()?;

        // Read back parent, verify 1s
        let buffer = Buffer::new(block_size * 5);
        parent
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![11; block_size * 5], *buffer.as_vec());

        // Create a volume out of this parent and the argument subvolume parts
        let mut volume = Volume::new(block_size as u64);

        for subvolume in subvolumes {
            volume.add_subvolume(subvolume.clone())?;
        }

        volume.add_read_only_parent(parent)?;

        volume.activate(0)?;

        assert_eq!(volume.total_size()?, block_size as u64 * 10);

        // Verify parent contents in one read
        let buffer = Buffer::new(block_size * 10);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        let mut expected = vec![11; block_size * 5];
        expected.extend(vec![0x00; block_size * 5]);
        assert_eq!(expected, *buffer.as_vec());

        // One big write!
        volume
            .write(
                Block::new(0, block_size.trailing_zeros()),
                Bytes::from(vec![55; block_size * 10]),
            )?
            .block_wait()?;

        // Verify volume contents in one read
        let buffer = Buffer::new(block_size * 10);
        volume
            .read(Block::new(0, block_size.trailing_zeros()), buffer.clone())?
            .block_wait()?;

        assert_eq!(vec![55; block_size * 10], *buffer.as_vec());

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
}
