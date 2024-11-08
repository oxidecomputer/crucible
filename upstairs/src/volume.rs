// Copyright 2023 Oxide Computer Company

use super::*;
use crate::guest::Guest;

use async_recursion::async_recursion;
use oximeter::types::ProducerRegistry;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use tokio::time::Instant;

use crucible_client_types::RegionExtentInfo;
use crucible_client_types::ReplacementRequestCheck;
use crucible_client_types::VolumeConstructionRequest;

/// Creates a `RegionDefinition` from a set of parameters in a region
/// construction request.
//
// TODO(#559): This should return a Vec<RegionDefinition> with one definition
// for each downstairs, each bearing the expected downstairs UUID for that
// downstairs, but this requires those UUIDs to be present in `opts`, which
// currently doesn't store them.
fn build_region_definition(
    extent_info: &RegionExtentInfo,
    opts: &CrucibleOpts,
) -> Result<RegionDefinition> {
    let mut region_options = RegionOptions::default();
    region_options.set_block_size(extent_info.block_size);
    region_options.set_extent_size(Block {
        value: extent_info.blocks_per_extent,
        shift: extent_info.block_size.trailing_zeros(),
    });
    region_options.set_encrypted(opts.key.is_some());

    let mut region_def = RegionDefinition::from_options(&region_options)?;
    region_def.set_extent_count(extent_info.extent_count);
    Ok(region_def)
}

// Result of comparing two [`VolumeConstructionRequest::Region`] values
#[derive(Debug)]
enum VCRDelta {
    /// The Regions are identical.
    Same,
    /// The newer Region only has a higher generation number.
    Generation,
    /// The newer Region has higher generation and just one target different.
    Target { old: SocketAddr, new: SocketAddr },
}

// Result of comparing two [`VolumeConstructionRequest::Volume`] values
#[derive(Debug)]
enum CompareResult {
    Volume {
        sub_compares: Vec<CompareResult>,
        read_only_parent_compare: Box<CompareResult>,
    },

    Region {
        delta: VCRDelta,
    },

    NewMissing,
}

impl CompareResult {
    pub fn all_same(&self) -> bool {
        let mut parts = VecDeque::new();
        parts.push_back(self);

        while let Some(part) = parts.pop_front() {
            match part {
                CompareResult::Volume {
                    sub_compares,
                    read_only_parent_compare,
                } => {
                    for sub_compare in sub_compares {
                        parts.push_back(sub_compare);
                    }

                    parts.push_back(read_only_parent_compare);
                }

                CompareResult::Region { delta } => {
                    match delta {
                        VCRDelta::Same => {
                            // no-op
                        }

                        VCRDelta::Generation | VCRDelta::Target { .. } => {
                            return false;
                        }
                    }
                }

                CompareResult::NewMissing => {
                    return false;
                }
            }
        }

        true
    }
}

#[derive(Debug, Clone)]
pub struct Volume(Arc<VolumeInner>);

impl std::ops::Deref for Volume {
    type Target = VolumeInner;
    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

pub struct VolumeBuilder(VolumeInner);

#[derive(Debug)]
pub struct VolumeInner {
    uuid: Uuid,

    sub_volumes: Vec<SubVolume>,
    read_only_parent: Option<SubVolume>,

    /*
     * The block below which the scrubber has written
     */
    scrub_point: AtomicU64,

    /*
     * Each sub volume should be the same block size (unit is bytes)
     */
    block_size: u64,
    count: AtomicU32,

    log: Logger,
}

#[derive(Clone)]
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

impl VolumeBuilder {
    pub fn new_with_id(block_size: u64, uuid: Uuid, log: Logger) -> Self {
        Self(VolumeInner {
            uuid,
            sub_volumes: vec![],
            read_only_parent: None,
            scrub_point: AtomicU64::new(0),
            block_size,
            count: AtomicU32::new(0),
            log,
        })
    }

    pub fn new(block_size: u64, log: Logger) -> Self {
        Self::new_with_id(block_size, Uuid::new_v4(), log)
    }

    pub async fn add_subvolume(
        &mut self,
        block_io: Arc<dyn BlockIO + Send + Sync>,
    ) -> Result<(), CrucibleError> {
        let block_size = block_io.get_block_size().await?;

        if block_size != self.0.block_size {
            crucible_bail!(BlockSizeMismatch);
        }

        let number_of_blocks = block_io.total_size().await? / block_size;

        self.0.sub_volumes.push(SubVolume {
            lba_range: self.0.compute_next_lba_range(number_of_blocks),
            block_io,
        });

        Ok(())
    }

    pub async fn add_subvolume_create_guest(
        &mut self,
        opts: CrucibleOpts,
        extent_info: RegionExtentInfo,
        gen: u64,
        producer_registry: Option<ProducerRegistry>,
    ) -> Result<(), CrucibleError> {
        let region_def = build_region_definition(&extent_info, &opts)?;
        let (guest, io) = Guest::new(Some(self.0.log.clone()));

        let _join_handle =
            up_main(opts, gen, Some(region_def), io, producer_registry)?;

        self.add_subvolume(Arc::new(guest)).await
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

        if block_size != self.0.block_size {
            crucible_bail!(BlockSizeMismatch);
        }

        let number_of_blocks = block_io.total_size().await? / block_size;

        // TODO migration task to pull blocks in from parent, then set parent to
        // None when done - read block by block, don't overwrite if owned by sub
        // volume

        self.0.read_only_parent = Some(SubVolume {
            // Read only parent LBA range always starts from 0
            lba_range: Range {
                start: 0,
                end: number_of_blocks,
            },
            block_io,
        });

        Ok(())
    }
}

impl From<VolumeInner> for Volume {
    fn from(v: VolumeInner) -> Volume {
        Volume(v.into())
    }
}

impl From<VolumeBuilder> for Volume {
    fn from(v: VolumeBuilder) -> Volume {
        Volume(v.0.into())
    }
}

impl VolumeInner {
    // Increment the counter to allow all IOs to have a unique number
    // for dtrace probes.
    pub fn next_count(&self) -> u32 {
        self.count.fetch_add(1, Ordering::Relaxed)
    }

    // Create a simple Volume from a single BlockIO
    pub async fn from_block_io(
        block_io: Arc<dyn BlockIO + Sync + Send>,
        log: Logger,
    ) -> Result<Self, CrucibleError> {
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
            scrub_point: AtomicU64::new(0),
            block_size,
            count: AtomicU32::new(0),
            log,
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

    // Check to see if this volume has a read only parent
    pub fn has_read_only_parent(&self) -> bool {
        self.read_only_parent.is_some()
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

    #[allow(clippy::if_same_then_else)]
    pub fn read_only_parent_for_lba_range(
        &self,
        start: u64,
        length: u64,
    ) -> Option<Range<u64>> {
        if let Some(ref read_only_parent) = self.read_only_parent {
            // Check if the scrubber has passed this offset
            let scrub_point = self.scrub_point.load(Ordering::SeqCst);
            if scrub_point >= read_only_parent.lba_range.end {
                // No need to check ROP, the scrub is done.
                None
            } else if start + length <= scrub_point {
                None
            } else {
                read_only_parent.lba_range_coverage(start, length)
            }
        } else {
            None
        }
    }

    // This method is called by both write and write_unwritten and
    // provides a single place so both can share common code.
    async fn volume_write_op(
        &self,
        offset: BlockIndex,
        mut data: BytesMut,
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

        if data.is_empty() {
            if is_write_unwritten {
                cdt::volume__writeunwritten__done!(|| (cc, self.uuid));
            } else {
                cdt::volume__write__done!(|| (cc, self.uuid));
            }
            return Ok(());
        }

        self.check_data_size(data.len()).await?;

        let affected_sub_volumes = self.sub_volumes_for_lba_range(
            offset.0,
            data.len() as u64 / self.block_size,
        );

        if affected_sub_volumes.is_empty() {
            crucible_bail!(OffsetInvalid);
        }

        // TODO parallel dispatch!
        for (coverage, sub_volume) in affected_sub_volumes {
            let sub_offset =
                BlockIndex(sub_volume.compute_sub_volume_lba(coverage.start));
            let sz = (coverage.end - coverage.start) as usize
                * self.block_size as usize;
            let slice = data.split_to(sz);

            // Take the write or write_unwritten path here.
            if is_write_unwritten {
                sub_volume.write_unwritten(sub_offset, slice).await?;
            } else {
                sub_volume.write(sub_offset, slice).await?;
            }
        }

        if is_write_unwritten {
            cdt::volume__writeunwritten__done!(|| (cc, self.uuid));
        } else {
            cdt::volume__write__done!(|| (cc, self.uuid));
        }

        Ok(())
    }
}

impl Volume {
    // Scrub a volume.
    // If a volume has a read only parent, we do the work to read from
    // the read only side, and write_unwritten to the LBA of the SubVolume
    // that is "below" it.
    //
    // Still TODO: here
    // If there is an error, how to we tell Nexus?
    // If we finish the scrub, how do we tell Nexus?
    pub async fn scrub(
        &self,
        start_delay: Option<u64>,
        scrub_pause: Option<u64>,
    ) -> Result<(), CrucibleError> {
        info!(self.log, "Scrub check for {}", self.uuid);
        // XXX Can we assert volume is activated?

        if let Some(ref read_only_parent) = self.read_only_parent {
            // If requested, setup the pause between IOs as well as an initial
            // waiting period before the scrubber starts.
            let pause_millis = scrub_pause.unwrap_or(0);

            if let Some(start_delay) = start_delay {
                info!(
                    self.log,
                    "Scrub pause {} seconds before starting", start_delay
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    start_delay,
                ))
                .await;
            }

            let ts = read_only_parent.total_size().await?;
            let bs = read_only_parent.get_block_size().await? as usize;

            info!(self.log, "Scrub for {} begins", self.uuid);
            info!(
                self.log,
                "Scrub with total_size:{:?} block_size:{:?}", ts, bs
            );
            let scrub_start = Instant::now();

            let start = read_only_parent.lba_range.start;
            let end = read_only_parent.lba_range.end;

            // Based on some seat of the pants measurements, we are doing
            // 256 KiB IOs during the scrub. Here we select how many blocks
            // this is.
            // TODO: Determine if this value should be adjusted.
            let mut block_count = 131072 / bs;
            info!(
                self.log,
                "Scrubs from block {:?} to {:?} in ({}) {:?} size IOs pm:{}",
                start,
                end,
                block_count,
                block_count * bs,
                pause_millis,
            );

            let mut retries = 0;
            let showstep = (end - start) / 25;
            let mut showat = start + showstep;
            let mut offset = start;
            while offset < end {
                if offset + block_count as u64 > end {
                    block_count = (end - offset) as usize;
                    info!(
                        self.log,
                        "Adjust block_count to {} at offset {}",
                        block_count,
                        offset
                    );
                }
                assert!(offset + block_count as u64 <= end);
                let block = BlockIndex(offset);

                // The read will first try to establish a connection to
                // the remote side before returning something we can await
                // on. If that initial connection fails, we can retry.
                let mut retry_count = 0;

                let mut buffer = Buffer::new(block_count, bs);

                loop {
                    match read_only_parent.read(block, &mut buffer).await {
                        Ok(()) => {
                            if retry_count > 0 {
                                // This counter indicates a retry was
                                // eventually successful.
                                retries += 1;
                            }

                            break;
                        }

                        Err(e) => {
                            warn!(self.log, "scrub {}, offset {}", e, offset);
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
                    BlockIndex(offset),
                    buffer.into_bytes_mut(),
                )
                .await?;

                offset += block_count as u64;

                // Set the scrub high water mark
                self.scrub_point.store(offset, Ordering::SeqCst);

                if offset > showat {
                    info!(
                        self.log,
                        "Scrub at offset {}/{} sp:{:?}",
                        offset,
                        end,
                        self.scrub_point
                    );
                    showat += showstep;
                }
                // Pause just a bit between IOs so we don't starve the guest
                // TODO: More benchmarking to find a good value here.
                tokio::time::sleep(Duration::from_millis(pause_millis)).await;
            }

            let total_time = scrub_start.elapsed();
            info!(
                self.log,
                "Scrub {} done in {} seconds. Retries:{} scrub_size:{} size:{} pause_milli:{}",
                self.uuid,
                total_time.as_secs(),
                retries,
                block_count * bs,
                end - start,
                pause_millis,
            );
            self.flush(None).await?;
            info!(self.log, "Deactivate read only parent {}", self.uuid,);
            if let Err(e) = read_only_parent.deactivate().await {
                warn!(
                    self.log,
                    "deactivate ROP on {} failed with {}", self.uuid, e
                );
            }
        } else {
            info!(self.log, "Scrub for {} not required", self.uuid);
        }

        Ok(())
    }

    pub async fn volume_extent_info(
        &self,
    ) -> Result<VolumeInfo, CrucibleError> {
        // A volume has multiple levels of extent info, not just one.
        let mut volumes = Vec::new();

        for sub_volume in &self.sub_volumes {
            match sub_volume.query_extent_info().await? {
                Some(ei) => {
                    // When a volume is created and sub_volumes are added to
                    // it, we verify that the block sizes match, so we never
                    // expect there to be a mismatch.
                    assert_eq!(self.block_size, ei.block_size);
                    let svi = SubVolumeInfo {
                        blocks_per_extent: ei.blocks_per_extent,
                        extent_count: ei.extent_count,
                    };
                    volumes.push(svi);
                }
                None => {
                    // Mixing sub_volumes with and without
                    if !volumes.is_empty() {
                        crucible_bail!(SubvolumeTypeMismatch);
                    }
                    continue;
                }
            }
        }

        Ok(VolumeInfo {
            block_size: self.block_size,
            volumes,
        })

        // TODO: add support for read only parents
    }
}

#[async_trait]
impl BlockIO for VolumeInner {
    async fn activate(&self) -> Result<(), CrucibleError> {
        for sub_volume in &self.sub_volumes {
            sub_volume.activate().await?;

            let sub_volume_computed_size = self.block_size
                * (sub_volume.lba_range.end - sub_volume.lba_range.start);

            if sub_volume.total_size().await? != sub_volume_computed_size {
                crucible_bail!(SubvolumeSizeMismatch);
            }
        }

        if let Some(ref read_only_parent) = &self.read_only_parent {
            read_only_parent.activate().await?;
        }

        Ok(())
    }
    async fn activate_with_gen(&self, gen: u64) -> Result<(), CrucibleError> {
        for sub_volume in &self.sub_volumes {
            sub_volume.activate_with_gen(gen).await?;

            let sub_volume_computed_size = self.block_size
                * (sub_volume.lba_range.end - sub_volume.lba_range.start);

            if sub_volume.total_size().await? != sub_volume_computed_size {
                crucible_bail!(SubvolumeSizeMismatch);
            }
        }

        if let Some(ref read_only_parent) = &self.read_only_parent {
            read_only_parent.activate_with_gen(gen).await?;
        }

        Ok(())
    }

    // Return a vec of these?
    // Placeholder for first round of crutest changes.
    // I think this should return some sort of VolumeInfo struct with an
    // SV/ROP structure where the SV is a Vec<WQC>  and the ROP is Option<WQC>
    async fn query_work_queue(&self) -> Result<WQCounts, CrucibleError> {
        let mut all_wq = WQCounts {
            up_count: 0,
            ds_count: 0,
            active_count: 0,
        };

        for sub_volume in &self.sub_volumes {
            let sv_wq = sub_volume.query_work_queue().await?;
            all_wq.up_count += sv_wq.up_count;
            all_wq.ds_count += sv_wq.ds_count;
            all_wq.active_count += sv_wq.active_count;
        }

        if let Some(ref read_only_parent) = &self.read_only_parent {
            let rop_wq = read_only_parent.query_work_queue().await?;
            all_wq.up_count += rop_wq.up_count;
            all_wq.ds_count += rop_wq.ds_count;
            all_wq.active_count += rop_wq.active_count;
        }

        Ok(all_wq)
    }

    // Return a vec of these?
    // Return a struct with a vec for SV and Some/None for ROP?
    async fn query_extent_info(
        &self,
    ) -> Result<Option<RegionExtentInfo>, CrucibleError> {
        // A volume has multiple levels of extent info, not just one.
        // To get the same info in the proper form, use volume_extent_info()
        Ok(None)
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
                total_blocks +=
                    sub_volume.lba_range.end - sub_volume.lba_range.start;
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
        offset: BlockIndex,
        data: &mut Buffer,
    ) -> Result<(), CrucibleError> {
        // In the case that this volume only has a read only parent, serve
        // reads directly from that
        let cc = self.next_count();
        cdt::volume__read__start!(|| (cc, self.uuid));

        if data.is_empty() {
            cdt::volume__read__done!(|| (cc, self.uuid));
            return Ok(());
        }

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

        let bs = self.check_data_size(data.len()).await? as usize;

        let affected_sub_volumes = self.sub_volumes_for_lba_range(
            offset.0,
            data.len() as u64 / self.block_size,
        );

        if affected_sub_volumes.is_empty() {
            crucible_bail!(OffsetInvalid);
        }

        // TODO parallel dispatch!
        let mut data_index = 0;
        let mut read_buffer = UninitializedBuffer::default();
        let single_volume = affected_sub_volumes.len() == 1;

        for (coverage, sub_volume) in affected_sub_volumes {
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
                let parent_offset = BlockIndex(coverage.start);
                let mut buf = read_buffer.reset_owned(
                    (parent_coverage.end - parent_coverage.start) as usize,
                    bs,
                );
                self.read_only_parent
                    .as_ref()
                    .unwrap()
                    .read(parent_offset, &mut buf)
                    .await?;

                read_buffer = data.eat(data_index, buf);
            }

            let sub_offset =
                BlockIndex(sub_volume.compute_sub_volume_lba(coverage.start));

            // Special case: if there's a single subvolume, then read directly
            // into the provided data buffer, to save a memcpy.
            if single_volume {
                sub_volume.read(sub_offset, data).await?;
            } else {
                let mut buf = read_buffer
                    .reset_owned((coverage.end - coverage.start) as usize, bs);
                sub_volume.read(sub_offset, &mut buf).await?;
                read_buffer = data.eat(data_index, buf);
            }

            data_index += (coverage.end - coverage.start) as usize
                * self.block_size as usize;
        }

        assert_eq!(data.len(), data_index);

        cdt::volume__read__done!(|| (cc, self.uuid));
        Ok(())
    }

    async fn write(
        &self,
        offset: BlockIndex,
        data: BytesMut,
    ) -> Result<(), CrucibleError> {
        // Make use of the volume specific write_op method to avoid
        // code duplication with volume.write_unwritten.
        self.volume_write_op(offset, data, false).await
    }

    async fn write_unwritten(
        &self,
        offset: BlockIndex,
        data: BytesMut,
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
            active_count: 0,
        };

        for sub_volume in &self.sub_volumes {
            let sub_wq_counts = sub_volume.show_work().await?;

            wq_counts.up_count += sub_wq_counts.up_count;
            wq_counts.ds_count += sub_wq_counts.ds_count;
            wq_counts.active_count += sub_wq_counts.active_count;
        }

        if let Some(ref read_only_parent) = &self.read_only_parent {
            let sub_wq_counts = read_only_parent.show_work().await?;

            wq_counts.up_count += sub_wq_counts.up_count;
            wq_counts.ds_count += sub_wq_counts.ds_count;
            wq_counts.active_count += sub_wq_counts.active_count;
        }

        Ok(wq_counts)
    }

    async fn replace_downstairs(
        &self,
        id: Uuid,
        old: SocketAddr,
        new: SocketAddr,
    ) -> Result<ReplaceResult, CrucibleError> {
        for sub_volume in &self.sub_volumes {
            let result = sub_volume.replace_downstairs(id, old, new).await?;
            match result {
                ReplaceResult::Started
                | ReplaceResult::StartedAlready
                | ReplaceResult::CompletedAlready
                | ReplaceResult::VcrMatches => {
                    // found the subvolume, so stop!
                    return Ok(result);
                }

                ReplaceResult::Missing => {
                    // keep looking!
                }
            }
        }

        // We did not find it in the sub_volumes, so check the read only
        // parents now.
        if let Some(rop) = &self.read_only_parent {
            let result = rop.replace_downstairs(id, old, new).await?;
            match result {
                ReplaceResult::Started
                | ReplaceResult::StartedAlready
                | ReplaceResult::CompletedAlready
                | ReplaceResult::VcrMatches => {
                    // Found a match in our read only parent
                    info!(self.log, "ROP has something here");
                    return Ok(result);
                }

                ReplaceResult::Missing => {}
            }
        }

        Ok(ReplaceResult::Missing)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct VolumeInfo {
    pub block_size: u64,
    pub volumes: Vec<SubVolumeInfo>,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SubVolumeInfo {
    pub blocks_per_extent: u64,
    pub extent_count: u32,
}

impl VolumeInfo {
    pub fn total_size(&self) -> u64 {
        self.block_size * (self.total_blocks() as u64)
    }

    pub fn total_blocks(&self) -> usize {
        let mut total_blocks = 0;
        for sv in &self.volumes {
            total_blocks += sv.blocks_per_extent * (sv.extent_count as u64);
        }
        total_blocks.try_into().unwrap()
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
            panic!(
                "should never get here! {:?} {} {}",
                self.lba_range, start, length
            );
        }
    }
}

#[async_trait]
impl BlockIO for SubVolume {
    async fn activate(&self) -> Result<(), CrucibleError> {
        self.block_io.activate().await
    }
    async fn activate_with_gen(&self, gen: u64) -> Result<(), CrucibleError> {
        self.block_io.activate_with_gen(gen).await
    }
    async fn query_work_queue(&self) -> Result<WQCounts, CrucibleError> {
        self.block_io.query_work_queue().await
    }
    async fn query_extent_info(
        &self,
    ) -> Result<Option<RegionExtentInfo>, CrucibleError> {
        self.block_io.query_extent_info().await
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
        offset: BlockIndex,
        data: &mut Buffer,
    ) -> Result<(), CrucibleError> {
        self.block_io.read(offset, data).await
    }

    async fn write(
        &self,
        offset: BlockIndex,
        data: BytesMut,
    ) -> Result<(), CrucibleError> {
        self.block_io.write(offset, data).await
    }

    async fn write_unwritten(
        &self,
        offset: BlockIndex,
        data: BytesMut,
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

    async fn replace_downstairs(
        &self,
        id: Uuid,
        old: SocketAddr,
        new: SocketAddr,
    ) -> Result<ReplaceResult, CrucibleError> {
        self.block_io.replace_downstairs(id, old, new).await
    }
}

impl Volume {
    pub fn as_blockio(&self) -> Arc<dyn BlockIO + Send + Sync> {
        self.0.clone()
    }

    #[async_recursion]
    pub async fn construct(
        request: VolumeConstructionRequest,
        producer_registry: Option<ProducerRegistry>,
        log: Logger,
    ) -> Result<Volume> {
        match request {
            VolumeConstructionRequest::Volume {
                id,
                block_size,
                sub_volumes,
                read_only_parent,
            } => {
                let mut vol =
                    VolumeBuilder::new_with_id(block_size, id, log.clone());

                for subreq in sub_volumes {
                    vol.add_subvolume(
                        Volume::construct(
                            subreq,
                            producer_registry.clone(),
                            log.clone(),
                        )
                        .await?
                        .as_blockio(),
                    )
                    .await?;
                }

                if let Some(read_only_parent) = read_only_parent {
                    vol.add_read_only_parent(
                        Volume::construct(
                            *read_only_parent,
                            producer_registry,
                            log,
                        )
                        .await?
                        .as_blockio(),
                    )
                    .await?;
                }

                Ok(vol.into())
            }

            VolumeConstructionRequest::Url {
                id,
                block_size,
                url,
            } => {
                let mut vol = VolumeBuilder::new(block_size, log.clone());
                vol.add_subvolume(Arc::new(
                    ReqwestBlockIO::new(id, block_size, url).await?,
                ))
                .await?;
                Ok(vol.into())
            }

            VolumeConstructionRequest::Region {
                block_size,
                blocks_per_extent,
                extent_count,
                opts,
                gen,
            } => {
                let mut vol = VolumeBuilder::new(block_size, log.clone());
                vol.add_subvolume_create_guest(
                    opts,
                    RegionExtentInfo {
                        block_size,
                        blocks_per_extent,
                        extent_count,
                    },
                    gen,
                    producer_registry,
                )
                .await?;
                Ok(vol.into())
            }

            VolumeConstructionRequest::File {
                id,
                block_size,
                path,
            } => {
                let mut vol = VolumeBuilder::new(block_size, log.clone());
                vol.add_subvolume(Arc::new(FileBlockIO::new(
                    id, block_size, path,
                )?))
                .await?;
                Ok(vol.into())
            }
        }
    }

    /// Attempts to deconstruct the volume back into a `VolumeBuilder`
    ///
    /// This will only succeed if the `Volume` has a single strong reference,
    /// and will return `None` otherwise.  This function should only be used in
    /// unit and integration tests, to do Weird Things to volumes.
    pub fn deconstruct(self) -> Option<VolumeBuilder> {
        Arc::try_unwrap(self.0).ok().map(VolumeBuilder)
    }

    // We compare two VolumeConstructionRequests to see if the second one
    // could be considered as an updated VCR to the original.  We are
    // considering two situations here, one for migration and one for a
    // downstairs target replacement.
    //
    // The requirements to allow a new VCR are:
    // 1. Only VolumeConstructionRequests::Volume type is supported.
    // 2. Sub volumes must all be VolumeConstructionRequest::Region
    // 3. Everything must be the same between the two Volumes, except:
    //    A. The new generation number must be greater than the old.
    //    B. The new Volume can have None for read only parent if the
    //       original had Some(), or it must match the Some().
    //
    //  Specifically for the downstairs target replacement case we also require
    //  this difference:
    //    C. Only one CrucibleOpts::target[] is different.
    //
    // Any other difference between the two volumes is considered a failure.
    //
    // We return Ok(None) if requirements 1, 2, 3A, 3B are met.  This would
    // mean a migration using the old/new VCRs are acceptable.
    //
    // We return Ok(Some(old_target, new_target)) if requirements 1, 2, 3A,
    // 3B, and 3C are all met.  This would mean that the VCRs are valid for
    // a downstairs replacement.

    pub fn compare_vcr_for_migration(
        original: VolumeConstructionRequest,
        replacement: VolumeConstructionRequest,
        log: &Logger,
    ) -> Result<(), CrucibleError> {
        match Self::compare_vcr_for_update(original, replacement, log)? {
            Some((_o, _n)) => crucible_bail!(
                ReplaceRequestInvalid,
                "VCR targets are different"
            ),
            None => Ok(()),
        }
    }

    pub fn compare_vcr_for_target_replacement(
        original: VolumeConstructionRequest,
        replacement: VolumeConstructionRequest,
        log: &Logger,
    ) -> Result<ReplacementRequestCheck, CrucibleError> {
        match Self::compare_vcr_for_update(original, replacement, log)? {
            Some((o, n)) => {
                Ok(ReplacementRequestCheck::Valid { old: o, new: n })
            }

            None => Ok(ReplacementRequestCheck::ReplacementMatchesOriginal),
        }
    }

    /// Given two VolumeConstructionRequests, compare them, and return:
    ///
    /// - Ok(Some(old, new)) if there's a single target difference in one of the
    ///   Region variants, and the replacement argument is a valid replacement
    ///   for the original argument. This also requires that the generation
    ///   numbers for the replacement VCR's subvolumes are higher than the
    ///   original VCR's subvolumes. Read-only parent generation numbers are
    ///   ignored.
    ///
    /// - Ok(None) if there are no differences between them, but the
    ///   generation number of the subvolumes is higher in the replacement VCR.
    ///   Read-only parent generation numbers are ignored.
    ///
    /// - Err(e) if there's a difference that means the replacement argument is
    ///   not a valid replacement for the original argument, or the two VCRs are
    ///   identical.
    pub fn compare_vcr_for_update(
        original: VolumeConstructionRequest,
        replacement: VolumeConstructionRequest,
        log: &Logger,
    ) -> Result<Option<(SocketAddr, SocketAddr)>, CrucibleError> {
        let compare_result = Self::compare_vcr_for_replacement(
            log,
            &original,
            &replacement,
            false,
        )?;

        info!(log, "compare result is {:?}", compare_result);

        // Have multiple Target deltas been reported?
        let mut targets = Vec::new();

        // Are there differences in the VCRs?
        let mut all_same = true;

        let mut parts = VecDeque::new();
        parts.push_back(&compare_result);

        while let Some(part) = parts.pop_front() {
            match part {
                CompareResult::Volume {
                    sub_compares,
                    read_only_parent_compare,
                } => {
                    for sub_compare in sub_compares {
                        parts.push_back(sub_compare);
                    }

                    parts.push_back(read_only_parent_compare);
                }
                CompareResult::Region { delta } => {
                    match delta {
                        VCRDelta::Same => {
                            // no-op
                        }

                        VCRDelta::Generation => {
                            all_same = false;
                        }

                        VCRDelta::Target { .. } => {
                            targets.push(delta);
                            all_same = false;
                        }
                    }
                }

                CompareResult::NewMissing => {
                    // no-op
                }
            }
        }

        if targets.len() > 1 {
            crucible_bail!(ReplaceRequestInvalid, "Multiple target changes")
        }

        if all_same {
            crucible_bail!(ReplaceRequestInvalid, "The VCRs have no difference")
        }

        // Beyond this point zero or one target changes where found

        match &compare_result {
            CompareResult::Volume { sub_compares, .. } => {
                // Walk the cases. As some deltas are okay for a read only
                // parent that are not for a sub_volume, we need to look at all
                // the possible permutations Nexus supports.
                if targets.is_empty() {
                    // If here, we passed by the `all_same` bail above, meaning
                    // there's a generation delta somewhere.

                    if sub_compares[0].all_same() {
                        // ROP shouldn't have a generation bump
                        crucible_bail!(
                            ReplaceRequestInvalid,
                            "ROP has generation bump!"
                        );
                    } else {
                        // Sub volume has generation bump, and ROP is all the
                        // same
                        return Ok(None);
                    }
                }

                // Past here, one target change found. Return it.
                let VCRDelta::Target { old, new } = targets[0] else {
                    panic!("should only have pushed Target");
                };

                // We know we have one target that is different.  Verify that
                // all the rest of the sub_volumes have a generation number
                // difference.
                for sc in sub_compares {
                    match sc {
                        CompareResult::Region { delta } => match delta {
                            VCRDelta::Same => {
                                crucible_bail!(
                                    ReplaceRequestInvalid,
                                    "SubVolumes don't have generation bump"
                                );
                            }
                            VCRDelta::Generation | VCRDelta::Target { .. } => {}
                        },
                        _ => {
                            panic!("Unsupported multi level CompareResult");
                        }
                    }
                }

                Ok(Some((*old, *new)))
            }

            CompareResult::Region { delta } => match delta {
                VCRDelta::Same => {
                    crucible_bail!(
                        ReplaceRequestInvalid,
                        "The VCRs have no difference"
                    )
                }

                VCRDelta::Generation => Ok(None),

                VCRDelta::Target { old, new } => Ok(Some((*old, *new))),
            },

            CompareResult::NewMissing => {
                crucible_bail!(
                    ReplaceRequestInvalid,
                    "Replacement missing when comparing to replace",
                );
            }
        }
    }

    // Given two VCRs, compare them to each other, including each sub_volume
    // and the read only parent.  Return the collection of results in the
    // CompareResult struct.  Any mismatch results in an error returned
    // right away and no further processing.  The caller is responsible for
    // parsing the CompareResult and deciding what to do if there are
    // multiple changes.
    fn compare_vcr_for_replacement(
        log: &Logger,
        o_vol: &VolumeConstructionRequest,
        n_vol: &VolumeConstructionRequest,
        read_only: bool,
    ) -> Result<CompareResult, CrucibleError> {
        match (o_vol, n_vol) {
            (
                VolumeConstructionRequest::Volume {
                    id: o_id,
                    block_size: o_block_size,
                    sub_volumes: o_sub_volumes,
                    read_only_parent: o_read_only_parent,
                },
                VolumeConstructionRequest::Volume {
                    id: n_id,
                    block_size: n_block_size,
                    sub_volumes: n_sub_volumes,
                    read_only_parent: n_read_only_parent,
                },
            ) => {
                if o_id != n_id {
                    crucible_bail!(
                        ReplaceRequestInvalid,
                        "ID mismatch {} vs. {}",
                        o_id,
                        n_id
                    );
                }

                if o_block_size != n_block_size {
                    crucible_bail!(
                        ReplaceRequestInvalid,
                        "block_size mismatch {} vs. {}",
                        o_block_size,
                        n_block_size
                    )
                }

                // Sub volume lengths should be the same.
                if n_sub_volumes.len() != o_sub_volumes.len() {
                    crucible_bail!(
                        ReplaceRequestInvalid,
                        "subvolume len mismatch {} vs. {}",
                        o_sub_volumes.len(),
                        n_sub_volumes.len(),
                    )
                }

                let mut sv_results = Vec::new();
                // Walk the list of SubVolumes, get the compare result from
                // each one and add it to list for the final result.
                // If we get an error, then that is returned right away and
                // we don't bother processing any further.
                for (o_sv, new_sv) in
                    o_sub_volumes.iter().zip(n_sub_volumes.iter())
                {
                    let sv_res = Self::compare_vcr_for_replacement(
                        log, o_sv, new_sv, read_only,
                    )?;
                    sv_results.push(sv_res);
                }
                let read_only_parent_compare =
                    Box::new(match (o_read_only_parent, n_read_only_parent) {
                        (.., None) => {
                            // The replacement VCR has no read_only_parent. This
                            // is a valid situation as read_only_parents will go
                            // away after a scrub finishes.
                            CompareResult::NewMissing
                        }

                        (None, Some(..)) => {
                            // It's never valid when comparing VCRs to have the
                            // original VCR be missing a read_only_parent and the
                            // new VCR to have a read_only_parent.
                            crucible_bail!(
                                ReplaceRequestInvalid,
                                "VCR added where there should not be one"
                            );
                        }

                        (Some(o_vol), Some(n_vol)) => {
                            Self::compare_vcr_for_replacement(
                                log, o_vol, n_vol, true,
                            )?
                        }
                    });

                let sv_res = CompareResult::Volume {
                    sub_compares: sv_results,
                    read_only_parent_compare,
                };

                Ok(sv_res)
            }

            (
                VolumeConstructionRequest::Region { .. },
                VolumeConstructionRequest::Region { .. },
            ) => Ok(CompareResult::Region {
                delta: Self::compare_vcr_region_for_replacement(
                    log, o_vol, n_vol, read_only,
                )?,
            }),

            _ => {
                crucible_bail!(ReplaceRequestInvalid, "Cannot replace VCR")
            }
        }
    }

    // Given two VCRs where we expect the only type to be a
    // VolumeConstructionRequest::Region.  The VCR can be from a sub_volume
    // or a read_only_parent.  The caller is expected to know how to
    // handle the result depending on what it sends us.
    //
    // We return:
    // VCRDelta::Same
    // If the two VCRs are identical
    //
    // VCRDelta::Generation
    // If it's just a generation number increase in the new VCR.
    //
    // VCRDelta::NewMissing
    // If the new VCR is missing (None).
    //
    // VCRDelta::Target(old_target, new_target)
    // If we have both a generation number increase, and one and only
    // one target is different in the new VCR.
    //
    // Any other difference is an error, and an error returned here means the
    // VCRs are incompatible in a way that prevents one from replacing another.
    fn compare_vcr_region_for_replacement(
        log: &Logger,
        o_vol: &VolumeConstructionRequest,
        n_vol: &VolumeConstructionRequest,
        read_only: bool,
    ) -> Result<VCRDelta, CrucibleError> {
        // Volumes to compare must all be VolumeConstructionRequest::Region
        let (
            o_sv_block_size,
            o_sv_blocks_per_extent,
            o_sv_extent_count,
            o_sv_opts,
            o_sv_gen,
        ) = match *o_vol {
            VolumeConstructionRequest::Region {
                block_size,
                blocks_per_extent,
                extent_count,
                ref opts,
                gen,
            } => (block_size, blocks_per_extent, extent_count, opts, gen),

            _ => {
                crucible_bail!(
                    ReplaceRequestInvalid,
                    "Invalid VCR type for region replacement"
                )
            }
        };

        let (
            n_sv_block_size,
            n_sv_blocks_per_extent,
            n_sv_extent_count,
            n_sv_opts,
            n_sv_gen,
        ) = match *n_vol {
            VolumeConstructionRequest::Region {
                block_size,
                blocks_per_extent,
                extent_count,
                ref opts,
                gen,
            } => (block_size, blocks_per_extent, extent_count, opts, gen),

            _ => {
                crucible_bail!(
                    ReplaceRequestInvalid,
                    "Invalid VCR type for region replacement"
                )
            }
        };

        if n_vol == o_vol {
            return Ok(VCRDelta::Same);
        }

        if o_sv_block_size != n_sv_block_size {
            crucible_bail!(
                ReplaceRequestInvalid,
                "sub_volume block_size mismatch"
            )
        }

        if o_sv_blocks_per_extent != n_sv_blocks_per_extent {
            crucible_bail!(
                ReplaceRequestInvalid,
                "sub_volume blocks_per_extent mismatch"
            )
        }
        if o_sv_extent_count != n_sv_extent_count {
            crucible_bail!(
                ReplaceRequestInvalid,
                "sub_volume extent_count mismatch"
            )
        }

        // The original generation number should always be lower than the new.
        // This is only valid for non read-only parent checks.
        if !read_only && o_sv_gen >= n_sv_gen {
            crucible_bail!(
                ReplaceRequestInvalid,
                "sub_volume generation invalid {:?} vs. {:?}",
                o_sv_gen,
                n_sv_gen,
            )
        }

        // Ignore generation number changes when comparing read-only Volumes
        if read_only && o_sv_gen != n_sv_gen {
            warn!(
                log,
                "read_only sub_volume generation changed: {:?} vs. {:?}",
                o_sv_gen,
                n_sv_gen,
            )
        }

        if o_sv_opts.id != n_sv_opts.id {
            crucible_bail!(
                ReplaceRequestInvalid,
                "sub_volume opts id invalid {:?} vs. {:?}",
                o_sv_opts.id,
                n_sv_opts.id
            )
        }

        if o_sv_opts.lossy != n_sv_opts.lossy {
            crucible_bail!(
                ReplaceRequestInvalid,
                "sub_volume opts lossy invalid {:?} vs. {:?}",
                o_sv_opts.lossy,
                n_sv_opts.lossy
            )
        }

        if o_sv_opts.flush_timeout != n_sv_opts.flush_timeout {
            crucible_bail!(
                ReplaceRequestInvalid,
                "sub_volume opts flush_timeout mismatch {:?} vs. {:?}",
                o_sv_opts.flush_timeout,
                n_sv_opts.flush_timeout
            )
        }

        if o_sv_opts.key != n_sv_opts.key {
            crucible_bail!(
                ReplaceRequestInvalid,
                "sub_volume opts key invalid {:?} vs. {:?}",
                o_sv_opts.key,
                n_sv_opts.key
            )
        }

        if o_sv_opts.cert_pem != n_sv_opts.cert_pem {
            crucible_bail!(
                ReplaceRequestInvalid,
                "sub_volume opts cert_pem invalid {:?} vs. {:?}",
                o_sv_opts.cert_pem,
                n_sv_opts.cert_pem
            )
        }

        if o_sv_opts.key_pem != n_sv_opts.key_pem {
            crucible_bail!(
                ReplaceRequestInvalid,
                "sub_volume opts key_pem invalid {:?} vs. {:?}",
                o_sv_opts.key_pem,
                n_sv_opts.key_pem
            )
        }

        if o_sv_opts.root_cert_pem != n_sv_opts.root_cert_pem {
            crucible_bail!(
                ReplaceRequestInvalid,
                "sub_volume opts root_cert_pem invalid {:?} vs. {:?}",
                o_sv_opts.root_cert_pem,
                n_sv_opts.root_cert_pem
            )
        }

        if o_sv_opts.control != n_sv_opts.control {
            crucible_bail!(
                ReplaceRequestInvalid,
                "sub_volume opts control invalid {:?} vs. {:?}",
                o_sv_opts.control,
                n_sv_opts.control
            )
        }

        if o_sv_opts.read_only != n_sv_opts.read_only {
            crucible_bail!(
                ReplaceRequestInvalid,
                "sub_volume opts read_only invalid {:?} vs. {:?}",
                o_sv_opts.read_only,
                n_sv_opts.read_only
            )
        }

        // Walk the targets, we can only have one different
        let mut new_target_cid = None;
        for cid in 0..3 {
            if o_sv_opts.target[cid] != n_sv_opts.target[cid] {
                if new_target_cid.is_some() {
                    crucible_bail!(
                        ReplaceRequestInvalid,
                        "sub_volume multiple different targets: {:?} vs. {:?}",
                        o_sv_opts.target,
                        n_sv_opts.target
                    )
                } else {
                    new_target_cid = Some(cid);
                }
            }
        }

        // If we found a old/new target, we can return that now.
        if let Some(cid) = new_target_cid {
            Ok(VCRDelta::Target {
                old: o_sv_opts.target[cid],
                new: n_sv_opts.target[cid],
            })
        } else if read_only {
            // VCRs are 100% the same (ignoring generation number changess if
            // read_only bool is true)
            Ok(VCRDelta::Same)
        } else {
            // We failed to find any targets different, but all the other
            // checks between the VCRs found the required differences which
            // includes a generation number bump.
            Ok(VCRDelta::Generation)
        }
    }

    /// Given two VolumeConstructionRequests, compare them to verify they only
    /// have the proper differences and if the VCRs are valid, submit the
    /// targets for replacement. A success here means the upstairs has accepted
    /// the replacement and the process has started, or there's no work to do
    /// because the original and replacement match. This is a &self function
    /// because Volumes do not store the VCR they were created with, and must
    /// accept both the original and replacement as arguments: these must be
    /// properly supplied by the caller.
    pub async fn target_replace(
        &self,
        original: VolumeConstructionRequest,
        replacement: VolumeConstructionRequest,
    ) -> Result<ReplaceResult, CrucibleError> {
        let (original_target, new_target) =
            match Self::compare_vcr_for_target_replacement(
                original,
                replacement,
                &self.log,
            )? {
                ReplacementRequestCheck::Valid { old, new } => (old, new),

                ReplacementRequestCheck::ReplacementMatchesOriginal => {
                    // The replacement VCR they sent is the same the original.
                    // No replacement is required
                    return Ok(ReplaceResult::VcrMatches);
                }
            };

        info!(
            self.log,
            "Volume {}, OK to replace: {original_target} with {new_target}",
            self.uuid
        );

        match self
            .replace_downstairs(self.uuid, original_target, new_target)
            .await
        {
            Ok(ReplaceResult::Missing) => {
                crucible_bail!(
                    ReplaceRequestInvalid,
                    "Volume {} Can't locate {} to replace",
                    self.uuid,
                    original_target,
                )
            }

            Ok(ReplaceResult::Started) => {
                info!(self.log, "Replace downstairs started for {}", self.uuid);

                Ok(ReplaceResult::Started)
            }

            Ok(ReplaceResult::StartedAlready) => {
                info!(
                    self.log,
                    "Replace downstairs already started for {}", self.uuid
                );

                Ok(ReplaceResult::StartedAlready)
            }

            Ok(ReplaceResult::CompletedAlready) => {
                info!(
                    self.log,
                    "Replace downstairs completed already for {}", self.uuid
                );

                Ok(ReplaceResult::CompletedAlready)
            }

            Ok(ReplaceResult::VcrMatches) => {
                info!(
                    self.log,
                    "No work required for replace for {}", self.uuid
                );

                Ok(ReplaceResult::VcrMatches)
            }

            Err(e) => {
                crucible_bail!(
                    ReplaceRequestInvalid,
                    "Replace downstairs failed for {} with {}",
                    self.uuid,
                    e
                )
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    use base64::{engine, Engine};
    use rand::prelude::*;
    use slog::{o, Drain, Logger};
    use tempfile::tempdir;

    // Create a simple logger
    fn csl() -> Logger {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!())
    }

    #[test]
    fn test_single_block() -> Result<()> {
        let (guest, _io) = Guest::new(Some(csl()));
        let sub_volume = SubVolume {
            lba_range: 0..10,
            block_io: Arc::new(guest),
        };

        // Coverage inside region
        assert_eq!(sub_volume.lba_range_coverage(0, 1), Some(0..1));

        Ok(())
    }

    #[test]
    fn test_single_sub_volume_lba_coverage() -> Result<()> {
        let (guest, _io) = Guest::new(Some(csl()));
        let sub_volume = SubVolume {
            lba_range: 0..2048,
            block_io: Arc::new(guest),
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
        let (guest, _io) = Guest::new(Some(csl()));
        let sub_volume = SubVolume {
            lba_range: 1024..2048,
            block_io: Arc::new(guest),
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
        let volume: Volume = VolumeInner {
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
            scrub_point: AtomicU64::new(0),
            block_size: 512,
            count: AtomicU32::new(0),
            log: csl(),
        }
        .into();

        assert_eq!(volume.total_size().await?, 512 * 1024);

        Ok(())
    }

    #[test]
    fn test_affected_subvolumes() -> Result<()> {
        // volume:       |--------|--------|--------|
        //               0        512      1024
        //
        // each sub volume LBA coverage is 512 blocks
        let volume: Volume = VolumeInner {
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
            scrub_point: AtomicU64::new(0),
            block_size: 512,
            count: AtomicU32::new(0),
            log: csl(),
        }
        .into();

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
        let volume: Volume = VolumeInner {
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
            scrub_point: AtomicU64::new(0),
            block_size: 512,
            count: AtomicU32::new(0),
            log: csl(),
        }
        .into();

        assert!(volume.read_only_parent_for_lba_range(0, 512).is_none());

        Ok(())
    }

    #[test]
    fn test_read_only_parent_for_lba_range() -> Result<()> {
        // sub volume:  |-------------------|
        // parent:      |xxxxxxxxx|
        let volume: Volume = VolumeInner {
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
            scrub_point: AtomicU64::new(0),
            block_size: 512,
            count: AtomicU32::new(0),
            log: csl(),
        }
        .into();

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
        let mut buffer = Buffer::new(8, BLOCK_SIZE as usize);
        disk.read(BlockIndex(0), &mut buffer).await?;
        assert_eq!(buffer.into_vec(), vec![0; 4096]);

        // Write ones to second block
        disk.write(BlockIndex(1), BytesMut::from(vec![1; 512].as_slice()))
            .await?;

        // Write unwritten twos to the second block.
        // This should not change the block.
        disk.write_unwritten(
            BlockIndex(1),
            BytesMut::from(vec![2; 512].as_slice()),
        )
        .await?;

        // Read and verify the data is from the first write
        let mut buffer = Buffer::new(8, BLOCK_SIZE as usize);
        disk.read(BlockIndex(0), &mut buffer).await?;

        let mut expected = vec![0; 512];
        expected.extend(vec![1; 512]);
        expected.extend(vec![0; 4096 - 1024]);
        assert_eq!(buffer.into_vec(), expected);

        // Write twos to first block
        disk.write(BlockIndex(0), BytesMut::from(vec![2; 512].as_slice()))
            .await?;

        // Read and verify
        let mut buffer = Buffer::new(8, BLOCK_SIZE as usize);
        disk.read(BlockIndex(0), &mut buffer).await?;

        let mut expected = vec![2; 512];
        expected.extend(vec![1; 512]);
        expected.extend(vec![0; 4096 - 1024]);
        assert_eq!(buffer.into_vec(), expected);

        // Write sevens to third and fourth blocks
        disk.write(BlockIndex(2), BytesMut::from(vec![7; 1024].as_slice()))
            .await?;

        // Write_unwritten eights to fifth and six blocks
        disk.write_unwritten(
            BlockIndex(4),
            BytesMut::from(vec![8; 1024].as_slice()),
        )
        .await?;

        // Read and verify
        let mut buffer = Buffer::new(8, BLOCK_SIZE as usize);
        disk.read(BlockIndex(0), &mut buffer).await?;

        let mut expected = vec![2; 512];
        expected.extend(vec![1; 512]);
        expected.extend(vec![7; 1024]);
        expected.extend(vec![8; 1024]);
        expected.extend(vec![0; 1024]);
        assert_eq!(buffer.into_vec(), expected);

        Ok(())
    }

    // Accept an initialization value so that we can test when the read only
    // parent is uninitialized, and is initialized with a value
    async fn test_parent_read_only_region(
        block_size: u64,
        parent: Arc<dyn BlockIO + Send + Sync>,
        volume: Volume,
        read_only_parent_init_value: u8,
    ) -> Result<()> {
        volume.activate().await?;
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
        let mut buffer = Buffer::new(8, 512);
        volume.read(BlockIndex(0), &mut buffer).await?;

        let mut expected = vec![read_only_parent_init_value; 512 * 4];
        expected.extend(vec![0x00; 512 * 4]);

        assert_eq!(buffer.into_vec(), expected);

        // If the parent volume has data, it should be returned. Write ones to
        // the first block of the parent:
        //
        // parent blocks: 1 n n n
        //    sub volume: - - - - - - - -
        //
        parent
            .write(BlockIndex(0), BytesMut::from(vec![1; 512].as_slice()))
            .await?;

        // Read whole volume and verify
        let mut buffer = Buffer::new(8, 512);
        volume.read(BlockIndex(0), &mut buffer).await?;

        let mut expected = vec![1; 512];
        expected.extend(vec![read_only_parent_init_value; 512 * 3]);
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(buffer.into_vec(), expected);

        // If the volume is written to and it doesn't overlap, still return the
        // parent data. Write twos to the volume:
        //
        // parent blocks: 1 n n n
        //    sub volume: - 2 2 - - - - -
        volume
            .write(BlockIndex(1), BytesMut::from(vec![2; 1024].as_slice()))
            .await?;

        // Make sure the parent data hasn't changed!
        {
            let mut buffer = Buffer::new(4, 512);
            parent.read(BlockIndex(0), &mut buffer).await?;

            let mut expected = vec![1; 512];
            expected.extend(vec![read_only_parent_init_value; 2048 - 512]);

            assert_eq!(buffer.into_vec(), expected);
        }

        // Read whole volume and verify
        let mut buffer = Buffer::new(8, 512);
        volume.read(BlockIndex(0), &mut buffer).await?;

        let mut expected = vec![1; 512];
        expected.extend(vec![2; 512 * 2]);
        expected.extend(vec![read_only_parent_init_value; 512]);
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(buffer.into_vec(), expected);

        // If the volume is written to and it does overlap, return the volume
        // data. Write threes to the volume:
        //
        // parent blocks: 1 n n n
        //    sub volume: 3 3 2 - - - - -
        volume
            .write(BlockIndex(0), BytesMut::from(vec![3; 512 * 2].as_slice()))
            .await?;

        // Make sure the parent data hasn't changed!
        {
            let mut buffer = Buffer::new(4, 512);
            parent.read(BlockIndex(0), &mut buffer).await?;

            let mut expected = vec![1; 512];
            expected.extend(vec![read_only_parent_init_value; 2048 - 512]);

            assert_eq!(buffer.into_vec(), expected);
        }

        // Read whole volume and verify
        let mut buffer = Buffer::new(8, 512);
        volume.read(BlockIndex(0), &mut buffer).await?;

        let mut expected = vec![3; 512 * 2];
        expected.extend(vec![2; 512]);
        expected.extend(vec![read_only_parent_init_value; 512]);
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(buffer.into_vec(), expected);

        // If the whole parent is now written to, only the last block should be
        // returned. Write fours to the parent
        //
        // parent blocks: 4 4 4 4
        //    sub volume: 3 3 2 - - - - -
        parent
            .write(BlockIndex(0), BytesMut::from(vec![4; 2048].as_slice()))
            .await?;

        // Read whole volume and verify
        let mut buffer = Buffer::new(8, 512);
        volume.read(BlockIndex(0), &mut buffer).await?;

        let mut expected = vec![3; 512 * 2];
        expected.extend(vec![2; 512]);
        expected.extend(vec![4; 512]);
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(buffer.into_vec(), expected);

        // If the parent goes away, then the sub volume data should still be
        // readable.
        let mut volume = Arc::try_unwrap(volume.0).unwrap();
        volume.read_only_parent = None;
        let volume = Volume::from(volume);

        // Read whole volume and verify
        let mut buffer = Buffer::new(8, 512);
        volume.read(BlockIndex(0), &mut buffer).await?;

        let mut expected = vec![3; 512 * 2];
        expected.extend(vec![2; 512]);
        expected.extend(vec![0; 512]); // <- was previously from parent, now "-"
        expected.extend(vec![0x00; 512 * 4]); // <- from subvolume, still "-"

        assert_eq!(buffer.into_vec(), expected);

        // Write to the whole volume. There's no more read-only parent (it was
        // dropped above)
        volume
            .write(BlockIndex(0), BytesMut::from(vec![9; 4096].as_slice()))
            .await?;

        let mut buffer = Buffer::new(8, 512);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(buffer.into_vec(), vec![9; 4096]);

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
                .write(BlockIndex(0), BytesMut::from(vec![i; 2048].as_slice()))
                .await?;

            // this layout has one volume that the parent lba range overlaps:
            //
            // volumes: 0 0 0 0 0 0 0 0
            //  parent: P P P P
            //
            // the total volume size is 4096b

            let mut builder = VolumeBuilder::new(BLOCK_SIZE, csl());
            builder.add_subvolume(disk).await?;
            builder.add_read_only_parent(parent.clone()).await?;
            let volume = Volume::from(builder);

            test_parent_read_only_region(BLOCK_SIZE, parent, volume, i).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn ownership_survives_through_volume() -> Result<()> {
        const BLOCK_SIZE: u64 = 512;

        let parent =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));

        // Ownership starts off false

        let mut buffer = Buffer::new(2, BLOCK_SIZE as usize);
        parent.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(buffer.owned_ref(), &[0, 0]);

        // Ownership is set by writing to blocks

        parent
            .write(BlockIndex(0), BytesMut::from(vec![9; 512].as_slice()))
            .await?;

        // Ownership is returned by the downstairs

        let mut buffer = Buffer::new(2, BLOCK_SIZE as usize);
        parent.read(BlockIndex(0), &mut buffer).await?;

        let mut expected = vec![9u8; 512];
        expected.extend(vec![0u8; 512]);

        assert_eq!(&*buffer, &expected);
        let expected_ownership = [1, 0];
        assert_eq!(buffer.owned_ref(), &expected_ownership);

        // Ownership through a volume should be the same!!

        let disk =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 4096));

        let mut builder = VolumeBuilder::new(BLOCK_SIZE, csl());
        builder.add_subvolume(disk).await?;
        builder.add_read_only_parent(parent.clone()).await?;
        let volume = Volume::from(builder);

        // So is it?

        let mut buffer = Buffer::new(2, BLOCK_SIZE as usize);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(&*buffer, &expected);
        assert_eq!(buffer.owned_ref(), &expected_ownership);

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

        let mut builder = VolumeBuilder::new(BLOCK_SIZE, csl());
        assert!(!builder.0.has_read_only_parent());
        builder.add_subvolume(disk).await?;
        assert!(!builder.0.has_read_only_parent());
        builder.add_read_only_parent(parent.clone()).await?;
        assert!(builder.0.has_read_only_parent());
        let volume = Volume::from(builder);

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
                .write(BlockIndex(0), BytesMut::from(vec![i; 2048].as_slice()))
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

            let mut builder = VolumeBuilder::new(BLOCK_SIZE, csl());
            builder.add_subvolume(subdisk1).await?;
            builder.add_subvolume(subdisk2).await?;
            builder.add_read_only_parent(parent.clone()).await?;
            let volume = Volume::from(builder);

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

        let mut builder = VolumeBuilder::new(BLOCK_SIZE, csl());
        builder.add_subvolume(subdisk1).await?;
        builder.add_subvolume(subdisk2).await?;
        builder.add_read_only_parent(parent.clone()).await?;
        let volume = Volume::from(builder);

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
                .write(BlockIndex(0), BytesMut::from(vec![i; 2048].as_slice()))
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

            let mut builder = VolumeBuilder::new(BLOCK_SIZE, csl());
            builder.add_subvolume(subdisk1).await?;
            builder.add_subvolume(subdisk2).await?;
            builder.add_read_only_parent(parent.clone()).await?;
            let volume = Volume::from(builder);

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

        let mut builder = VolumeBuilder::new(BLOCK_SIZE, csl());
        builder.add_subvolume(subdisk1).await?;
        builder.add_subvolume(subdisk2).await?;
        builder.add_read_only_parent(parent.clone()).await?;
        let volume = Volume::from(builder);

        test_parent_read_only_region(BLOCK_SIZE, parent, volume, 0x00).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_volume_with_only_read_only_parent() -> Result<()> {
        const BLOCK_SIZE: u64 = 512;

        let parent =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));

        parent.activate().await?;

        // Write 0x80 into parent
        parent
            .write(BlockIndex(0), BytesMut::from(vec![128; 2048].as_slice()))
            .await?;

        let volume: Volume = VolumeInner {
            uuid: Uuid::new_v4(),
            sub_volumes: vec![],
            read_only_parent: Some(SubVolume {
                lba_range: Range {
                    start: 0,
                    end: parent.total_size().await? / BLOCK_SIZE,
                },
                block_io: parent.clone(),
            }),
            scrub_point: AtomicU64::new(0),
            block_size: BLOCK_SIZE,
            count: AtomicU32::new(0),
            log: csl(),
        }
        .into();

        volume.activate().await?;

        assert_eq!(volume.total_size().await?, 2048);

        // Read volume and verify contents
        let mut buffer = Buffer::new(4, BLOCK_SIZE as usize);
        volume.read(BlockIndex(0), &mut buffer).await?;

        let expected = vec![128; 2048];

        assert_eq!(buffer.into_vec(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_writing_to_volume_with_only_read_only_parent() {
        const BLOCK_SIZE: u64 = 512;

        let parent =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));

        let volume: Volume = VolumeInner {
            uuid: Uuid::new_v4(),
            sub_volumes: vec![],
            read_only_parent: Some(SubVolume {
                lba_range: Range {
                    start: 0,
                    end: parent.total_size().await.unwrap() / BLOCK_SIZE,
                },
                block_io: parent.clone(),
            }),
            scrub_point: AtomicU64::new(0),
            block_size: BLOCK_SIZE,
            count: AtomicU32::new(0),
            log: csl(),
        }
        .into();

        volume.activate().await.unwrap();

        // Write 0x80 into volume - this will error
        let res = volume
            .write(BlockIndex(0), BytesMut::from(vec![128; 2048].as_slice()))
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_write_unwritten_to_volume_with_only_read_only_parent() {
        const BLOCK_SIZE: u64 = 512;

        let parent =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));

        let volume: Volume = VolumeInner {
            uuid: Uuid::new_v4(),
            sub_volumes: vec![],
            read_only_parent: Some(SubVolume {
                lba_range: Range {
                    start: 0,
                    end: parent.total_size().await.unwrap() / BLOCK_SIZE,
                },
                block_io: parent.clone(),
            }),
            scrub_point: AtomicU64::new(0),
            block_size: BLOCK_SIZE,
            count: AtomicU32::new(0),
            log: csl(),
        }
        .into();

        volume.activate().await.unwrap();

        // Write 0x80 into volume - this will error
        let res = volume
            .write_unwritten(
                BlockIndex(0),
                BytesMut::from(vec![128; 2048].as_slice()),
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
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 10].as_slice()),
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
            let mut builder = VolumeBuilder::new(BLOCK_SIZE as u64, csl());
            builder.add_subvolume(overlay.clone()).await?;
            builder.add_read_only_parent(parent.clone()).await?;
            let volume = Volume::from(builder);

            let mut buffer = Buffer::new(10, BLOCK_SIZE);
            volume.read(BlockIndex(0), &mut buffer).await?;

            assert_eq!(vec![0x55; BLOCK_SIZE * 10], buffer.into_vec());

            volume
                .write(
                    BlockIndex(0),
                    BytesMut::from(vec![0xFF; BLOCK_SIZE * 10].as_slice()),
                )
                .await?;

            let mut buffer = Buffer::new(10, BLOCK_SIZE);
            volume.read(BlockIndex(0), &mut buffer).await?;

            assert_eq!(vec![0xFF; BLOCK_SIZE * 10], buffer.into_vec());
        }

        // Create the same volume, verify data was written
        // Note that add function order is reversed, it shouldn't matter
        let mut builder = VolumeBuilder::new(BLOCK_SIZE as u64, csl());
        builder.add_read_only_parent(parent).await?;
        builder.add_subvolume(overlay).await?;
        let volume = Volume::from(builder);

        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0xFF; BLOCK_SIZE * 10], buffer.into_vec());

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
                BlockIndex(0),
                BytesMut::from(vec![0x55; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read parent, verify contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        parent.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x55; BLOCK_SIZE * 10], buffer.into_vec());

        let mut parent_builder = VolumeBuilder::new(BLOCK_SIZE as u64, csl());
        parent_builder.add_subvolume(parent).await?;
        let parent_volume = Volume::from(parent_builder);

        let overlay = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            BLOCK_SIZE as u64,
            BLOCK_SIZE * 10,
        ));

        let mut builder = VolumeBuilder::new(BLOCK_SIZE as u64, csl());
        builder.add_subvolume(overlay).await?;
        builder
            .add_read_only_parent(parent_volume.as_blockio())
            .await?;
        let volume = Volume::from(builder);

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
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0x55; BLOCK_SIZE * 10], buffer.into_vec());

        // Write over whole volume
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![0xFF; BLOCK_SIZE * 10].as_slice()),
            )
            .await?;

        // Read whole volume, verify new contents
        let mut buffer = Buffer::new(10, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![0xFF; BLOCK_SIZE * 10], buffer.into_vec());

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
        let volume = Volume::construct(request, None, csl()).await.unwrap();

        let mut buffer = Buffer::new(1, BLOCK_SIZE);
        volume.read(BlockIndex(0), &mut buffer).await.unwrap();

        assert_eq!(vec![0x0; BLOCK_SIZE], buffer.into_vec());

        let mut buffer = Buffer::new(1, BLOCK_SIZE);
        volume.read(BlockIndex(1), &mut buffer).await.unwrap();

        assert_eq!(vec![0x5; BLOCK_SIZE], buffer.into_vec());
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
                BlockIndex(0),
                BytesMut::from(vec![11; block_size * 5].as_slice()),
            )
            .await?;

        // Read back parent, verify 1s
        let mut buffer = Buffer::new(5, block_size);
        parent.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![11; block_size * 5], buffer.into_vec());

        // Create a volume out of this parent and the argument subvolume parts
        let mut builder = VolumeBuilder::new(block_size as u64, csl());

        for subvolume in subvolumes {
            builder.add_subvolume(subvolume.clone()).await?;
        }

        builder.add_read_only_parent(parent).await?;
        let volume = Volume::from(builder);

        volume.activate().await?;

        assert_eq!(volume.total_size().await?, block_size as u64 * 10);

        // Verify volume contents in one read
        let mut buffer = Buffer::new(10, block_size);
        volume.read(BlockIndex(0), &mut buffer).await?;

        let mut expected = vec![11; block_size * 5];
        expected.extend(vec![0x00; block_size * 5]);
        assert_eq!(expected, buffer.into_vec());

        // One big write!
        volume
            .write(
                BlockIndex(0),
                BytesMut::from(vec![55; block_size * 10].as_slice()),
            )
            .await?;

        // Verify volume contents in one read
        let mut buffer = Buffer::new(10, block_size);
        volume.read(BlockIndex(0), &mut buffer).await?;

        assert_eq!(vec![55; block_size * 10], buffer.into_vec());

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
        let mut builder = VolumeBuilder::new(block_size as u64, csl());

        // Create the subvolume(s) of the requested size(s)
        for size in subvol_sizes {
            let subvolume = Arc::new(InMemoryBlockIO::new(
                Uuid::new_v4(),
                block_size as u64,
                block_size * size,
            ));
            builder.add_subvolume(subvolume.clone()).await?;
        }

        // Create the read only parent
        let parent = Arc::new(InMemoryBlockIO::new(
            Uuid::new_v4(),
            block_size as u64,
            block_size * parent_blocks,
        ));

        builder.add_read_only_parent(parent.clone()).await?;
        let volume = Volume::from(builder);
        volume.activate().await?;

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
                    } else if scrub_point >= parent_blocks {
                        // We have completed the scrub of the ROP, so we
                        // go directly to the SubVolume
                        println!(
                            "scrub_point {} >= size {}, scrub done. No check",
                            scrub_point, parent_blocks
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

    /// Confirm that an out-of-bounds read or write will return an error
    #[tokio::test]
    async fn test_out_of_bounds() {
        const BLOCK_SIZE: u64 = 512;

        let parent =
            Arc::new(InMemoryBlockIO::new(Uuid::new_v4(), BLOCK_SIZE, 2048));

        let volume: Volume = VolumeInner {
            uuid: Uuid::new_v4(),
            sub_volumes: vec![SubVolume {
                lba_range: Range {
                    start: 0,
                    end: parent.total_size().await.unwrap() / BLOCK_SIZE,
                },
                block_io: parent.clone(),
            }],
            read_only_parent: None,
            scrub_point: AtomicU64::new(0),
            block_size: BLOCK_SIZE,
            count: AtomicU32::new(0),
            log: csl(),
        }
        .into();

        volume.activate().await.unwrap();

        let out_of_bounds = volume.total_size().await.unwrap() * BLOCK_SIZE;

        let mut buffer = Buffer::new(1, BLOCK_SIZE as usize);
        let res = volume
            .read(BlockIndex(out_of_bounds / BLOCK_SIZE), &mut buffer)
            .await;

        assert!(matches!(res, Err(CrucibleError::OffsetInvalid)));

        let res = volume
            .write(
                BlockIndex(out_of_bounds / BLOCK_SIZE),
                BytesMut::from(vec![128; 2048].as_slice()),
            )
            .await;

        assert!(matches!(res, Err(CrucibleError::OffsetInvalid)));

        let res = volume
            .write_unwritten(
                BlockIndex(out_of_bounds / BLOCK_SIZE),
                BytesMut::from(vec![128; 2048].as_slice()),
            )
            .await;

        assert!(matches!(res, Err(CrucibleError::OffsetInvalid)));
    }

    // Return a generic set of CrucibleOpts
    fn generic_crucible_opts(vol_id: Uuid) -> CrucibleOpts {
        // Generate random data for our key
        let key_bytes = rand::thread_rng().gen::<[u8; 32]>();
        let key_string = engine::general_purpose::STANDARD.encode(key_bytes);

        CrucibleOpts {
            id: vol_id,
            target: vec![
                "127.0.0.1:5555".parse().unwrap(),
                "127.0.0.1:6666".parse().unwrap(),
                "127.0.0.1:7777".parse().unwrap(),
            ],
            lossy: false,
            flush_timeout: None,
            key: Some(key_string),
            cert_pem: None,
            key_pem: None,
            root_cert_pem: None,
            control: None,
            read_only: false,
        }
    }

    // For tests to decide what mix of read_only_parents the two VCRs should
    // be created with.
    #[derive(Debug, PartialEq)]
    enum ReadOnlyParentMode {
        Both,
        OnlyOriginal,
        OnlyReplacement,
        Neither,
    }

    #[test]
    fn volume_replace_basic() {
        // A valid replacement VCR is provided with only one target being
        // different.
        // Test all three targets for replacement.
        // Test with 1, 2, and 3 sub_volumes.
        // Test with the difference being in each sub_volume.
        // Test all combinations of read_only_parents.
        for sv in 1..4 {
            for sv_changed in 0..sv {
                for cid in 0..3 {
                    test_volume_replace_inner(
                        sv,
                        sv_changed,
                        cid,
                        ReadOnlyParentMode::Both,
                    )
                    .unwrap();
                    test_volume_replace_inner(
                        sv,
                        sv_changed,
                        cid,
                        ReadOnlyParentMode::OnlyOriginal,
                    )
                    .unwrap();
                    test_volume_replace_inner(
                        sv,
                        sv_changed,
                        cid,
                        ReadOnlyParentMode::OnlyReplacement,
                    )
                    .unwrap_err();
                    test_volume_replace_inner(
                        sv,
                        sv_changed,
                        cid,
                        ReadOnlyParentMode::Neither,
                    )
                    .unwrap();
                }
            }
        }
    }

    // Helper function to build as many sv_count sub-volumes as requested.
    fn build_subvolume_vcr(
        sv_count: usize,
        block_size: u64,
        blocks_per_extent: u64,
        extent_count: u32,
        opts: CrucibleOpts,
        gen: u64,
    ) -> Vec<VolumeConstructionRequest> {
        (0..sv_count)
            .map(|_| VolumeConstructionRequest::Region {
                block_size,
                blocks_per_extent,
                extent_count,
                opts: opts.clone(),
                gen,
            })
            .collect()
    }

    // Take an existing VCR (required to be VCR::Region), and fabricate a
    // new set of VCRs from it.  We change the generation number of all the
    // new sub_volumes, but only change the requested target in one requested
    // sub_volume at the requested client ID.
    fn build_replacement_subvol(
        sv_count: usize,
        sv_changed: usize,
        source_vcr: VolumeConstructionRequest,
        cid: usize,
    ) -> Vec<VolumeConstructionRequest> {
        let new_target: SocketAddr = "127.0.0.1:8888".parse().unwrap();

        let (block_size, blocks_per_extent, extent_count, opts, gen) =
            match source_vcr {
                VolumeConstructionRequest::Region {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                    opts,
                    gen,
                } => {
                    (block_size, blocks_per_extent, extent_count, opts, gen + 1)
                }
                _ => {
                    panic!("Unsupported VCR");
                }
            };

        (0..sv_count)
            .map(|i| {
                let mut replacement_opts = opts.clone();

                if i == sv_changed {
                    replacement_opts.target[cid] = new_target;
                }

                VolumeConstructionRequest::Region {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                    opts: replacement_opts.clone(),
                    gen,
                }
            })
            .collect()
    }

    // A basic replacement of a target in a sub_volume, with options to
    // create multiple sub_volumes, and to select which sub_volume and specific
    // target to be different.
    //
    // sv_count: The number of sub volumes we create in each volume.
    // sv_changed: The index (zero based) of the sub volume where we
    // want the target to be different.
    // cid: The client ID where the change will happen, which is the
    // same as the index in the crucible opts target array.
    // rop_mode: enum of which VCR will have a ROP and which will not.
    fn test_volume_replace_inner(
        sv_count: usize,
        sv_changed: usize,
        cid: usize,
        rop_mode: ReadOnlyParentMode,
    ) -> Result<()> {
        assert!(sv_count > sv_changed);
        assert!(cid < 3);
        // A valid replacement VCR is provided with a larger generation
        // number and only one target being different.
        let vol_id = Uuid::new_v4();
        let opts = generic_crucible_opts(vol_id);

        let block_size = 512;
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let test_rop = Box::new(VolumeConstructionRequest::Region {
            block_size,
            blocks_per_extent,
            extent_count,
            opts: opts.clone(),
            gen: 3,
        });

        let (original_rop, replacement_rop) = match rop_mode {
            ReadOnlyParentMode::Both => {
                (Some(test_rop.clone()), Some(test_rop))
            }
            ReadOnlyParentMode::OnlyOriginal => (Some(test_rop), None),
            ReadOnlyParentMode::OnlyReplacement => (None, Some(test_rop)),
            ReadOnlyParentMode::Neither => (None, None),
        };

        let sub_volumes = build_subvolume_vcr(
            sv_count,
            block_size,
            blocks_per_extent,
            extent_count,
            opts.clone(),
            2,
        );

        let original = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes: sub_volumes.clone(),
            read_only_parent: original_rop,
        };

        // Change just the minimum things and use the updated values
        // in the replacement volume.
        let original_target = opts.target[cid];
        let new_target: SocketAddr = "127.0.0.1:8888".parse().unwrap();

        let replacement_sub_volumes = build_replacement_subvol(
            sv_count,
            sv_changed,
            sub_volumes[0].clone(),
            cid,
        );

        let replacement = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes: replacement_sub_volumes,
            read_only_parent: replacement_rop,
        };

        let log = csl();
        info!(log,
            "replacement of CID {} with sv_count:{} sv_changed:{} rop_mode:{:?}",
            cid, sv_count, sv_changed, rop_mode
        );
        let ReplacementRequestCheck::Valid { old, new } =
            Volume::compare_vcr_for_target_replacement(
                original,
                replacement,
                &log,
            )?
        else {
            panic!("wrong variant returned!");
        };

        info!(log, "replaced {old} with {new}");
        assert_eq!(original_target, old);
        assert_eq!(new_target, new);
        Ok(())
    }

    #[test]
    fn test_volume_replace_no_gen() {
        // We need at least two sub_volumes for this test.
        for sv in 2..4 {
            for sv_changed in 0..sv {
                test_volume_replace_no_gen_inner(
                    sv,
                    sv_changed,
                    ReadOnlyParentMode::Both,
                );
                test_volume_replace_no_gen_inner(
                    sv,
                    sv_changed,
                    ReadOnlyParentMode::OnlyOriginal,
                );
                test_volume_replace_no_gen_inner(
                    sv,
                    sv_changed,
                    ReadOnlyParentMode::OnlyReplacement,
                );
                test_volume_replace_no_gen_inner(
                    sv,
                    sv_changed,
                    ReadOnlyParentMode::Neither,
                );
            }
        }
    }

    fn test_volume_replace_no_gen_inner(
        sv_count: usize,
        sv_changed: usize,
        rop_mode: ReadOnlyParentMode,
    ) {
        assert!(sv_count > sv_changed);
        // A replacement VCR is created with a single sub_volume that has
        // a new target and a larger generation, but the other sub_volumes do
        // not have increased generation numbers, which is not valid
        // We require that all sub_volumes have bumped generation numbers.
        let vol_id = Uuid::new_v4();
        let opts = generic_crucible_opts(vol_id);

        let block_size = 512;
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let test_rop = Box::new(VolumeConstructionRequest::Region {
            block_size,
            blocks_per_extent,
            extent_count,
            opts: opts.clone(),
            gen: 3,
        });

        let (original_rop, replacement_rop) = match rop_mode {
            ReadOnlyParentMode::Both => {
                (Some(test_rop.clone()), Some(test_rop))
            }
            ReadOnlyParentMode::OnlyOriginal => (Some(test_rop), None),
            ReadOnlyParentMode::OnlyReplacement => (None, Some(test_rop)),
            ReadOnlyParentMode::Neither => (None, None),
        };

        let sub_volumes = build_subvolume_vcr(
            sv_count,
            block_size,
            blocks_per_extent,
            extent_count,
            opts.clone(),
            2,
        );

        let original = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes,
            read_only_parent: original_rop,
        };

        // Change just the target and gen for one subvolume, but not the others.
        let new_target: SocketAddr = "127.0.0.1:8888".parse().unwrap();

        let replacement_sub_volumes = (0..sv_count)
            .map(|i| {
                let mut replacement_opts = opts.clone();
                let mut replacement_gen = 2;

                if i == sv_changed {
                    replacement_opts.target[1] = new_target;
                    replacement_gen += 1;
                }

                VolumeConstructionRequest::Region {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                    opts: replacement_opts.clone(),
                    gen: replacement_gen,
                }
            })
            .collect();

        let replacement = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes: replacement_sub_volumes,
            read_only_parent: replacement_rop,
        };

        let log = csl();
        info!(
            log,
            "replacement of with sv_count:{} sv_changed:{} rop_mode:{:?}",
            sv_count,
            sv_changed,
            rop_mode,
        );
        assert!(Volume::compare_vcr_for_target_replacement(
            original,
            replacement,
            &log,
        )
        .is_err());
    }

    #[tokio::test]
    async fn volume_replace_drop_rop() {
        // A replacement VCR is provided with one target being different, The
        // original has a read only parent while the replacement does not, and
        // that is okay.
        let block_size = 512;
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let mut opts = generic_crucible_opts(vol_id);

        // Create the read only parent
        let rop = Box::new(VolumeConstructionRequest::Region {
            block_size,
            blocks_per_extent,
            extent_count,
            opts: opts.clone(),
            gen: 3,
        });

        let original = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size,
                blocks_per_extent,
                extent_count,
                opts: opts.clone(),
                gen: 2,
            }],
            read_only_parent: Some(rop),
        };

        let original_target = opts.target[1];
        let new_target: SocketAddr = "127.0.0.1:8888".parse().unwrap();
        opts.target[1] = new_target;

        let replacement = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size,
                blocks_per_extent,
                extent_count,
                opts: opts.clone(),
                gen: 3,
            }],
            read_only_parent: None,
        };

        let log = csl();
        let ReplacementRequestCheck::Valid { old, new } =
            Volume::compare_vcr_for_target_replacement(
                original,
                replacement,
                &log,
            )
            .unwrap()
        else {
            panic!("wrong variant returned!");
        };

        assert_eq!(original_target, old);
        assert_eq!(new_target, new);
    }

    #[test]
    // Send the same VCR as both old and new, this should return error
    // because the generation number did not change.
    // Test with 1, 2, and 3 sub_volumes.
    // Test both with and without a read_only_parent.
    fn volume_replace_self() {
        for sv in 1..4 {
            test_volume_replace_self_inner(sv, ReadOnlyParentMode::Both);
            test_volume_replace_self_inner(sv, ReadOnlyParentMode::Neither);
        }
    }

    fn test_volume_replace_self_inner(
        sv_count: usize,
        rop_mode: ReadOnlyParentMode,
    ) {
        let block_size = 512;
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let opts = generic_crucible_opts(vol_id);

        let rop = match rop_mode {
            ReadOnlyParentMode::Both => {
                let test_rop = Box::new(VolumeConstructionRequest::Region {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                    opts: opts.clone(),
                    gen: 3,
                });
                Some(test_rop)
            }
            ReadOnlyParentMode::Neither => None,
            ReadOnlyParentMode::OnlyOriginal
            | ReadOnlyParentMode::OnlyReplacement => {
                panic!("Unsupported test mode");
            }
        };

        let sub_volumes = build_subvolume_vcr(
            sv_count,
            block_size,
            blocks_per_extent,
            extent_count,
            opts.clone(),
            2,
        );
        let original = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes,
            read_only_parent: rop,
        };

        let log = csl();

        assert!(Volume::compare_vcr_for_target_replacement(
            original.clone(),
            original,
            &log
        )
        .is_err());
    }

    #[test]
    // A replacement VCR is provided with differing gen, but the same targets.
    // This is valid for a migration, but not valid for a target replacement.
    // We test both calls here.
    // Test with 1, 2, and 3 sub_volumes.
    // Test with all combinations of read_only_parent, knowing that the
    // OnlyReplacement case should always fail.
    fn volume_vcr_no_targets() {
        for sv in 1..3 {
            volume_vcr_no_targets_inner(sv, ReadOnlyParentMode::Both);
            volume_vcr_no_targets_inner(sv, ReadOnlyParentMode::OnlyOriginal);
            volume_vcr_no_targets_inner(
                sv,
                ReadOnlyParentMode::OnlyReplacement,
            );
            volume_vcr_no_targets_inner(sv, ReadOnlyParentMode::Neither);
        }
    }

    fn volume_vcr_no_targets_inner(
        sv_count: usize,
        rop_mode: ReadOnlyParentMode,
    ) {
        let block_size = 512;
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let opts = generic_crucible_opts(vol_id);
        let test_rop = Box::new(VolumeConstructionRequest::Region {
            block_size,
            blocks_per_extent,
            extent_count,
            opts: opts.clone(),
            gen: 3,
        });

        let (original_rop, replacement_rop) = match rop_mode {
            ReadOnlyParentMode::Both => {
                (Some(test_rop.clone()), Some(test_rop))
            }
            ReadOnlyParentMode::OnlyOriginal => (Some(test_rop), None),
            ReadOnlyParentMode::OnlyReplacement => (None, Some(test_rop)),
            ReadOnlyParentMode::Neither => (None, None),
        };

        let sub_volumes = build_subvolume_vcr(
            sv_count,
            block_size,
            blocks_per_extent,
            extent_count,
            opts.clone(),
            2,
        );

        let original = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes,
            read_only_parent: original_rop,
        };

        let replacement_sub_volumes = (0..sv_count)
            .map(|_| VolumeConstructionRequest::Region {
                block_size,
                blocks_per_extent,
                extent_count,
                opts: opts.clone(),
                gen: 3,
            })
            .collect();

        let replacement = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes: replacement_sub_volumes,
            read_only_parent: replacement_rop,
        };

        let log = csl();

        // Replacement should return ReplacementMatchesOriginal
        let res = Volume::compare_vcr_for_target_replacement(
            original.clone(),
            replacement.clone(),
            &log,
        );
        if rop_mode == ReadOnlyParentMode::OnlyReplacement {
            assert!(res.is_err());
        } else {
            assert!(matches!(
                res,
                Ok(ReplacementRequestCheck::ReplacementMatchesOriginal)
            ));
        };

        // Migration is valid with these VCRs
        let res =
            Volume::compare_vcr_for_migration(original, replacement, &log);
        if rop_mode == ReadOnlyParentMode::OnlyReplacement {
            assert!(res.is_err());
        } else {
            assert!(res.is_ok());
        };
    }

    #[test]
    // A replacement VCR is provided with one target being different, but with
    // incorrect volume layer block size
    // Test with 1, 2, and 3 sub_volumes.
    // Test both with each sub_volume having the difference.
    fn volume_replace_mismatch_vblock() {
        for sv in 1..4 {
            for sv_changed in 0..sv {
                volume_replace_mismatch_vblock_inner(sv, sv_changed);
            }
        }
    }

    fn volume_replace_mismatch_vblock_inner(
        sv_count: usize,
        sv_changed: usize,
    ) {
        assert!(sv_count > sv_changed);
        let block_size = 512;
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let opts = generic_crucible_opts(vol_id);

        let sub_volumes = build_subvolume_vcr(
            sv_count,
            block_size,
            blocks_per_extent,
            extent_count,
            opts.clone(),
            2,
        );

        let replacement_sub_volumes = build_replacement_subvol(
            sv_count,
            sv_changed,
            sub_volumes[0].clone(),
            1,
        );

        let original = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes,
            read_only_parent: None,
        };

        let replacement = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size: 4096,
            sub_volumes: replacement_sub_volumes,
            read_only_parent: None,
        };

        let log = csl();
        info!(
            log,
            "block_size mismatch with sv_count:{} sv_changed:{}",
            sv_count,
            sv_changed,
        );

        assert!(Volume::compare_vcr_for_target_replacement(
            original,
            replacement,
            &log
        )
        .is_err());
    }

    #[test]
    // A replacement VCR is provided with one target being different, but with
    // incorrect volume layer volume id
    // Test with 1, 2, and 3 sub_volumes.
    // Test both with each sub_volume having the difference.
    fn volume_replace_mismatch_vid() {
        for sv in 1..4 {
            for sv_changed in 0..sv {
                volume_replace_mismatch_vid_inner(sv, sv_changed);
            }
        }
    }

    fn volume_replace_mismatch_vid_inner(sv_count: usize, sv_changed: usize) {
        assert!(sv_count > sv_changed);
        let block_size = 512;
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let opts = generic_crucible_opts(vol_id);

        let sub_volumes = build_subvolume_vcr(
            sv_count,
            block_size,
            blocks_per_extent,
            extent_count,
            opts.clone(),
            2,
        );

        let replacement_sub_volumes = build_replacement_subvol(
            sv_count,
            sv_changed,
            sub_volumes[0].clone(),
            1,
        );

        let original = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes,
            read_only_parent: None,
        };

        let replacement = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size,
            sub_volumes: replacement_sub_volumes,
            read_only_parent: None,
        };

        let log = csl();
        info!(
            log,
            "volume id mismatch with sv_count:{} sv_changed:{}",
            sv_count,
            sv_changed,
        );
        assert!(Volume::compare_vcr_for_target_replacement(
            original,
            replacement,
            &log
        )
        .is_err());
    }

    #[test]
    // A replacement VCR is provided with one target being different, but with
    // the replacement volume having a read only parent, which is not allowed.
    // Test with 1, 2, and 3 sub_volumes.
    // Test both with each sub_volume having the difference.
    fn volume_replace_mismatch_vrop() {
        for sv in 1..4 {
            for sv_changed in 0..sv {
                volume_replace_mismatch_vrop_inner(sv, sv_changed);
            }
        }
    }

    fn volume_replace_mismatch_vrop_inner(sv_count: usize, sv_changed: usize) {
        assert!(sv_count > sv_changed);
        let block_size = 512;
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let opts = generic_crucible_opts(vol_id);

        let sub_volumes = build_subvolume_vcr(
            sv_count,
            block_size,
            blocks_per_extent,
            extent_count,
            opts.clone(),
            2,
        );

        let replacement_sub_volumes = build_replacement_subvol(
            sv_count,
            sv_changed,
            sub_volumes[0].clone(),
            1,
        );

        let original = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes,
            read_only_parent: None,
        };

        // Replacement can't have a read_only_parent
        let replacement = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes: replacement_sub_volumes,
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                    opts: opts.clone(),
                    gen: 3,
                },
            )),
        };

        let log = csl();
        info!(
            log,
            "volume no new ROP with sv_count:{} sv_changed:{}",
            sv_count,
            sv_changed,
        );
        assert!(Volume::compare_vcr_for_target_replacement(
            original,
            replacement,
            &log
        )
        .is_err());
    }

    #[test]
    // Verify VCR replacement passes with a valid difference in
    // just the read_only_parents.
    // Test with 1, 2, and 3 sub_volumes.
    fn volume_replace_read_only_parent_ok() {
        for sv in 1..4 {
            volume_replace_read_only_parent_ok_inner(sv);
        }
    }

    fn volume_replace_read_only_parent_ok_inner(sv_count: usize) {
        let block_size = 512;
        let vol_id = Uuid::new_v4();
        let rop_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let opts = generic_crucible_opts(vol_id);

        // Make the sub_volume(s).
        let sub_volumes = build_subvolume_vcr(
            sv_count,
            block_size,
            blocks_per_extent,
            extent_count,
            opts.clone(),
            2,
        );

        // Make the read only parent.
        let mut rop_opts = generic_crucible_opts(rop_id);

        // Make the original VCR using what we created above.
        let original = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes,
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                    opts: rop_opts.clone(),
                    gen: 4,
                },
            )),
        };

        // Update the ROP target with a new downstairs
        let original_target = rop_opts.target[1];
        let new_target: SocketAddr = "127.0.0.1:8888".parse().unwrap();
        rop_opts.target[1] = new_target;

        let sub_volumes = build_subvolume_vcr(
            sv_count,
            block_size,
            blocks_per_extent,
            extent_count,
            opts.clone(),
            3,
        );
        // Make the replacement VCR with the updated target for the
        // read_only_parent.
        let replacement = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes,
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                    opts: rop_opts.clone(),
                    gen: 4,
                },
            )),
        };

        let log = csl();
        let ReplacementRequestCheck::Valid { old, new } =
            Volume::compare_vcr_for_target_replacement(
                original,
                replacement,
                &log,
            )
            .unwrap()
        else {
            panic!("wrong variant returned!");
        };

        assert_eq!(original_target, old);
        assert_eq!(new_target, new);
    }

    #[test]
    // Verify that a valid diff in both the sub_volumes and the
    // read_only_parent returns an error (we can do only one at a time).
    // Test with 1, 2, and 3 sub_volumes.
    // Test both with each sub_volume having the difference.
    fn volume_replace_with_both_subvol_and_rop_diff() {
        for sv in 1..4 {
            for sv_changed in 0..sv {
                volume_replace_with_both_subvol_and_rop_diff_inner(
                    sv, sv_changed,
                );
            }
        }
    }

    fn volume_replace_with_both_subvol_and_rop_diff_inner(
        sv_count: usize,
        sv_changed: usize,
    ) {
        assert!(sv_count > sv_changed);
        let block_size = 512;
        let vol_id = Uuid::new_v4();
        let rop_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        // Make the sub_volume(s) that both VCRs will share.
        let opts = generic_crucible_opts(vol_id);
        let sub_volumes = build_subvolume_vcr(
            sv_count,
            block_size,
            blocks_per_extent,
            extent_count,
            opts.clone(),
            2,
        );

        // Make the read only parent.
        let mut rop_opts = generic_crucible_opts(rop_id);
        let rop = VolumeConstructionRequest::Region {
            block_size,
            blocks_per_extent,
            extent_count,
            opts: rop_opts.clone(),
            gen: 4,
        };

        let replacement_sub_volumes = build_replacement_subvol(
            sv_count,
            sv_changed,
            sub_volumes[0].clone(),
            1,
        );

        // Make the original VCR using what we created above.
        let original = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes,
            read_only_parent: Some(Box::new(rop.clone())),
        };

        // Update the ROP target with a new downstairs
        rop_opts.target[1] = "127.0.0.1:8888".parse().unwrap();

        // Make the replacement VCR with the updated targets and bump
        // the generation numbers for both.
        let replacement = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes: replacement_sub_volumes,
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                    opts: rop_opts.clone(),
                    gen: 4,
                },
            )),
        };

        let log = csl();
        info!(
            log,
            "new ROP and new SV with sv_count:{} sv_changed:{}",
            sv_count,
            sv_changed,
        );

        assert!(Volume::compare_vcr_for_target_replacement(
            original,
            replacement,
            &log
        )
        .is_err());
    }

    #[test]
    // A replacement VCR is provided with one target being different, but with
    // the replacement volume having a sub_volume with a different block size.
    // Test with 1, 2, and 3 sub_volumes.
    // Test both with each sub_volume having the difference.
    fn volume_replace_mismatch_sv_bs() {
        for sv in 1..4 {
            for sv_changed in 0..sv {
                volume_replace_mismatch_sv_bs_inner(sv, sv_changed);
            }
        }
    }

    fn volume_replace_mismatch_sv_bs_inner(sv_count: usize, sv_changed: usize) {
        assert!(sv_count > sv_changed);
        let block_size = 512;
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let opts = generic_crucible_opts(vol_id);
        let sub_volumes = build_subvolume_vcr(
            sv_count,
            block_size,
            blocks_per_extent,
            extent_count,
            opts.clone(),
            2,
        );

        let original = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes,
            read_only_parent: None,
        };

        let replacement_sub_volumes = (0..sv_count)
            .map(|i| {
                let mut replacement_opts = opts.clone();
                let replacement_gen = 3;
                let mut replacement_block_size = block_size;

                if i == sv_changed {
                    replacement_opts.target[1] =
                        "127.0.0.1:8888".parse().unwrap();
                    replacement_block_size = 4096;
                }
                VolumeConstructionRequest::Region {
                    block_size: replacement_block_size,
                    blocks_per_extent,
                    extent_count,
                    opts: replacement_opts,
                    gen: replacement_gen,
                }
            })
            .collect();

        let replacement = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes: replacement_sub_volumes,
            read_only_parent: None,
        };

        let log = csl();
        info!(
            log,
            "replacement sub-vol mismatch block_size sv_count:{} sv_changed:{}",
            sv_count,
            sv_changed,
        );
        assert!(Volume::compare_vcr_for_target_replacement(
            original,
            replacement,
            &log
        )
        .is_err());
    }

    #[test]
    // A replacement VCR is provided with one target being different, but with
    // the replacement volume having a sub_volume with a different blocks per
    // extent.
    // Test with 1, 2, and 3 sub_volumes.
    // Test both with each sub_volume having the difference.
    fn volume_replace_mismatch_sv_bpe() {
        for sv in 1..4 {
            for sv_changed in 0..sv {
                volume_replace_mismatch_sv_bpe_inner(sv, sv_changed);
            }
        }
    }

    fn volume_replace_mismatch_sv_bpe_inner(
        sv_count: usize,
        sv_changed: usize,
    ) {
        assert!(sv_count > sv_changed);
        let block_size = 512;
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let opts = generic_crucible_opts(vol_id);
        let sub_volumes = build_subvolume_vcr(
            sv_count,
            block_size,
            blocks_per_extent,
            extent_count,
            opts.clone(),
            2,
        );

        let original = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes,
            read_only_parent: None,
        };

        let replacement_sub_volumes = (0..sv_count)
            .map(|i| {
                let mut replacement_opts = opts.clone();
                let replacement_gen = 3;
                let mut replacement_bpe = blocks_per_extent;

                if i == sv_changed {
                    replacement_opts.target[1] =
                        "127.0.0.1:8888".parse().unwrap();
                    replacement_bpe += 2;
                }
                VolumeConstructionRequest::Region {
                    block_size,
                    blocks_per_extent: replacement_bpe,
                    extent_count,
                    opts: replacement_opts,
                    gen: replacement_gen,
                }
            })
            .collect();

        let replacement = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes: replacement_sub_volumes,
            read_only_parent: None,
        };

        let log = csl();
        info!(
            log,
            "subvolume mismatch in blocks_per_extent sv_count:{} sv_changed:{}",
            sv_count,
            sv_changed,
        );
        assert!(Volume::compare_vcr_for_target_replacement(
            original,
            replacement,
            &log
        )
        .is_err());
    }

    #[test]
    // A replacement VCR is provided with one target being different, but with
    // the replacement volume having a sub_volume with a different extent count.
    // Test with 1, 2, and 3 sub_volumes.
    // Test both with each sub_volume having the difference.
    fn volume_replace_mismatch_sv_ec() {
        for sv in 1..4 {
            for sv_changed in 0..sv {
                volume_replace_mismatch_sv_ec_inner(sv, sv_changed);
            }
        }
    }
    fn volume_replace_mismatch_sv_ec_inner(sv_count: usize, sv_changed: usize) {
        assert!(sv_count > sv_changed);
        let block_size = 512;
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let opts = generic_crucible_opts(vol_id);
        let sub_volumes = build_subvolume_vcr(
            sv_count,
            block_size,
            blocks_per_extent,
            extent_count,
            opts.clone(),
            2,
        );

        let original = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes,
            read_only_parent: None,
        };

        let replacement_sub_volumes = (0..sv_count)
            .map(|i| {
                let mut replacement_opts = opts.clone();
                let replacement_gen = 3;
                let mut replacement_ec = extent_count;

                if i == sv_changed {
                    replacement_opts.target[1] =
                        "127.0.0.1:8888".parse().unwrap();
                    replacement_ec += 2;
                }
                VolumeConstructionRequest::Region {
                    block_size,
                    blocks_per_extent,
                    extent_count: replacement_ec,
                    opts: replacement_opts,
                    gen: replacement_gen,
                }
            })
            .collect();

        let replacement = VolumeConstructionRequest::Volume {
            id: vol_id,
            block_size,
            sub_volumes: replacement_sub_volumes,
            read_only_parent: None,
        };

        let log = csl();
        assert!(Volume::compare_vcr_for_update(original, replacement, &log)
            .is_err());
    }

    // This is a wrapper function to test changing CrucibleOpts structures.
    // We create two Volumes with the provided information, and use o_opts
    // for one Volume and n_opts for the other.  We return the result of
    // the compare_vcr_for_target_replacement function.
    // sv_count: The number of sub_volumes to create.
    // sv_changed: The index for which sub_volume we put the new opts..
    #[allow(clippy::too_many_arguments)]
    fn test_volume_replace_opts(
        id: Uuid,
        block_size: u64,
        blocks_per_extent: u64,
        extent_count: u32,
        o_opts: CrucibleOpts,
        n_opts: CrucibleOpts,
        sv_count: usize,
        sv_changed: usize,
    ) -> Result<(SocketAddr, SocketAddr), crucible_common::CrucibleError> {
        assert!(sv_count > sv_changed);
        let sub_volumes = build_subvolume_vcr(
            sv_count,
            block_size,
            blocks_per_extent,
            extent_count,
            o_opts.clone(),
            2,
        );

        let original = VolumeConstructionRequest::Volume {
            id,
            block_size,
            sub_volumes,
            read_only_parent: None,
        };

        let replacement_sub_volumes = (0..sv_count)
            .map(|i| {
                let mut replacement_opts = o_opts.clone();
                let replacement_gen = 3;

                if i == sv_changed {
                    replacement_opts = n_opts.clone();
                }
                VolumeConstructionRequest::Region {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                    opts: replacement_opts,
                    gen: replacement_gen,
                }
            })
            .collect();

        let replacement = VolumeConstructionRequest::Volume {
            id,
            block_size,
            sub_volumes: replacement_sub_volumes,
            read_only_parent: None,
        };

        let log = csl();
        let ReplacementRequestCheck::Valid { old, new } =
            Volume::compare_vcr_for_target_replacement(
                original,
                replacement,
                &log,
            )?
        else {
            panic!("wrong variant returned!");
        };

        Ok((old, new))
    }

    #[tokio::test]
    async fn volume_replace_mismatch_opts_id() {
        // A replacement VCR is provided with one target being
        // different, but with the replacement volume having a sub_volume
        // with a different opts.id.
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let o_opts = generic_crucible_opts(vol_id);
        let mut n_opts = o_opts.clone();

        n_opts.target[1] = "127.0.0.1:8888".parse().unwrap();
        n_opts.id = Uuid::new_v4();

        for sv in 1..4 {
            for sv_changed in 0..sv {
                assert!(test_volume_replace_opts(
                    vol_id,
                    512,
                    blocks_per_extent,
                    extent_count,
                    o_opts.clone(),
                    n_opts.clone(),
                    sv,
                    sv_changed,
                )
                .is_err());
            }
        }
    }

    #[tokio::test]
    async fn volume_replace_mismatch_opts_lossy() {
        // A replacement VCR is provided with one target being
        // different, but with the replacement volume having a sub_volume
        // with a different opts.lossy.
        // We test with 1 to 3 sub_volumes and each possible sub_volume having
        // the change
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let o_opts = generic_crucible_opts(vol_id);
        let mut n_opts = o_opts.clone();

        n_opts.target[1] = "127.0.0.1:8888".parse().unwrap();
        n_opts.lossy = true;

        for sv in 1..4 {
            for sv_changed in 0..sv {
                assert!(test_volume_replace_opts(
                    vol_id,
                    512,
                    blocks_per_extent,
                    extent_count,
                    o_opts.clone(),
                    n_opts.clone(),
                    sv,
                    sv_changed,
                )
                .is_err());
            }
        }
    }

    #[tokio::test]
    async fn volume_replace_mismatch_opts_flush_timeout() {
        // A replacement VCR is provided with one target being
        // different, but with the replacement volume having a sub_volume
        // with a different opts.flush_timeout.
        // We test with 1 to 3 sub_volumes and each possible sub_volume having
        // the change
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let o_opts = generic_crucible_opts(vol_id);
        let mut n_opts = o_opts.clone();

        n_opts.target[1] = "127.0.0.1:8888".parse().unwrap();
        n_opts.flush_timeout = Some(1.23459);

        for sv in 1..4 {
            for sv_changed in 0..sv {
                assert!(test_volume_replace_opts(
                    vol_id,
                    512,
                    blocks_per_extent,
                    extent_count,
                    o_opts.clone(),
                    n_opts.clone(),
                    sv,
                    sv_changed,
                )
                .is_err());
            }
        }
    }

    #[tokio::test]
    async fn volume_replace_mismatch_opts_key() {
        // A replacement VCR is provided with one target being
        // different, but with the replacement volume having a sub_volume
        // with a different opts.key.
        // We test with 1 to 3 sub_volumes and each possible sub_volume having
        // the change
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let o_opts = generic_crucible_opts(vol_id);
        let mut n_opts = o_opts.clone();

        n_opts.target[1] = "127.0.0.1:8888".parse().unwrap();
        let key_bytes = rand::thread_rng().gen::<[u8; 32]>();
        let key_string = engine::general_purpose::STANDARD.encode(key_bytes);
        n_opts.key = Some(key_string);

        for sv in 1..4 {
            for sv_changed in 0..sv {
                assert!(test_volume_replace_opts(
                    vol_id,
                    512,
                    blocks_per_extent,
                    extent_count,
                    o_opts.clone(),
                    n_opts.clone(),
                    sv,
                    sv_changed,
                )
                .is_err());
            }
        }
    }

    #[tokio::test]
    async fn volume_replace_mismatch_opts_cert_pem() {
        // A replacement VCR is provided with one target being
        // different, but with the replacement volume having a sub_volume
        // with a different opts.cert_pem.
        // We test with 1 to 3 sub_volumes and each possible sub_volume having
        // the change
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let o_opts = generic_crucible_opts(vol_id);
        let mut n_opts = o_opts.clone();

        n_opts.target[1] = "127.0.0.1:8888".parse().unwrap();
        n_opts.cert_pem = Some("cert_pem".to_string());

        for sv in 1..4 {
            for sv_changed in 0..sv {
                assert!(test_volume_replace_opts(
                    vol_id,
                    512,
                    blocks_per_extent,
                    extent_count,
                    o_opts.clone(),
                    n_opts.clone(),
                    sv,
                    sv_changed,
                )
                .is_err());
            }
        }
    }

    #[tokio::test]
    async fn volume_replace_mismatch_opts_key_pem() {
        // A replacement VCR is provided with one target being
        // different, but with the replacement volume having a sub_volume
        // with a different opts.key_pem.
        // We test with 1 to 3 sub_volumes and each possible sub_volume having
        // the change
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let o_opts = generic_crucible_opts(vol_id);
        let mut n_opts = o_opts.clone();

        n_opts.target[1] = "127.0.0.1:8888".parse().unwrap();
        n_opts.key_pem = Some("key_pem".to_string());

        for sv in 1..4 {
            for sv_changed in 0..sv {
                assert!(test_volume_replace_opts(
                    vol_id,
                    512,
                    blocks_per_extent,
                    extent_count,
                    o_opts.clone(),
                    n_opts.clone(),
                    sv,
                    sv_changed,
                )
                .is_err());
            }
        }
    }

    #[tokio::test]
    async fn volume_replace_mismatch_opts_root_cert() {
        // A replacement VCR is provided with one target being
        // different, but with the replacement volume having a sub_volume
        // with a different opts.root_cert.
        // We test with 1 to 3 sub_volumes and each possible sub_volume having
        // the change
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let o_opts = generic_crucible_opts(vol_id);
        let mut n_opts = o_opts.clone();

        n_opts.target[1] = "127.0.0.1:8888".parse().unwrap();
        n_opts.root_cert_pem = Some("root_pem".to_string());

        for sv in 1..4 {
            for sv_changed in 0..sv {
                assert!(test_volume_replace_opts(
                    vol_id,
                    512,
                    blocks_per_extent,
                    extent_count,
                    o_opts.clone(),
                    n_opts.clone(),
                    sv,
                    sv_changed,
                )
                .is_err());
            }
        }
    }

    #[tokio::test]
    async fn volume_replace_mismatch_opts_control() {
        // A replacement VCR is provided with one target being
        // different, but with the replacement volume having a sub_volume
        // with a different opts.control.
        // We test with 1 to 3 sub_volumes and each possible sub_volume having
        // the change
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let o_opts = generic_crucible_opts(vol_id);
        let mut n_opts = o_opts.clone();

        n_opts.target[1] = "127.0.0.1:8888".parse().unwrap();
        n_opts.control = Some("127.0.0.1:8888".parse().unwrap());

        for sv in 1..4 {
            for sv_changed in 0..sv {
                assert!(test_volume_replace_opts(
                    vol_id,
                    512,
                    blocks_per_extent,
                    extent_count,
                    o_opts.clone(),
                    n_opts.clone(),
                    sv,
                    sv_changed,
                )
                .is_err());
            }
        }
    }

    #[tokio::test]
    async fn volume_replace_mismatch_opts_read_only() {
        // A replacement VCR is provided with one target being
        // different, but with the replacement volume having a sub_volume
        // with a different opts.read_only.
        // We test with 1 to 3 sub_volumes and each possible sub_volume having
        // the change
        let vol_id = Uuid::new_v4();
        let blocks_per_extent = 10;
        let extent_count = 9;

        let o_opts = generic_crucible_opts(vol_id);
        let mut n_opts = o_opts.clone();

        n_opts.target[1] = "127.0.0.1:8888".parse().unwrap();
        n_opts.read_only = true;

        for sv in 1..4 {
            for sv_changed in 0..sv {
                assert!(test_volume_replace_opts(
                    vol_id,
                    512,
                    blocks_per_extent,
                    extent_count,
                    o_opts.clone(),
                    n_opts.clone(),
                    sv,
                    sv_changed,
                )
                .is_err());
            }
        }
    }

    /// Test that changes under a read-only parent work
    #[test]
    fn volume_replace_rop_changes() {
        let vol_id = Uuid::new_v4();
        let original = VolumeConstructionRequest::Volume {
            block_size: 512,
            id: vol_id,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 512,
                blocks_per_extent: 131072,
                extent_count: 128,
                gen: 3,
                opts: CrucibleOpts {
                    id: vol_id,
                    target: vec![
                        "[fd00:1122:3344:102::8]:19004".parse().unwrap(),
                        "[fd00:1122:3344:101::7]:19003".parse().unwrap(),
                        "[fd00:1122:3344:104::8]:19000".parse().unwrap(),
                    ],
                    ..Default::default()
                },
            }],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Volume {
                    block_size: 512,
                    id: Uuid::new_v4(),
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 131072,
                        extent_count: 32,
                        gen: 2,
                        opts: CrucibleOpts {
                            id: Uuid::new_v4(),
                            target: vec![
                                "[fd00:1122:3344:103::7]:19002"
                                    .parse()
                                    .unwrap(),
                                "[fd00:1122:3344:101::7]:19002"
                                    .parse()
                                    .unwrap(),
                                "[fd00:1122:3344:102::8]:19002"
                                    .parse()
                                    .unwrap(),
                            ],
                            ..Default::default()
                        },
                    }],
                    read_only_parent: None,
                },
            )),
        };

        // Replace one of the ROP subvolume targets, and bump the gen of the sub
        // volume region.
        let mut replacement = original.clone();
        match &mut replacement {
            VolumeConstructionRequest::Volume {
                sub_volumes,
                read_only_parent,
                ..
            } => {
                match &mut sub_volumes[0] {
                    VolumeConstructionRequest::Region { gen, .. } => {
                        *gen += 1;
                    }

                    _ => {
                        panic!("how?!");
                    }
                }

                let Some(read_only_parent) = read_only_parent.as_mut() else {
                    panic!("how?!");
                };

                match read_only_parent.as_mut() {
                    VolumeConstructionRequest::Volume {
                        sub_volumes, ..
                    } => match &mut sub_volumes[0] {
                        VolumeConstructionRequest::Region { opts, .. } => {
                            opts.target[1] = "[fd00:1122:3344:111::a]:20000"
                                .parse()
                                .unwrap();
                        }

                        _ => {
                            panic!("how?!");
                        }
                    },

                    _ => {
                        panic!("how?!");
                    }
                }
            }

            _ => {
                panic!("how?!");
            }
        }

        let log = csl();
        let result =
            Volume::compare_vcr_for_update(original, replacement, &log)
                .unwrap();
        assert_eq!(
            result,
            Some((
                "[fd00:1122:3344:101::7]:19002".parse().unwrap(),
                "[fd00:1122:3344:111::a]:20000".parse().unwrap(),
            )),
        );
    }

    /// Test that changes under a second read-only parent work
    #[test]
    fn volume_replace_second_rop_changes() {
        let vol_id = Uuid::new_v4();
        let original = VolumeConstructionRequest::Volume {
            block_size: 512,
            id: vol_id,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 512,
                blocks_per_extent: 131072,
                extent_count: 128,
                gen: 3,
                opts: CrucibleOpts {
                    id: vol_id,
                    target: vec![
                        "[fd00:1122:3344:102::8]:19004".parse().unwrap(),
                        "[fd00:1122:3344:101::7]:19003".parse().unwrap(),
                        "[fd00:1122:3344:104::8]:19000".parse().unwrap(),
                    ],
                    ..Default::default()
                },
            }],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Volume {
                    block_size: 512,
                    id: Uuid::new_v4(),
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 131072,
                        extent_count: 32,
                        gen: 2,
                        opts: CrucibleOpts {
                            id: Uuid::new_v4(),
                            target: vec![
                                "[fd00:1122:3344:103::7]:19002"
                                    .parse()
                                    .unwrap(),
                                "[fd00:1122:3344:101::7]:19002"
                                    .parse()
                                    .unwrap(),
                                "[fd00:1122:3344:102::8]:19002"
                                    .parse()
                                    .unwrap(),
                            ],
                            ..Default::default()
                        },
                    }],
                    read_only_parent: Some(Box::new(
                        VolumeConstructionRequest::Volume {
                            block_size: 512,
                            id: Uuid::new_v4(),
                            sub_volumes: vec![
                                VolumeConstructionRequest::Region {
                                    block_size: 512,
                                    blocks_per_extent: 131072,
                                    extent_count: 32,
                                    gen: 1,
                                    opts: CrucibleOpts {
                                        id: Uuid::new_v4(),
                                        target: vec![
                                            "[fd00:1122:3344:203::7]:19002"
                                                .parse()
                                                .unwrap(),
                                            "[fd00:1122:3344:201::7]:19002"
                                                .parse()
                                                .unwrap(),
                                            "[fd00:1122:3344:202::8]:19002"
                                                .parse()
                                                .unwrap(),
                                        ],
                                        ..Default::default()
                                    },
                                },
                            ],
                            read_only_parent: None,
                        },
                    )),
                },
            )),
        };

        // Replace one of the deeper ROP's subvolume targets, and don't bump the
        // gen
        let mut replacement = original.clone();
        match &mut replacement {
            VolumeConstructionRequest::Volume {
                sub_volumes,
                read_only_parent,
                ..
            } => {
                match &mut sub_volumes[0] {
                    VolumeConstructionRequest::Region { gen, .. } => {
                        *gen += 1;
                    }

                    _ => {
                        panic!("how?!");
                    }
                }

                let Some(read_only_parent) = read_only_parent.as_mut() else {
                    panic!("how?!");
                };

                match read_only_parent.as_mut() {
                    VolumeConstructionRequest::Volume {
                        read_only_parent,
                        ..
                    } => {
                        let Some(read_only_parent) = read_only_parent.as_mut()
                        else {
                            panic!("how?!");
                        };

                        match read_only_parent.as_mut() {
                            VolumeConstructionRequest::Volume {
                                sub_volumes,
                                ..
                            } => match &mut sub_volumes[0] {
                                VolumeConstructionRequest::Region {
                                    opts,
                                    ..
                                } => {
                                    opts.target[2] =
                                        "[fd00:1122:3344:111::a]:20000"
                                            .parse()
                                            .unwrap();
                                }

                                _ => {
                                    panic!("how?!");
                                }
                            },

                            _ => {
                                panic!("how?!");
                            }
                        }
                    }

                    _ => {
                        panic!("how?!");
                    }
                }
            }

            _ => {
                panic!("how?!");
            }
        }

        let log = csl();
        let result =
            Volume::compare_vcr_for_update(original, replacement, &log)
                .unwrap();
        assert_eq!(
            result,
            Some((
                "[fd00:1122:3344:202::8]:19002".parse().unwrap(),
                "[fd00:1122:3344:111::a]:20000".parse().unwrap(),
            )),
        );
    }

    /// Test that ROP generation number increasing is accepted
    #[test]
    fn volume_replace_rop_changes_increase_gen() {
        let vol_id = Uuid::new_v4();
        let original = VolumeConstructionRequest::Volume {
            block_size: 512,
            id: vol_id,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 512,
                blocks_per_extent: 131072,
                extent_count: 128,
                gen: 3,
                opts: CrucibleOpts {
                    id: vol_id,
                    target: vec![
                        "[fd00:1122:3344:102::8]:19004".parse().unwrap(),
                        "[fd00:1122:3344:101::7]:19003".parse().unwrap(),
                        "[fd00:1122:3344:104::8]:19000".parse().unwrap(),
                    ],
                    ..Default::default()
                },
            }],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Volume {
                    block_size: 512,
                    id: Uuid::new_v4(),
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 131072,
                        extent_count: 32,
                        gen: 2,
                        opts: CrucibleOpts {
                            id: Uuid::new_v4(),
                            target: vec![
                                "[fd00:1122:3344:103::7]:19002"
                                    .parse()
                                    .unwrap(),
                                "[fd00:1122:3344:101::7]:19002"
                                    .parse()
                                    .unwrap(),
                                "[fd00:1122:3344:102::8]:19002"
                                    .parse()
                                    .unwrap(),
                            ],
                            ..Default::default()
                        },
                    }],
                    read_only_parent: None,
                },
            )),
        };

        // Replace one of the ROP subvolume targets, bump the gen of the sub
        // volume region, and increase the gen of the ROP.
        let mut replacement = original.clone();
        match &mut replacement {
            VolumeConstructionRequest::Volume {
                sub_volumes,
                read_only_parent,
                ..
            } => {
                match &mut sub_volumes[0] {
                    VolumeConstructionRequest::Region { gen, .. } => {
                        *gen += 1;
                    }

                    _ => {
                        panic!("how?!");
                    }
                }

                let Some(read_only_parent) = read_only_parent.as_mut() else {
                    panic!("how?!");
                };

                match read_only_parent.as_mut() {
                    VolumeConstructionRequest::Volume {
                        sub_volumes, ..
                    } => match &mut sub_volumes[0] {
                        VolumeConstructionRequest::Region {
                            opts, gen, ..
                        } => {
                            opts.target[1] = "[fd00:1122:3344:111::a]:20000"
                                .parse()
                                .unwrap();

                            *gen += 1;
                        }

                        _ => {
                            panic!("how?!");
                        }
                    },

                    _ => {
                        panic!("how?!");
                    }
                }
            }

            _ => {
                panic!("how?!");
            }
        }

        let log = csl();
        let result =
            Volume::compare_vcr_for_update(original, replacement, &log)
                .unwrap();
        assert_eq!(
            result,
            Some((
                "[fd00:1122:3344:101::7]:19002".parse().unwrap(),
                "[fd00:1122:3344:111::a]:20000".parse().unwrap(),
            )),
        );
    }

    /// Test that ROP generation number decreasing is also accepted
    #[test]
    fn volume_replace_rop_changes_decrease_gen() {
        let vol_id = Uuid::new_v4();
        let original = VolumeConstructionRequest::Volume {
            block_size: 512,
            id: vol_id,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 512,
                blocks_per_extent: 131072,
                extent_count: 128,
                gen: 3,
                opts: CrucibleOpts {
                    id: vol_id,
                    target: vec![
                        "[fd00:1122:3344:102::8]:19004".parse().unwrap(),
                        "[fd00:1122:3344:101::7]:19003".parse().unwrap(),
                        "[fd00:1122:3344:104::8]:19000".parse().unwrap(),
                    ],
                    ..Default::default()
                },
            }],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Volume {
                    block_size: 512,
                    id: Uuid::new_v4(),
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 131072,
                        extent_count: 32,
                        gen: 2,
                        opts: CrucibleOpts {
                            id: Uuid::new_v4(),
                            target: vec![
                                "[fd00:1122:3344:103::7]:19002"
                                    .parse()
                                    .unwrap(),
                                "[fd00:1122:3344:101::7]:19002"
                                    .parse()
                                    .unwrap(),
                                "[fd00:1122:3344:102::8]:19002"
                                    .parse()
                                    .unwrap(),
                            ],
                            ..Default::default()
                        },
                    }],
                    read_only_parent: None,
                },
            )),
        };

        // Replace one of the ROP subvolume targets, bump the gen of the sub
        // volume region, and decrease the gen of the ROP.
        let mut replacement = original.clone();
        match &mut replacement {
            VolumeConstructionRequest::Volume {
                sub_volumes,
                read_only_parent,
                ..
            } => {
                match &mut sub_volumes[0] {
                    VolumeConstructionRequest::Region { gen, .. } => {
                        *gen += 1;
                    }

                    _ => {
                        panic!("how?!");
                    }
                }

                let Some(read_only_parent) = read_only_parent.as_mut() else {
                    panic!("how?!");
                };

                match read_only_parent.as_mut() {
                    VolumeConstructionRequest::Volume {
                        sub_volumes, ..
                    } => match &mut sub_volumes[0] {
                        VolumeConstructionRequest::Region {
                            opts, gen, ..
                        } => {
                            opts.target[1] = "[fd00:1122:3344:111::a]:20000"
                                .parse()
                                .unwrap();

                            *gen -= 1;
                        }

                        _ => {
                            panic!("how?!");
                        }
                    },

                    _ => {
                        panic!("how?!");
                    }
                }
            }

            _ => {
                panic!("how?!");
            }
        }

        let log = csl();
        let result =
            Volume::compare_vcr_for_update(original, replacement, &log)
                .unwrap();
        assert_eq!(
            result,
            Some((
                "[fd00:1122:3344:101::7]:19002".parse().unwrap(),
                "[fd00:1122:3344:111::a]:20000".parse().unwrap(),
            )),
        );
    }

    /// Test that ROP generation number not changing is also accepted
    #[test]
    fn volume_replace_rop_changes_same_gen() {
        let vol_id = Uuid::new_v4();
        let original = VolumeConstructionRequest::Volume {
            block_size: 512,
            id: vol_id,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 512,
                blocks_per_extent: 131072,
                extent_count: 128,
                gen: 3,
                opts: CrucibleOpts {
                    id: vol_id,
                    target: vec![
                        "[fd00:1122:3344:102::8]:19004".parse().unwrap(),
                        "[fd00:1122:3344:101::7]:19003".parse().unwrap(),
                        "[fd00:1122:3344:104::8]:19000".parse().unwrap(),
                    ],
                    ..Default::default()
                },
            }],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Volume {
                    block_size: 512,
                    id: Uuid::new_v4(),
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 131072,
                        extent_count: 32,
                        gen: 2,
                        opts: CrucibleOpts {
                            id: Uuid::new_v4(),
                            target: vec![
                                "[fd00:1122:3344:103::7]:19002"
                                    .parse()
                                    .unwrap(),
                                "[fd00:1122:3344:101::7]:19002"
                                    .parse()
                                    .unwrap(),
                                "[fd00:1122:3344:102::8]:19002"
                                    .parse()
                                    .unwrap(),
                            ],
                            ..Default::default()
                        },
                    }],
                    read_only_parent: None,
                },
            )),
        };

        // Replace one of the ROP subvolume targets, and bump the gen of the sub
        // volume region.
        let mut replacement = original.clone();
        match &mut replacement {
            VolumeConstructionRequest::Volume {
                sub_volumes,
                read_only_parent,
                ..
            } => {
                match &mut sub_volumes[0] {
                    VolumeConstructionRequest::Region { gen, .. } => {
                        *gen += 1;
                    }

                    _ => {
                        panic!("how?!");
                    }
                }

                let Some(read_only_parent) = read_only_parent.as_mut() else {
                    panic!("how?!");
                };

                match read_only_parent.as_mut() {
                    VolumeConstructionRequest::Volume {
                        sub_volumes, ..
                    } => match &mut sub_volumes[0] {
                        VolumeConstructionRequest::Region { opts, .. } => {
                            opts.target[1] = "[fd00:1122:3344:111::a]:20000"
                                .parse()
                                .unwrap();
                        }

                        _ => {
                            panic!("how?!");
                        }
                    },

                    _ => {
                        panic!("how?!");
                    }
                }
            }

            _ => {
                panic!("how?!");
            }
        }

        let log = csl();
        let result =
            Volume::compare_vcr_for_update(original, replacement, &log)
                .unwrap();
        assert_eq!(
            result,
            Some((
                "[fd00:1122:3344:101::7]:19002".parse().unwrap(),
                "[fd00:1122:3344:111::a]:20000".parse().unwrap(),
            )),
        );
    }

    #[tokio::test]
    async fn volume_cannot_construct_mixed_block_size_1() {
        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: 512,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 4096,
                blocks_per_extent: 1,
                extent_count: 1,
                opts: CrucibleOpts {
                    id: Uuid::new_v4(),
                    target: vec![],
                    lossy: false,
                    flush_timeout: None,
                    key: None,
                    cert_pem: None,
                    key_pem: None,
                    root_cert_pem: None,
                    control: None,
                    read_only: false,
                },
                gen: 1,
            }],
            read_only_parent: None,
        };

        Volume::construct(vcr, None, csl()).await.unwrap_err();
    }

    #[tokio::test]
    async fn volume_cannot_construct_mixed_block_size_2() {
        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: 512,
            sub_volumes: vec![
                VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 1,
                    extent_count: 1,
                    opts: CrucibleOpts {
                        id: Uuid::new_v4(),
                        target: vec![],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                    gen: 1,
                },
                VolumeConstructionRequest::Region {
                    block_size: 4096,
                    blocks_per_extent: 1,
                    extent_count: 1,
                    opts: CrucibleOpts {
                        id: Uuid::new_v4(),
                        target: vec![],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                    gen: 1,
                },
            ],
            read_only_parent: None,
        };

        Volume::construct(vcr, None, csl()).await.unwrap_err();
    }

    #[tokio::test]
    async fn volume_cannot_construct_mixed_block_size_3() {
        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: 512,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 512,
                blocks_per_extent: 1,
                extent_count: 1,
                opts: CrucibleOpts {
                    id: Uuid::new_v4(),
                    target: vec![],
                    lossy: false,
                    flush_timeout: None,
                    key: None,
                    cert_pem: None,
                    key_pem: None,
                    root_cert_pem: None,
                    control: None,
                    read_only: false,
                },
                gen: 1,
            }],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size: 4096,
                    blocks_per_extent: 1,
                    extent_count: 1,
                    opts: CrucibleOpts {
                        id: Uuid::new_v4(),
                        target: vec![],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                    gen: 1,
                },
            )),
        };

        Volume::construct(vcr, None, csl()).await.unwrap_err();
    }

    #[tokio::test]
    async fn volume_cannot_construct_mixed_block_size_4() {
        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: 4096,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 4096,
                blocks_per_extent: 1,
                extent_count: 1,
                opts: CrucibleOpts {
                    id: Uuid::new_v4(),
                    target: vec![],
                    lossy: false,
                    flush_timeout: None,
                    key: None,
                    cert_pem: None,
                    key_pem: None,
                    root_cert_pem: None,
                    control: None,
                    read_only: false,
                },
                gen: 1,
            }],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(),
                    block_size: 512,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 4096,
                        blocks_per_extent: 1,
                        extent_count: 1,
                        opts: CrucibleOpts {
                            id: Uuid::new_v4(),
                            target: vec![],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: false,
                        },
                        gen: 1,
                    }],
                    read_only_parent: None,
                },
            )),
        };

        Volume::construct(vcr, None, csl()).await.unwrap_err();
    }
}
