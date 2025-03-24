// Copyright 2022 Oxide Computer Company

use super::*;

use std::iter::FusedIterator;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ImpactedAddr {
    pub extent_id: ExtentId,
    pub block: BlockOffset,
}

/// Store a list of impacted blocks for later job dependency calculation
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ImpactedBlocks {
    /// No blocks are impacted
    Empty,

    /// First impacted block and last impacted block (inclusive!)
    InclusiveRange(BlockIndex, BlockIndex),
}

/// An iteration over the blocks in an [`ImpactedBlocks`] range
pub struct ImpactedBlockIter {
    active_range: Option<(BlockIndex, BlockIndex)>,
}

impl Iterator for ImpactedBlockIter {
    type Item = BlockIndex;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.active_range {
            None => None,
            Some((first, last)) => {
                let result = *first;

                // If next == last, then the iterator is now done. Otherwise,
                // increment next.
                if first == last {
                    self.active_range = None
                } else {
                    first.0 += 1;
                }

                Some(result)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len(), Some(self.len()))
    }

    fn count(self) -> usize {
        self.len()
    }

    fn last(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        self.active_range.map(|(_, last)| last)
    }
}

impl ExactSizeIterator for ImpactedBlockIter {
    fn len(&self) -> usize {
        match self.active_range {
            None => 0,
            Some((fst, lst)) => (lst.0 + 1 - fst.0) as usize,
        }
    }
}

impl DoubleEndedIterator for ImpactedBlockIter {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        match &mut self.active_range {
            None => None,
            Some((first, last)) => {
                let result = *last;

                // If first == last, then the iterator is now done. Otherwise,
                // increment first.
                if first == last {
                    self.active_range = None
                } else {
                    last.0 -= 1;
                }

                Some(result)
            }
        }
    }
}

impl FusedIterator for ImpactedBlockIter {}

impl ImpactedBlocks {
    /// Create a new ImpactedBlocks range from first and last impacted blocks
    /// (inclusive). Panics if last_impacted < first_impacted.
    pub fn new(first_impacted: BlockIndex, last_impacted: BlockIndex) -> Self {
        assert!(last_impacted >= first_impacted);
        ImpactedBlocks::InclusiveRange(first_impacted, last_impacted)
    }

    pub fn empty() -> Self {
        ImpactedBlocks::Empty
    }

    /// Returns the first impacted address
    pub fn start(&self) -> Option<BlockIndex> {
        match self {
            ImpactedBlocks::InclusiveRange(a, _) => Some(*a),
            ImpactedBlocks::Empty => None,
        }
    }

    /// Create a new [`ImpactedBlocks`] range starting at a given block, and
    /// stretching `n_blocks` further into the extent.  Returns
    /// [`ImpactedBlocks::Empty`] if `n_blocks` is 0.
    pub fn from_offset(start_block: BlockIndex, n_blocks: u64) -> Self {
        if n_blocks == 0 {
            ImpactedBlocks::Empty
        } else {
            // So because we're inclusive, if we have 1 block then the first
            // impacted should be the same as the last impacted. If we have
            // 2 blocks, it's +1. etc.
            let ending_block = BlockIndex(n_blocks + start_block.0 - 1);

            ImpactedBlocks::InclusiveRange(start_block, ending_block)
        }
    }

    pub fn is_empty(&self) -> bool {
        self == &ImpactedBlocks::Empty
    }

    /// Return an iterator over impacted extents
    pub fn extents(
        &self,
        ddef: &RegionDefinition,
    ) -> impl Iterator<Item = ExtentId> {
        let blocks_per_extent = ddef.extent_size().value;
        match self {
            ImpactedBlocks::Empty => None, /* empty range */
            ImpactedBlocks::InclusiveRange(fst, lst) => Some(
                ((fst.0 / blocks_per_extent) as u32)
                    ..=((lst.0 / blocks_per_extent) as u32),
            ),
        }
        .into_iter()
        .flatten()
        .map(ExtentId)
    }

    pub fn blocks(&self) -> ImpactedBlockIter {
        let active_range = match self {
            ImpactedBlocks::Empty => None,
            ImpactedBlocks::InclusiveRange(fst, lst) => Some((*fst, *lst)),
        };

        ImpactedBlockIter { active_range }
    }

    /// Returns the number of impacted blocks
    pub fn len(&self) -> usize {
        self.blocks().len()
    }
}

/// Given a global offset and number of blocks, compute a range of impacted
/// blocks:
///
///  |eid0                   |eid1
///  |───────────────────────────────────────────────│
///  ┌───────────────────────|───────────────────────┐
///  │   |   |xxx|xxx|xxx|xxx|xxx|xxx|xxx|   |   |   │
///  └───────────────────────|───────────────────────┘
///    0   1 | 2   3   4   5   0   1   2 | 3   4   5
///          |---------------------------|
///          offset                     offset + len
///
/// The example offset and length above spans 7 blocks over two extents (where
/// the numbers at the bottom of the diagram are block numbers).
///
/// Return an [`ImpactedBlocks`] object that stores the affected region as a range
/// of [`ImpactedAddr`] values (which are `(ExtentId, BlockOffset)` tuples)
pub fn extent_from_offset(
    ddef: &RegionDefinition,
    offset: BlockIndex,
    num_blocks: u64,
) -> ImpactedBlocks {
    let extent_size = ddef.extent_size().value;

    // If we get invalid data at this point, something has gone terribly
    // wrong further up the stack. So here's the assumptions we're making:

    // First, the offset is actually within the region
    assert!(offset.0 < ddef.extent_count() as u64 * extent_size);

    // Second, the last block is also within the region. These are widened to
    // u128 in order to catch the case where first_block + n_blocks == u64::MAX.
    assert!(
        offset.0 as u128 + num_blocks as u128
            <= ddef.extent_count() as u128 * extent_size as u128
    );

    ImpactedBlocks::from_offset(offset, num_blocks)
}

/// Converts an extent to a range of impacted blocks
pub fn extent_to_impacted_blocks(
    ddef: &RegionDefinition,
    eid: ExtentId,
) -> ImpactedBlocks {
    assert!(eid.0 < ddef.extent_count());
    let extent_size = ddef.extent_size().value;
    let start = BlockIndex(extent_size * u64::from(eid.0));
    let end = BlockIndex(start.0 + extent_size - 1);
    ImpactedBlocks::InclusiveRange(start, end)
}

#[cfg(test)]
mod test {
    use super::*;
    use proptest::prelude::*;
    use std::panic;
    use std::panic::UnwindSafe;
    use test_strategy::{proptest, Arbitrary};

    fn basic_region_definition(
        extent_size: u32,
        extent_count: u32,
    ) -> RegionDefinition {
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(u64::from(extent_size)));
        ddef.set_extent_count(extent_count);
        ddef
    }

    /// extent_to_impacted blocks should return all the blocks for a
    /// given extent.
    #[test]
    fn test_extent_to_impacted_blocks() {
        let ddef = &basic_region_definition(2, 10);

        // First
        assert_eq!(
            extent_to_impacted_blocks(ddef, ExtentId(0))
                .blocks()
                .collect::<Vec<_>>(),
            vec![BlockIndex(0), BlockIndex(1)],
        );
        // Somewhere in the middle
        assert_eq!(
            extent_to_impacted_blocks(ddef, ExtentId(3))
                .blocks()
                .collect::<Vec<_>>(),
            vec![BlockIndex(6), BlockIndex(7)],
        );
        // Last
        assert_eq!(
            extent_to_impacted_blocks(ddef, ExtentId(9))
                .blocks()
                .collect::<Vec<_>>(),
            vec![BlockIndex(18), BlockIndex(19)],
        );
    }

    #[test]
    fn test_large_extent_to_impacted_blocks() {
        // Test a slightly larger number of blocks per extent, and
        // an odd number of blocks per extent.
        let ddef = &basic_region_definition(9, 5);

        assert_eq!(
            extent_to_impacted_blocks(ddef, ExtentId(0))
                .blocks()
                .collect::<Vec<_>>(),
            vec![
                BlockIndex(0),
                BlockIndex(1),
                BlockIndex(2),
                BlockIndex(3),
                BlockIndex(4),
                BlockIndex(5),
                BlockIndex(6),
                BlockIndex(7),
                BlockIndex(8),
            ],
        );
        // Somewhere in the middle
        assert_eq!(
            extent_to_impacted_blocks(ddef, ExtentId(2))
                .blocks()
                .collect::<Vec<_>>(),
            vec![
                BlockIndex(18),
                BlockIndex(19),
                BlockIndex(20),
                BlockIndex(21),
                BlockIndex(22),
                BlockIndex(23),
                BlockIndex(24),
                BlockIndex(25),
                BlockIndex(26),
            ],
        );
        // Last
        assert_eq!(
            extent_to_impacted_blocks(ddef, ExtentId(4))
                .blocks()
                .collect::<Vec<_>>(),
            vec![
                BlockIndex(36),
                BlockIndex(37),
                BlockIndex(38),
                BlockIndex(39),
                BlockIndex(40),
                BlockIndex(41),
                BlockIndex(42),
                BlockIndex(43),
                BlockIndex(44),
            ],
        );
    }

    #[test]
    /// extent_from_offset is currently the main interface by which other parts
    /// of the codebase interact with ImpactedBlocks.
    fn test_extent_from_offset() {
        let ddef = &basic_region_definition(2, 10);

        // Test block size, less than extent size
        assert_eq!(
            extent_from_offset(ddef, BlockIndex(0), 1)
                .blocks()
                .collect::<Vec<_>>(),
            vec![BlockIndex(0)],
        );

        // Test greater than block size, less than extent size
        assert_eq!(
            extent_from_offset(ddef, BlockIndex(0), 2)
                .blocks()
                .collect::<Vec<_>>(),
            vec![BlockIndex(0), BlockIndex(1)],
        );

        // Test greater than extent size
        assert_eq!(
            extent_from_offset(ddef, BlockIndex(0), 4)
                .blocks()
                .collect::<Vec<_>>(),
            vec![BlockIndex(0), BlockIndex(1), BlockIndex(2), BlockIndex(3)],
        );

        // Test offsets
        assert_eq!(
            extent_from_offset(ddef, BlockIndex(1), 4)
                .blocks()
                .collect::<Vec<_>>(),
            vec![BlockIndex(1), BlockIndex(2), BlockIndex(3), BlockIndex(4)],
        );

        assert_eq!(
            extent_from_offset(ddef, BlockIndex(2), 4)
                .blocks()
                .collect::<Vec<_>>(),
            vec![BlockIndex(2), BlockIndex(3), BlockIndex(4), BlockIndex(5)],
        );

        assert_eq!(
            extent_from_offset(ddef, BlockIndex(2), 16)
                .blocks()
                .collect::<Vec<_>>(),
            vec![
                BlockIndex(2),
                BlockIndex(3),
                BlockIndex(4),
                BlockIndex(5),
                BlockIndex(6),
                BlockIndex(7),
                BlockIndex(8),
                BlockIndex(9),
                BlockIndex(10),
                BlockIndex(11),
                BlockIndex(12),
                BlockIndex(13),
                BlockIndex(14),
                BlockIndex(15),
                BlockIndex(16),
                BlockIndex(17),
            ],
        );
    }

    #[test]
    fn test_extent_from_offset_single_block_only() {
        let ddef = &basic_region_definition(2, 10);

        assert_eq!(
            extent_from_offset(
                ddef,
                BlockIndex(2), // offset
                1,             // num_blocks
            )
            .blocks()
            .collect::<Vec<_>>(),
            vec![BlockIndex(2)]
        );

        assert_eq!(
            extent_from_offset(
                ddef,
                BlockIndex(2), // offset
                2,             // num_blocks
            )
            .blocks()
            .collect::<Vec<_>>(),
            vec![BlockIndex(2), BlockIndex(3)]
        );

        assert_eq!(
            extent_from_offset(
                ddef,
                BlockIndex(2), // offset
                3,             // num_blocks
            )
            .blocks()
            .collect::<Vec<_>>(),
            vec![BlockIndex(2), BlockIndex(3), BlockIndex(4)]
        );
    }

    #[test]
    #[should_panic]
    /// If we create an impacted range where the last block comes before the
    /// first block, that range should be empty.
    fn test_new_range_panics_when_last_block_before_first() {
        // Block ordering works
        let _ = ImpactedBlocks::new(BlockIndex(1), BlockIndex(0));
    }

    #[test]
    /// This tests all sorts of combinations of ImpactedBlocks::from_offset.
    /// Basically all the weird edge-cases that are slightly hard to code
    /// around, where the problems are likely to happen
    fn test_impacted_blocks_from_offset() {
        const EXTENT_SIZE: u64 = 512;

        // Test that extent-aligned creation works
        let fst = BlockIndex(EXTENT_SIZE * 2 + 20);
        let control =
            ImpactedBlocks::new(fst, BlockIndex(EXTENT_SIZE * 4 + 19));
        assert_eq!(control, ImpactedBlocks::from_offset(fst, EXTENT_SIZE * 2));

        // Single block within a single extent
        let fst = BlockIndex(EXTENT_SIZE * 2 + 20);
        let control =
            ImpactedBlocks::new(fst, BlockIndex(EXTENT_SIZE * 2 + 20));
        assert_eq!(control, ImpactedBlocks::from_offset(fst, 1));

        // Ending on the end of an extent should work
        let fst = BlockIndex(EXTENT_SIZE * 2 + 20);
        let lst = BlockIndex(EXTENT_SIZE * 2 + EXTENT_SIZE - 1);
        let control = ImpactedBlocks::new(fst, lst);
        assert_eq!(
            control,
            ImpactedBlocks::from_offset(fst, lst.0 - fst.0 + 1)
        );

        // Ending on the start of an extent should work
        let fst = BlockIndex(EXTENT_SIZE * 2 + 20);
        let lst = BlockIndex(EXTENT_SIZE * 3);
        let control = ImpactedBlocks::new(fst, lst);
        assert_eq!(
            control,
            ImpactedBlocks::from_offset(fst, lst.0 - fst.0 + 1)
        );

        // 0-length should be empty
        assert_eq!(ImpactedBlocks::from_offset(fst, 0), ImpactedBlocks::Empty);
    }

    // Proptest time

    /// prop_assert that something should panic
    fn prop_should_panic<F: FnOnce() -> R + UnwindSafe, R>(
        f: F,
    ) -> Result<(), TestCaseError> {
        let original_panic_hook = panic::take_hook();
        panic::set_hook(Box::new(|_| {}));
        let unwind = panic::catch_unwind(f);
        panic::set_hook(original_panic_hook);

        prop_assert!(unwind.is_err());
        Ok(())
    }

    #[derive(Arbitrary, Debug)]
    struct ArbitraryRegionDefinition {
        // Need at least one extent
        #[strategy(1..=u32::MAX)]
        extent_count: u32,

        // extent_count * extent_size must be <= u64::MAX.
        //
        // extent_size must be >= 1
        //    satisfied by adding one to the left side of the range
        //
        // Therefor, extent_size =     [1..(usize::MAX / extent_count)]
        #[strategy(1 ..= (u64::MAX / #extent_count as u64))]
        extent_size: u64,

        #[strategy(crate::MIN_SHIFT..=crate::MAX_SHIFT)]
        block_shift: u32,
    }

    #[derive(Arbitrary, Debug)]
    enum ArbitraryImpactedBlocks {
        // Weighted this way because we want to test lots of InclusiveRanges
        // against each other, since Empty is a very simple case.
        #[weight(19)]
        InclusiveRange(
            (proptest::sample::Index, proptest::sample::Index),
            (proptest::sample::Index, proptest::sample::Index),
        ),

        #[weight(1)]
        Empty,
    }

    fn reify_region_definition(
        test_ddef: ArbitraryRegionDefinition,
    ) -> RegionDefinition {
        let mut ddef = RegionDefinition::default();
        ddef.set_extent_size(Block::new(
            test_ddef.extent_size,
            test_ddef.block_shift,
        ));
        ddef.set_extent_count(test_ddef.extent_count);
        ddef.set_block_size(1 << test_ddef.block_shift);
        ddef
    }

    fn reify_impacted_blocks(
        test_iblocks: ArbitraryImpactedBlocks,
        extent_count: u32,
        extent_size: usize,
    ) -> ImpactedBlocks {
        match test_iblocks {
            ArbitraryImpactedBlocks::Empty => ImpactedBlocks::Empty,
            ArbitraryImpactedBlocks::InclusiveRange(
                (left_eid, left_block),
                (right_eid_offset, right_block_offset),
            ) => {
                let left_addr = ImpactedAddr {
                    extent_id: ExtentId(
                        left_eid
                            .index(extent_count as usize)
                            .try_into()
                            .unwrap(),
                    ),
                    block: BlockOffset(left_block.index(extent_size) as u64),
                };

                let extent_offset: u32 = right_eid_offset
                    .index(
                        extent_count as usize - left_addr.extent_id.0 as usize,
                    )
                    .try_into()
                    .unwrap();
                let right_addr = ImpactedAddr {
                    extent_id: left_addr.extent_id + extent_offset,
                    block: BlockOffset(
                        right_block_offset
                            .index(extent_size - left_addr.block.0 as usize)
                            as u64
                            + left_addr.block.0,
                    ),
                };

                let left_block = BlockIndex(
                    u64::from(left_addr.extent_id.0) * extent_size as u64
                        + left_addr.block.0,
                );
                let right_block = BlockIndex(
                    u64::from(right_addr.extent_id.0) * extent_size as u64
                        + right_addr.block.0,
                );

                ImpactedBlocks::InclusiveRange(left_block, right_block)
            }
        }
    }

    /// Map the index of a TestImpactedBlocks to fit within the region
    fn reify_impacted_blocks_in_region(
        test_iblocks: ArbitraryImpactedBlocks,
        ddef: &RegionDefinition,
    ) -> ImpactedBlocks {
        reify_impacted_blocks(
            test_iblocks,
            ddef.extent_count(),
            ddef.extent_size().value as usize,
        )
    }

    fn region_def_strategy() -> impl Strategy<Value = RegionDefinition> {
        any::<ArbitraryRegionDefinition>().prop_map(reify_region_definition)
    }

    /// Generate a random region definition, and a single ImpactedBlocks range
    /// within it.
    fn region_and_impacted_blocks_strategy(
    ) -> impl Strategy<Value = (RegionDefinition, ImpactedBlocks)> {
        any::<(ArbitraryRegionDefinition, ArbitraryImpactedBlocks)>().prop_map(
            |(test_ddef, test_iblocks)| {
                let ddef = reify_region_definition(test_ddef);
                let iblocks =
                    reify_impacted_blocks_in_region(test_iblocks, &ddef);
                (ddef, iblocks)
            },
        )
    }

    #[proptest]
    fn iblocks_from_offset_is_empty_for_zero_blocks(
        #[strategy(1..=u32::MAX as u64)] extent_size: u64,
        start_eid: u32,
        #[strategy(0..#extent_size)] start_block: u64,
    ) {
        prop_assert_eq!(
            ImpactedBlocks::from_offset(
                BlockIndex(u64::from(start_eid) * extent_size + start_block),
                0
            ),
            ImpactedBlocks::Empty
        );
    }

    #[proptest]
    /// Make sure that when the right address is less than the left address,
    /// ImpactedBlocks::new() panics
    fn iblocks_new_panics_for_flipped_polarity(
        #[strategy(0..=u64::MAX - 1)] start_block: u64,
        #[strategy(#start_block + 1 ..= u64::MAX)] end_block: u64,
    ) {
        prop_should_panic(|| {
            ImpactedBlocks::new(BlockIndex(end_block), BlockIndex(start_block))
        })?;
    }

    #[proptest]
    fn iblocks_blocks_iterates_over_all_blocks(
        // Keep these reasonably sized so we don't OOM running this test
        #[strategy(1..=128u32)] extent_count: u32,
        #[strategy(1..=128u64)] extent_size: u64,

        #[strategy(0..#extent_count)] start_eid: u32,
        #[strategy(0..#extent_size)] start_block: u64,

        #[strategy(#start_eid..#extent_count)] end_eid: u32,
        #[strategy(#start_block..#extent_size)] end_block: u64,
    ) {
        let start_eid = ExtentId(start_eid);
        let end_eid = ExtentId(end_eid);

        // Set up our iblocks
        let iblocks = ImpactedBlocks::new(
            BlockIndex(u64::from(start_eid.0) * extent_size + start_block),
            BlockIndex(u64::from(end_eid.0) * extent_size + end_block),
        );

        let mut ddef = RegionDefinition::default();
        ddef.set_extent_count(extent_count);
        ddef.set_extent_size(Block::new_512(extent_size));
        ddef.set_block_size(512);

        // Generate reference data
        let mut expected_addresses = Vec::new();
        let mut cur_block = u64::from(start_eid.0) * extent_size + start_block;
        let end_block = u64::from(end_eid.0) * extent_size + end_block;
        loop {
            expected_addresses.push(BlockIndex(cur_block));
            if cur_block == end_block {
                break;
            }

            cur_block += 1;
        }

        prop_assert_eq!(
            iblocks.blocks().collect::<Vec<_>>(),
            expected_addresses
        );
    }

    #[proptest]
    /// We have a manual test of extent_from_offset above, but here's another
    /// for good measure. We'll generate an ImpactedBlocks in a region, and then
    /// make sure we can re-create it using extent_from_offset
    fn extent_from_offset_can_recreate_iblocks(
        #[strategy(region_and_impacted_blocks_strategy())]
        #[filter(!#region_and_iblocks.1.is_empty())]
        region_and_iblocks: (RegionDefinition, ImpactedBlocks),
    ) {
        let (ddef, iblocks) = region_and_iblocks;

        let first = iblocks.blocks().next().unwrap();
        let num_blocks = iblocks.len() as u64;

        prop_assert_eq!(extent_from_offset(&ddef, first, num_blocks), iblocks);
    }

    #[proptest]
    fn extent_from_offset_panics_when_num_blocks_outside_region(
        // First block should be inside the region
        #[strategy(0..#ddef.extent_count() as u64 * #ddef.extent_size().value)]
        first_block: u64,

        // Last block should be outside the region
        #[strategy(((#ddef.extent_count() as u64 * #ddef.extent_size().value) - #first_block + 1)..=u64::MAX)]
        n_blocks: u64,

        #[strategy(region_def_strategy())] ddef: RegionDefinition,
    ) {
        let first = BlockIndex(first_block);
        let num_blocks = n_blocks;

        prop_should_panic(|| extent_from_offset(&ddef, first, num_blocks))?;
    }

    #[proptest]
    fn extent_from_offset_panics_for_offsets_outside_region(
        // First block should be outside the region
        #[strategy(#ddef.extent_count() as u64 * #ddef.extent_size().value..u64::MAX)]
        first_block: u64,

        // At least one block
        #[strategy(1..=u64::MAX)] n_blocks: u64,

        #[strategy(region_def_strategy())] ddef: RegionDefinition,
    ) {
        let first = BlockIndex(first_block);
        let num_blocks = n_blocks;

        prop_should_panic(|| extent_from_offset(&ddef, first, num_blocks))?;
    }
}
