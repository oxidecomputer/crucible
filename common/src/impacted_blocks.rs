// Copyright 2022 Oxide Computer Company

use super::*;

use std::{iter::FusedIterator, ops::RangeInclusive};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ImpactedAddr {
    pub extent_id: ExtentId,
    pub block: BlockOffset,
}

/// Store a list of impacted blocks for later job dependency calculation
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ImpactedBlocks {
    // Note that there is no extent size stored here. This is intentional! If
    // we stored an extent size we'd need to do all sorts of messy checks
    // when comparing two ImpactedBlocks ranges to make sure their extent
    // sizes match. Instead, if something wants a list of blocks, they tell
    // us how big they think an extent is, and we give them what they want.
    /// No blocks are impacted
    Empty,

    /// First impacted block and last impacted block (inclusive!)
    InclusiveRange(ImpactedAddr, ImpactedAddr),
}

/// An iteration over the blocks in an ImpactedBlocks range
pub struct ImpactedBlockIter {
    extent_size: u64,
    active_range: Option<(ImpactedAddr, ImpactedAddr)>,
}

impl Iterator for ImpactedBlockIter {
    type Item = (ExtentId, BlockOffset);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.active_range {
            None => None,
            Some((first, last)) => {
                let result = (first.extent_id, first.block);

                // If next == last, then the iterator is now done. Otherwise,
                // increment next.
                if first == last {
                    self.active_range = None
                } else if first.block.0 == self.extent_size - 1 {
                    first.block.0 = 0;
                    first.extent_id += 1;
                } else {
                    first.block.0 += 1;
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
        self.active_range
            .map(|(_, last)| (last.extent_id, last.block))
    }
}

impl ExactSizeIterator for ImpactedBlockIter {
    fn len(&self) -> usize {
        match self.active_range {
            None => 0,
            Some((fst, lst)) => {
                let extents = (lst.extent_id - fst.extent_id) as u64;
                (extents * self.extent_size + lst.block.0 - fst.block.0 + 1)
                    as usize
            }
        }
    }
}

impl DoubleEndedIterator for ImpactedBlockIter {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        match &mut self.active_range {
            None => None,
            Some((first, last)) => {
                let result = (last.extent_id, last.block);

                // If first == last, then the iterator is now done. Otherwise,
                // increment first.
                if first == last {
                    self.active_range = None
                } else if last.block.0 == 0 {
                    last.block.0 = self.extent_size - 1;
                    last.extent_id -= 1;
                } else {
                    last.block.0 -= 1;
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
    pub fn new(
        first_impacted: ImpactedAddr,
        last_impacted: ImpactedAddr,
    ) -> Self {
        assert!(last_impacted >= first_impacted);
        ImpactedBlocks::InclusiveRange(first_impacted, last_impacted)
    }

    pub fn empty() -> Self {
        ImpactedBlocks::Empty
    }

    /// Returns the first impacted address
    pub fn start(&self) -> Option<ImpactedAddr> {
        match self {
            ImpactedBlocks::InclusiveRange(a, _) => Some(*a),
            ImpactedBlocks::Empty => None,
        }
    }

    /// Create a new ImpactedBlocks range starting at a given offset, and
    /// stretching n_blocks further into the extent. Panics an error if
    /// `extent_size` is 0. Returns ImpactedBlocks::Empty if n_blocks is 0.
    pub fn from_offset(
        extent_size: u64,
        first_impacted: ImpactedAddr,
        n_blocks: u64,
    ) -> Self {
        assert!(extent_size > 0);

        if n_blocks == 0 {
            return ImpactedBlocks::Empty;
        }

        // So because we're inclusive, if we have 1 block then the first
        // impacted should be the same as the last impacted. If we have
        // 2 blocks, it's +1. etc.
        let ending_block = n_blocks + first_impacted.block.0 - 1;

        let last_impacted = ImpactedAddr {
            extent_id: first_impacted.extent_id
                + (ending_block / extent_size) as u32,
            block: BlockOffset(ending_block % extent_size),
        };

        ImpactedBlocks::InclusiveRange(first_impacted, last_impacted)
    }

    pub fn is_empty(&self) -> bool {
        self == &ImpactedBlocks::Empty
    }

    /// Return a range of impacted extents
    pub fn extents(&self) -> Option<RangeInclusive<u32>> {
        match self {
            ImpactedBlocks::Empty => None, /* empty range */
            ImpactedBlocks::InclusiveRange(fst, lst) => {
                Some(fst.extent_id.0..=lst.extent_id.0)
            }
        }
    }

    pub fn blocks(&self, ddef: &RegionDefinition) -> ImpactedBlockIter {
        let extent_size = ddef.extent_size().value;
        let active_range = match self {
            ImpactedBlocks::Empty => None,
            ImpactedBlocks::InclusiveRange(fst, lst) => Some((*fst, *lst)),
        };

        ImpactedBlockIter {
            extent_size,
            active_range,
        }
    }

    /// Returns the number of impacted blocks
    pub fn len(&self, ddef: &RegionDefinition) -> usize {
        self.blocks(ddef).len()
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
/// Return an ImpactedBlocks object that stores the affected region as a range
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

    let fst = ImpactedAddr {
        extent_id: ExtentId((offset.0 / extent_size) as u32),
        block: BlockOffset(offset.0 % extent_size),
    };

    ImpactedBlocks::from_offset(extent_size, fst, num_blocks)
}

pub fn extent_to_impacted_blocks(
    ddef: &RegionDefinition,
    eid: ExtentId,
) -> ImpactedBlocks {
    assert!(eid.0 < ddef.extent_count());
    let one = ImpactedAddr {
        extent_id: eid,
        block: BlockOffset(0),
    };
    let two = ImpactedAddr {
        extent_id: eid,
        block: BlockOffset(ddef.extent_size().value - 1),
    };
    ImpactedBlocks::InclusiveRange(one, two)
}

#[cfg(test)]
mod test {
    use super::*;
    use proptest::prelude::*;
    use std::panic;
    use std::panic::UnwindSafe;
    use test_strategy::{proptest, Arbitrary};

    fn extent_tuple(eid: u32, offset: u64) -> (ExtentId, BlockOffset) {
        (ExtentId(eid), BlockOffset(offset))
    }

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
                .blocks(ddef)
                .collect::<Vec<_>>(),
            vec![extent_tuple(0, 0), extent_tuple(0, 1)],
        );
        // Somewhere in the middle
        assert_eq!(
            extent_to_impacted_blocks(ddef, ExtentId(3))
                .blocks(ddef)
                .collect::<Vec<_>>(),
            vec![extent_tuple(3, 0), extent_tuple(3, 1)],
        );
        // Last
        assert_eq!(
            extent_to_impacted_blocks(ddef, ExtentId(9))
                .blocks(ddef)
                .collect::<Vec<_>>(),
            vec![extent_tuple(9, 0), extent_tuple(9, 1)],
        );
    }

    #[test]
    fn test_large_extent_to_impacted_blocks() {
        // Test a slightly larger number of blocks per extent, and
        // an odd number of blocks per extent.
        let ddef = &basic_region_definition(9, 5);

        assert_eq!(
            extent_to_impacted_blocks(ddef, ExtentId(0))
                .blocks(ddef)
                .collect::<Vec<_>>(),
            vec![
                extent_tuple(0, 0),
                extent_tuple(0, 1),
                extent_tuple(0, 2),
                extent_tuple(0, 3),
                extent_tuple(0, 4),
                extent_tuple(0, 5),
                extent_tuple(0, 6),
                extent_tuple(0, 7),
                extent_tuple(0, 8),
            ],
        );
        // Somewhere in the middle
        assert_eq!(
            extent_to_impacted_blocks(ddef, ExtentId(2))
                .blocks(ddef)
                .collect::<Vec<_>>(),
            vec![
                extent_tuple(2, 0),
                extent_tuple(2, 1),
                extent_tuple(2, 2),
                extent_tuple(2, 3),
                extent_tuple(2, 4),
                extent_tuple(2, 5),
                extent_tuple(2, 6),
                extent_tuple(2, 7),
                extent_tuple(2, 8),
            ],
        );
        // Last
        assert_eq!(
            extent_to_impacted_blocks(ddef, ExtentId(4))
                .blocks(ddef)
                .collect::<Vec<_>>(),
            vec![
                extent_tuple(4, 0),
                extent_tuple(4, 1),
                extent_tuple(4, 2),
                extent_tuple(4, 3),
                extent_tuple(4, 4),
                extent_tuple(4, 5),
                extent_tuple(4, 6),
                extent_tuple(4, 7),
                extent_tuple(4, 8),
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
                .blocks(ddef)
                .collect::<Vec<_>>(),
            vec![extent_tuple(0, 0)],
        );

        // Test greater than block size, less than extent size
        assert_eq!(
            extent_from_offset(ddef, BlockIndex(0), 2)
                .blocks(ddef)
                .collect::<Vec<_>>(),
            vec![extent_tuple(0, 0), extent_tuple(0, 1)],
        );

        // Test greater than extent size
        assert_eq!(
            extent_from_offset(ddef, BlockIndex(0), 4)
                .blocks(ddef)
                .collect::<Vec<_>>(),
            vec![
                extent_tuple(0, 0),
                extent_tuple(0, 1),
                extent_tuple(1, 0),
                extent_tuple(1, 1),
            ],
        );

        // Test offsets
        assert_eq!(
            extent_from_offset(ddef, BlockIndex(1), 4)
                .blocks(ddef)
                .collect::<Vec<_>>(),
            vec![
                extent_tuple(0, 1),
                extent_tuple(1, 0),
                extent_tuple(1, 1),
                extent_tuple(2, 0),
            ],
        );

        assert_eq!(
            extent_from_offset(ddef, BlockIndex(2), 4)
                .blocks(ddef)
                .collect::<Vec<_>>(),
            vec![
                extent_tuple(1, 0),
                extent_tuple(1, 1),
                extent_tuple(2, 0),
                extent_tuple(2, 1),
            ],
        );

        assert_eq!(
            extent_from_offset(ddef, BlockIndex(2), 16)
                .blocks(ddef)
                .collect::<Vec<_>>(),
            vec![
                extent_tuple(1, 0),
                extent_tuple(1, 1),
                extent_tuple(2, 0),
                extent_tuple(2, 1),
                extent_tuple(3, 0),
                extent_tuple(3, 1),
                extent_tuple(4, 0),
                extent_tuple(4, 1),
                extent_tuple(5, 0),
                extent_tuple(5, 1),
                extent_tuple(6, 0),
                extent_tuple(6, 1),
                extent_tuple(7, 0),
                extent_tuple(7, 1),
                extent_tuple(8, 0),
                extent_tuple(8, 1),
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
            .blocks(ddef)
            .collect::<Vec<_>>(),
            vec![extent_tuple(1, 0)]
        );

        assert_eq!(
            extent_from_offset(
                ddef,
                BlockIndex(2), // offset
                2,             // num_blocks
            )
            .blocks(ddef)
            .collect::<Vec<_>>(),
            vec![extent_tuple(1, 0), extent_tuple(1, 1)]
        );

        assert_eq!(
            extent_from_offset(
                ddef,
                BlockIndex(2), // offset
                3,             // num_blocks
            )
            .blocks(ddef)
            .collect::<Vec<_>>(),
            vec![extent_tuple(1, 0), extent_tuple(1, 1), extent_tuple(2, 0)]
        );
    }

    #[test]
    #[should_panic]
    /// If we create an impacted range where the last block comes before the
    /// first block, that range should be empty.
    fn test_new_range_panics_when_last_extent_before_first() {
        // Extent id ordering works
        let _ = ImpactedBlocks::new(
            ImpactedAddr {
                extent_id: ExtentId(1),
                block: BlockOffset(0),
            },
            ImpactedAddr {
                extent_id: ExtentId(0),
                block: BlockOffset(0),
            },
        );
    }

    #[test]
    #[should_panic]
    /// If we create an impacted range where the last block comes before the
    /// first block, that range should be empty.
    fn test_new_range_panics_when_last_block_before_first() {
        // Block ordering works
        let _ = ImpactedBlocks::new(
            ImpactedAddr {
                extent_id: ExtentId(0),
                block: BlockOffset(1),
            },
            ImpactedAddr {
                extent_id: ExtentId(0),
                block: BlockOffset(0),
            },
        );
    }

    #[test]
    /// This tests all sorts of combinations of ImpactedBlocks::from_offset.
    /// Basically all the weird edge-cases that are slightly hard to code
    /// around, where the problems are likely to happen
    fn test_impacted_blocks_from_offset() {
        const EXTENT_SIZE: u64 = 512;

        // Test that extent-aligned creation works
        let fst = ImpactedAddr {
            extent_id: ExtentId(2),
            block: BlockOffset(20),
        };
        let control = ImpactedBlocks::new(
            fst,
            ImpactedAddr {
                extent_id: ExtentId(4),
                block: BlockOffset(19),
            },
        );
        assert_eq!(
            control,
            ImpactedBlocks::from_offset(EXTENT_SIZE, fst, EXTENT_SIZE * 2)
        );

        // Single block within a single extent
        let fst = ImpactedAddr {
            extent_id: ExtentId(2),
            block: BlockOffset(20),
        };
        let control = ImpactedBlocks::new(
            fst,
            ImpactedAddr {
                extent_id: ExtentId(2),
                block: BlockOffset(20),
            },
        );
        assert_eq!(control, ImpactedBlocks::from_offset(EXTENT_SIZE, fst, 1));

        // Ending on the end of an extent should work
        let fst = ImpactedAddr {
            extent_id: ExtentId(2),
            block: BlockOffset(20),
        };
        let control = ImpactedBlocks::new(
            fst,
            ImpactedAddr {
                extent_id: ExtentId(2),
                block: BlockOffset(EXTENT_SIZE - 1),
            },
        );
        assert_eq!(
            control,
            ImpactedBlocks::from_offset(
                EXTENT_SIZE,
                fst,
                EXTENT_SIZE - fst.block.0,
            )
        );

        // Ending on the start of an extent should work
        let fst = ImpactedAddr {
            extent_id: ExtentId(2),
            block: BlockOffset(20),
        };
        let control = ImpactedBlocks::new(
            fst,
            ImpactedAddr {
                extent_id: ExtentId(3),
                block: BlockOffset(0),
            },
        );
        assert_eq!(
            control,
            ImpactedBlocks::from_offset(
                EXTENT_SIZE,
                fst,
                EXTENT_SIZE - fst.block.0 + 1,
            )
        );

        // 0-length should be empty
        assert_eq!(
            ImpactedBlocks::from_offset(EXTENT_SIZE, fst, 0),
            ImpactedBlocks::Empty
        );
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

                ImpactedBlocks::InclusiveRange(left_addr, right_addr)
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
        #[strategy(1..=u64::MAX)] extent_size: u64,
        start_eid: u32,
        #[strategy(0..#extent_size)] start_block: u64,
    ) {
        prop_assert_eq!(
            ImpactedBlocks::from_offset(
                extent_size,
                ImpactedAddr {
                    extent_id: ExtentId(start_eid),
                    block: BlockOffset(start_block),
                },
                0
            ),
            ImpactedBlocks::Empty
        );
    }

    #[proptest]
    fn iblocks_from_offset_with_zero_extent_size_panics(
        start_eid: u32,
        start_block: u64,
        n_blocks: u64,
    ) {
        prop_should_panic(|| {
            ImpactedBlocks::from_offset(
                0,
                ImpactedAddr {
                    extent_id: ExtentId(start_eid),
                    block: BlockOffset(start_block),
                },
                n_blocks,
            )
        })?;
    }

    #[proptest]
    /// Make sure that when the right address is less than the left address,
    /// ImpactedBlocks::new() panics
    fn iblocks_new_panics_for_flipped_polarity(
        #[strategy(0..=u32::MAX - 1)] start_eid: u32,
        #[strategy(0..=u64::MAX - 1)] start_block: u64,

        #[strategy(#start_eid + 1 ..= u32::MAX)] end_eid: u32,
        #[strategy(#start_block + 1 ..= u64::MAX)] end_block: u64,
    ) {
        let start_addr = ImpactedAddr {
            extent_id: ExtentId(start_eid),
            block: BlockOffset(start_block),
        };

        let end_addr = ImpactedAddr {
            extent_id: ExtentId(end_eid),
            block: BlockOffset(end_block),
        };

        prop_should_panic(|| ImpactedBlocks::new(end_addr, start_addr))?;
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
            ImpactedAddr {
                extent_id: start_eid,
                block: BlockOffset(start_block),
            },
            ImpactedAddr {
                extent_id: end_eid,
                block: BlockOffset(end_block),
            },
        );

        let mut ddef = RegionDefinition::default();
        ddef.set_extent_count(extent_count);
        ddef.set_extent_size(Block::new_512(extent_size));
        ddef.set_block_size(512);

        // Generate reference data
        let mut expected_addresses = Vec::new();
        let mut cur_eid = start_eid;
        let mut cur_block = start_block;
        loop {
            expected_addresses.push((cur_eid, BlockOffset(cur_block)));
            if cur_eid == end_eid && cur_block == end_block {
                break;
            }

            cur_block += 1;
            if cur_block == extent_size {
                cur_block = 0;
                cur_eid += 1;
            }
        }

        prop_assert_eq!(
            iblocks.blocks(&ddef).collect::<Vec<_>>(),
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

        let (first_eid, first_block) = iblocks.blocks(&ddef).next().unwrap();
        let first = BlockIndex(
            first_block.0 + first_eid.0 as u64 * ddef.extent_size().value,
        );
        let num_blocks = iblocks.len(&ddef) as u64;

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

    #[proptest]
    fn iblocks_extents_returns_correct_extents(
        eid_a: u32,
        block_a: u64,

        #[strategy(#eid_a..=u32::MAX)] eid_b: u32,
        #[strategy(#block_a..=u64::MAX)] block_b: u64,
    ) {
        let addr_a = ImpactedAddr {
            extent_id: ExtentId(eid_a),
            block: BlockOffset(block_a),
        };
        let addr_b = ImpactedAddr {
            extent_id: ExtentId(eid_b),
            block: BlockOffset(block_b),
        };
        let iblocks = ImpactedBlocks::new(addr_a, addr_b);
        prop_assert_eq!(iblocks.extents(), Some(eid_a..=eid_b));
    }
}
