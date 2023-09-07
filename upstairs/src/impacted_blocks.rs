// Copyright 2022 Oxide Computer Company

use super::*;

use std::{iter::FusedIterator, ops::RangeInclusive};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ImpactedAddr {
    pub extent_id: u64,
    pub block: u64,
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
    block_shift: u32,
    active_range: Option<(ImpactedAddr, ImpactedAddr)>,
}

impl Iterator for ImpactedBlockIter {
    type Item = (u64, Block);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.active_range {
            None => None,
            Some((first, last)) => {
                let result = (
                    first.extent_id,
                    Block::new(first.block, self.block_shift),
                );

                // If next == last, then the iterator is now done. Otherwise,
                // increment next.
                if first == last {
                    self.active_range = None
                } else if first.block == self.extent_size - 1 {
                    first.block = 0;
                    first.extent_id += 1;
                } else {
                    first.block += 1;
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
        self.active_range.map(|(_, last)| {
            (last.extent_id, Block::new(last.block, self.block_shift))
        })
    }
}

impl ExactSizeIterator for ImpactedBlockIter {
    fn len(&self) -> usize {
        match self.active_range {
            None => 0,
            Some((fst, lst)) => {
                let extents = lst.extent_id - fst.extent_id;
                (extents * self.extent_size + lst.block - fst.block + 1)
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
                let result =
                    (last.extent_id, Block::new(last.block, self.block_shift));

                // If first == last, then the iterator is now done. Otherwise,
                // increment first.
                if first == last {
                    self.active_range = None
                } else if last.block == 0 {
                    last.block = self.extent_size - 1;
                    last.extent_id -= 1;
                } else {
                    last.block -= 1;
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
        let ending_block = n_blocks + first_impacted.block - 1;

        let last_impacted = ImpactedAddr {
            extent_id: first_impacted.extent_id + ending_block / extent_size,
            block: ending_block % extent_size,
        };

        ImpactedBlocks::InclusiveRange(first_impacted, last_impacted)
    }

    pub fn intersection(&self, other: &ImpactedBlocks) -> ImpactedBlocks {
        let ImpactedBlocks::InclusiveRange(fst_self, lst_self) = self else {
            return ImpactedBlocks::Empty;
        };

        let ImpactedBlocks::InclusiveRange(fst_other, lst_other) = other else {
            return ImpactedBlocks::Empty;
        };

        let fst = fst_self.max(fst_other);
        let lst = lst_self.min(lst_other);
        if lst >= fst {
            ImpactedBlocks::InclusiveRange(*fst, *lst)
        } else {
            ImpactedBlocks::Empty
        }
    }

    pub fn union(&self, other: &ImpactedBlocks) -> ImpactedBlocks {
        let ImpactedBlocks::InclusiveRange(fst_self, lst_self) = self else {
            return *other;
        };

        let ImpactedBlocks::InclusiveRange(fst_other, lst_other) = other else {
            return *self;
        };
        let fst = fst_self.min(fst_other);
        let lst = lst_self.max(lst_other);
        ImpactedBlocks::InclusiveRange(*fst, *lst)
    }

    pub fn is_empty(&self) -> bool {
        self == &ImpactedBlocks::Empty
    }

    /// Return true if this list of impacted blocks overlaps with another.
    pub fn conflicts(&self, other: &ImpactedBlocks) -> bool {
        !self.intersection(other).is_empty()
    }

    pub fn fully_contains(&self, other: &ImpactedBlocks) -> bool {
        !self.is_empty()
            && !other.is_empty()
            && self.intersection(other) == *other
    }

    /// Return a range of impacted extents
    pub fn extents(&self) -> Option<RangeInclusive<u64>> {
        match self {
            ImpactedBlocks::Empty => None, /* empty range */
            ImpactedBlocks::InclusiveRange(fst, lst) => {
                Some(fst.extent_id..=lst.extent_id)
            }
        }
    }

    pub fn blocks(&self, ddef: &RegionDefinition) -> ImpactedBlockIter {
        let extent_size = ddef.extent_size().value;
        let block_shift = ddef.block_size().trailing_zeros();
        let active_range = match self {
            ImpactedBlocks::Empty => None,
            ImpactedBlocks::InclusiveRange(fst, lst) => Some((*fst, *lst)),
        };

        ImpactedBlockIter {
            extent_size,
            block_shift,
            active_range,
        }
    }

    /// Returns the number of impacted blocks
    pub fn len(&self, ddef: &RegionDefinition) -> usize {
        self.blocks(ddef).len()
    }
}

/// Given an offset and number of blocks, compute a list of individually
/// impacted blocks:
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
/// Return an ImpactedBlocks object that stores which extents are impacted,
/// along with the specific blocks in those extents. For the above example, the
/// hashmap inside ImpactedBlocks would be:
///
///  blocks = {
///    eid0 -> [2, 3, 4, 5],
///    eid1 -> [0, 1, 2],
///  }
pub fn extent_from_offset(
    ddef: &RegionDefinition,
    offset: Block,
    num_blocks: Block,
) -> ImpactedBlocks {
    let extent_size = ddef.extent_size().value;
    let shift = ddef.block_size().trailing_zeros();

    // If we get invalid data at this point, something has gone terribly
    // wrong further up the stack. So here's the assumptions we're making:
    // First, our blocks are actually the same size as this region's blocks
    assert_eq!(offset.shift, shift);
    assert_eq!(num_blocks.shift, shift);

    // Second, the offset is actually within the region
    assert!(offset.value < ddef.extent_count() as u64 * extent_size);

    // Third, the last block is also within the region. These are widened to
    // u128 in order to catch the case where first_block + n_blocks == u64::MAX.
    assert!(
        offset.value as u128 + num_blocks.value as u128
            <= ddef.extent_count() as u128 * extent_size as u128
    );

    let fst = ImpactedAddr {
        extent_id: offset.value / extent_size,
        block: offset.value % extent_size,
    };

    ImpactedBlocks::from_offset(extent_size, fst, num_blocks.value)
}

pub fn extent_to_impacted_blocks(
    ddef: &RegionDefinition,
    eid: u64,
) -> ImpactedBlocks {
    assert!(eid < ddef.extent_count() as u64);
    let one = ImpactedAddr {
        extent_id: eid,
        block: 0,
    };
    let two = ImpactedAddr {
        extent_id: eid,
        block: ddef.extent_size().value - 1,
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

    fn extent_tuple(eid: u64, offset: u64) -> (u64, Block) {
        (eid, Block::new_512(offset))
    }

    fn basic_region_definition(
        extent_size: u64,
        extent_count: u32,
    ) -> RegionDefinition {
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(extent_size));
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
            extent_to_impacted_blocks(ddef, 0)
                .blocks(ddef)
                .collect::<Vec<(u64, Block)>>(),
            vec![extent_tuple(0, 0), extent_tuple(0, 1)],
        );
        // Somewhere in the middle
        assert_eq!(
            extent_to_impacted_blocks(ddef, 3)
                .blocks(ddef)
                .collect::<Vec<(u64, Block)>>(),
            vec![extent_tuple(3, 0), extent_tuple(3, 1)],
        );
        // Last
        assert_eq!(
            extent_to_impacted_blocks(ddef, 9)
                .blocks(ddef)
                .collect::<Vec<(u64, Block)>>(),
            vec![extent_tuple(9, 0), extent_tuple(9, 1)],
        );
    }

    #[test]
    fn test_large_extent_to_impacted_blocks() {
        // Test a slightly larger number of blocks per extent, and
        // an odd number of blocks per extent.
        let ddef = &basic_region_definition(9, 5);

        assert_eq!(
            extent_to_impacted_blocks(ddef, 0)
                .blocks(ddef)
                .collect::<Vec<(u64, Block)>>(),
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
            extent_to_impacted_blocks(ddef, 2)
                .blocks(ddef)
                .collect::<Vec<(u64, Block)>>(),
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
            extent_to_impacted_blocks(ddef, 4)
                .blocks(ddef)
                .collect::<Vec<(u64, Block)>>(),
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
            extent_from_offset(ddef, Block::new_512(0), Block::new_512(1))
                .blocks(ddef)
                .collect::<Vec<(u64, Block)>>(),
            vec![extent_tuple(0, 0)],
        );

        // Test greater than block size, less than extent size
        assert_eq!(
            extent_from_offset(ddef, Block::new_512(0), Block::new_512(2))
                .blocks(ddef)
                .collect::<Vec<(u64, Block)>>(),
            vec![extent_tuple(0, 0), extent_tuple(0, 1)],
        );

        // Test greater than extent size
        assert_eq!(
            extent_from_offset(ddef, Block::new_512(0), Block::new_512(4))
                .blocks(ddef)
                .collect::<Vec<(u64, Block)>>(),
            vec![
                extent_tuple(0, 0),
                extent_tuple(0, 1),
                extent_tuple(1, 0),
                extent_tuple(1, 1),
            ],
        );

        // Test offsets
        assert_eq!(
            extent_from_offset(ddef, Block::new_512(1), Block::new_512(4))
                .blocks(ddef)
                .collect::<Vec<(u64, Block)>>(),
            vec![
                extent_tuple(0, 1),
                extent_tuple(1, 0),
                extent_tuple(1, 1),
                extent_tuple(2, 0),
            ],
        );

        assert_eq!(
            extent_from_offset(ddef, Block::new_512(2), Block::new_512(4))
                .blocks(ddef)
                .collect::<Vec<(u64, Block)>>(),
            vec![
                extent_tuple(1, 0),
                extent_tuple(1, 1),
                extent_tuple(2, 0),
                extent_tuple(2, 1),
            ],
        );

        assert_eq!(
            extent_from_offset(ddef, Block::new_512(2), Block::new_512(16))
                .blocks(ddef)
                .collect::<Vec<(u64, Block)>>(),
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
                Block::new_512(2), // offset
                Block::new_512(1), // num_blocks
            )
            .blocks(ddef)
            .collect::<Vec<(u64, Block)>>(),
            vec![extent_tuple(1, 0)]
        );

        assert_eq!(
            extent_from_offset(
                ddef,
                Block::new_512(2), // offset
                Block::new_512(2), // num_blocks
            )
            .blocks(ddef)
            .collect::<Vec<(u64, Block)>>(),
            vec![extent_tuple(1, 0), extent_tuple(1, 1)]
        );

        assert_eq!(
            extent_from_offset(
                ddef,
                Block::new_512(2), // offset
                Block::new_512(3), // num_blocks
            )
            .blocks(ddef)
            .collect::<Vec<(u64, Block)>>(),
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
                extent_id: 1,
                block: 0,
            },
            ImpactedAddr {
                extent_id: 0,
                block: 0,
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
                extent_id: 0,
                block: 1,
            },
            ImpactedAddr {
                extent_id: 0,
                block: 0,
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
            extent_id: 2,
            block: 20,
        };
        let control = ImpactedBlocks::new(
            fst,
            ImpactedAddr {
                extent_id: 4,
                block: 19,
            },
        );
        assert_eq!(
            control,
            ImpactedBlocks::from_offset(EXTENT_SIZE, fst, EXTENT_SIZE * 2)
        );

        // Single block within a single extent
        let fst = ImpactedAddr {
            extent_id: 2,
            block: 20,
        };
        let control = ImpactedBlocks::new(
            fst,
            ImpactedAddr {
                extent_id: 2,
                block: 20,
            },
        );
        assert_eq!(control, ImpactedBlocks::from_offset(EXTENT_SIZE, fst, 1));

        // Ending on the end of an extent should work
        let fst = ImpactedAddr {
            extent_id: 2,
            block: 20,
        };
        let control = ImpactedBlocks::new(
            fst,
            ImpactedAddr {
                extent_id: 2,
                block: EXTENT_SIZE - 1,
            },
        );
        assert_eq!(
            control,
            ImpactedBlocks::from_offset(
                EXTENT_SIZE,
                fst,
                EXTENT_SIZE - fst.block,
            )
        );

        // Ending on the start of an extent should work
        let fst = ImpactedAddr {
            extent_id: 2,
            block: 20,
        };
        let control = ImpactedBlocks::new(
            fst,
            ImpactedAddr {
                extent_id: 3,
                block: 0,
            },
        );
        assert_eq!(
            control,
            ImpactedBlocks::from_offset(
                EXTENT_SIZE,
                fst,
                EXTENT_SIZE - fst.block + 1,
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

        #[strategy(crucible_common::MIN_SHIFT..=crucible_common::MAX_SHIFT)]
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
        extent_count: usize,
        extent_size: usize,
    ) -> ImpactedBlocks {
        match test_iblocks {
            ArbitraryImpactedBlocks::Empty => ImpactedBlocks::Empty,
            ArbitraryImpactedBlocks::InclusiveRange(
                (left_eid, left_block),
                (right_eid_offset, right_block_offset),
            ) => {
                let left_addr = ImpactedAddr {
                    extent_id: left_eid.index(extent_count) as u64,
                    block: left_block.index(extent_size) as u64,
                };

                let right_addr = ImpactedAddr {
                    extent_id: right_eid_offset
                        .index(extent_count - left_addr.extent_id as usize)
                        as u64
                        + left_addr.extent_id,
                    block: right_block_offset
                        .index(extent_size - left_addr.block as usize)
                        as u64
                        + left_addr.block,
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
            ddef.extent_count() as usize,
            ddef.extent_size().value as usize,
        )
    }

    /// Map the ImpactedBlocks to the full range of possible values
    fn reify_impacted_blocks_without_region(
        test_iblocks: ArbitraryImpactedBlocks,
    ) -> ImpactedBlocks {
        reify_impacted_blocks(test_iblocks, usize::MAX, usize::MAX)
    }

    fn region_def_strategy() -> impl Strategy<Value = RegionDefinition> {
        any::<ArbitraryRegionDefinition>().prop_map(reify_region_definition)
    }

    /// Generate a random region definition, and a pair of ImpactedBlocks ranges
    /// that fit within it
    fn region_and_impacted_pair_strategy(
    ) -> impl Strategy<Value = (RegionDefinition, ImpactedBlocks, ImpactedBlocks)>
    {
        any::<(
            ArbitraryRegionDefinition,
            ArbitraryImpactedBlocks,
            ArbitraryImpactedBlocks,
        )>()
        .prop_map(|(test_ddef, test_iblocks_a, test_iblocks_b)| {
            let ddef = reify_region_definition(test_ddef);
            let iblocks_a =
                reify_impacted_blocks_in_region(test_iblocks_a, &ddef);
            let iblocks_b =
                reify_impacted_blocks_in_region(test_iblocks_b, &ddef);
            (ddef, iblocks_a, iblocks_b)
        })
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

    fn any_impacted_blocks_strategy() -> impl Strategy<Value = ImpactedBlocks> {
        any::<ArbitraryImpactedBlocks>()
            .prop_map(reify_impacted_blocks_without_region)
    }

    #[proptest]
    fn intersection_produces_less_than_or_equal_block_count(
        #[strategy(region_and_impacted_pair_strategy())]
        region_and_impacted_pair: (
            RegionDefinition,
            ImpactedBlocks,
            ImpactedBlocks,
        ),
    ) {
        let (ddef, iblocks_a, iblocks_b) = region_and_impacted_pair;
        let intersection = iblocks_a.intersection(&iblocks_b);

        let len_a = iblocks_a.len(&ddef);
        let len_b = iblocks_b.len(&ddef);
        let len_intersection = intersection.len(&ddef);

        prop_assert!(len_intersection <= len_a);
        prop_assert!(len_intersection <= len_b);
    }

    #[proptest]
    fn union_produces_greater_than_or_equal_block_count(
        #[strategy(region_and_impacted_pair_strategy())]
        region_and_impacted_pair: (
            RegionDefinition,
            ImpactedBlocks,
            ImpactedBlocks,
        ),
    ) {
        let (ddef, iblocks_a, iblocks_b) = region_and_impacted_pair;
        let union = iblocks_a.union(&iblocks_b);

        let len_a = iblocks_a.len(&ddef);
        let len_b = iblocks_b.len(&ddef);
        let len_union = union.len(&ddef);

        prop_assert!(len_union >= len_a);
        prop_assert!(len_union >= len_b);
    }

    #[proptest]
    fn intersection_with_empty_is_empty(
        #[strategy(any_impacted_blocks_strategy())] iblocks: ImpactedBlocks,
    ) {
        // a . Empty == Empty
        prop_assert_eq!(
            iblocks.intersection(&ImpactedBlocks::Empty),
            ImpactedBlocks::Empty
        );
    }

    #[proptest]
    fn union_with_empty_is_identity(
        #[strategy(any_impacted_blocks_strategy())] iblocks: ImpactedBlocks,
    ) {
        // a . Empty == a
        prop_assert_eq!(iblocks.union(&ImpactedBlocks::Empty), iblocks);
    }

    #[proptest]
    fn intersection_is_commutative(
        #[strategy(any_impacted_blocks_strategy())] iblocks_a: ImpactedBlocks,
        #[strategy(any_impacted_blocks_strategy())] iblocks_b: ImpactedBlocks,
    ) {
        // a . b == b . a
        prop_assert_eq!(
            iblocks_a.intersection(&iblocks_b),
            iblocks_b.intersection(&iblocks_a)
        );
    }

    #[proptest]
    fn union_is_commutative(
        #[strategy(any_impacted_blocks_strategy())] iblocks_a: ImpactedBlocks,
        #[strategy(any_impacted_blocks_strategy())] iblocks_b: ImpactedBlocks,
    ) {
        // a . b == b . a
        prop_assert_eq!(
            iblocks_a.union(&iblocks_b),
            iblocks_b.union(&iblocks_a)
        );
    }

    #[proptest]
    fn intersection_is_associative(
        #[strategy(any_impacted_blocks_strategy())] iblocks_a: ImpactedBlocks,
        #[strategy(any_impacted_blocks_strategy())] iblocks_b: ImpactedBlocks,
        #[strategy(any_impacted_blocks_strategy())] iblocks_c: ImpactedBlocks,
    ) {
        // a . (b . c)
        let l = iblocks_a.intersection(&iblocks_b.intersection(&iblocks_c));

        // (a . b) . c
        let r = iblocks_a.intersection(&iblocks_b).intersection(&iblocks_c);

        // a . (b . c) == (a . b) . c
        prop_assert_eq!(l, r);
    }

    #[proptest]
    fn union_is_associative(
        #[strategy(any_impacted_blocks_strategy())] iblocks_a: ImpactedBlocks,
        #[strategy(any_impacted_blocks_strategy())] iblocks_b: ImpactedBlocks,
        #[strategy(any_impacted_blocks_strategy())] iblocks_c: ImpactedBlocks,
    ) {
        // a . (b . c)
        let l = iblocks_a.union(&iblocks_b.union(&iblocks_c));

        // (a . b) . c
        let r = iblocks_a.union(&iblocks_b).union(&iblocks_c);

        // a . (b . c) == (a . b) . c
        prop_assert_eq!(l, r);
    }

    #[proptest]
    fn iblocks_from_offset_is_empty_for_zero_blocks(
        #[strategy(1..=u64::MAX)] extent_size: u64,
        start_eid: u64,
        #[strategy(0..#extent_size)] start_block: u64,
    ) {
        prop_assert_eq!(
            ImpactedBlocks::from_offset(
                extent_size,
                ImpactedAddr {
                    extent_id: start_eid,
                    block: start_block
                },
                0
            ),
            ImpactedBlocks::Empty
        );
    }

    #[proptest]
    fn iblocks_from_offset_with_zero_extent_size_panics(
        start_eid: u64,
        start_block: u64,
        n_blocks: u64,
    ) {
        prop_should_panic(|| {
            ImpactedBlocks::from_offset(
                0,
                ImpactedAddr {
                    extent_id: start_eid,
                    block: start_block,
                },
                n_blocks,
            )
        })?;
    }

    #[proptest]
    /// Make sure that when the right address is less than the left address,
    /// ImpactedBlocks::new() panics
    fn iblocks_new_panics_for_flipped_polarity(
        #[strategy(0..=u64::MAX - 1)] start_eid: u64,
        #[strategy(0..=u64::MAX - 1)] start_block: u64,

        #[strategy(#start_eid + 1 ..= u64::MAX)] end_eid: u64,
        #[strategy(#start_block + 1 ..= u64::MAX)] end_block: u64,
    ) {
        let start_addr = ImpactedAddr {
            extent_id: start_eid,
            block: start_block,
        };

        let end_addr = ImpactedAddr {
            extent_id: end_eid,
            block: end_block,
        };

        prop_should_panic(|| ImpactedBlocks::new(end_addr, start_addr))?;
    }

    #[proptest]
    fn iblocks_blocks_iterates_over_all_blocks(
        // Keep these reasonably sized so we don't OOM running this test
        #[strategy(1..=128u32)] extent_count: u32,
        #[strategy(1..=128u64)] extent_size: u64,

        #[strategy(0..#extent_count as u64)] start_eid: u64,
        #[strategy(0..#extent_size)] start_block: u64,

        #[strategy(#start_eid..#extent_count as u64)] end_eid: u64,
        #[strategy(#start_block..#extent_size)] end_block: u64,
    ) {
        // Set up our iblocks
        let iblocks = ImpactedBlocks::new(
            ImpactedAddr {
                extent_id: start_eid,
                block: start_block,
            },
            ImpactedAddr {
                extent_id: end_eid,
                block: end_block,
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
            expected_addresses.push((cur_eid, Block::new_512(cur_block)));
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
            iblocks.blocks(&ddef).collect::<Vec<(u64, Block)>>(),
            expected_addresses
        );
    }

    #[proptest]
    fn iblocks_conflicts_is_commutative(
        #[strategy(any_impacted_blocks_strategy())] iblocks_a: ImpactedBlocks,
        #[strategy(any_impacted_blocks_strategy())] iblocks_b: ImpactedBlocks,
    ) {
        // a . b == b . a
        prop_assert_eq!(
            iblocks_a.conflicts(&iblocks_b),
            iblocks_b.conflicts(&iblocks_a)
        );
    }

    #[proptest]
    fn empty_impacted_blocks_never_conflict(
        #[strategy(any_impacted_blocks_strategy())] iblocks: ImpactedBlocks,
    ) {
        prop_assert!(!iblocks.conflicts(&ImpactedBlocks::Empty));
    }

    #[proptest]
    fn overlapping_impacted_blocks_should_conflict(
        // 4 random points, we'll make two overlapping ranges out of them
        point_a: (u64, u64),
        point_b: (u64, u64),
        point_c: (u64, u64),
        point_d: (u64, u64),
    ) {
        // First we need to sort the points so we can use them correctly.
        let mut sorted_points = vec![point_a, point_b, point_c, point_d];
        sorted_points.sort();
        let (eid_a, block_a) = sorted_points[0];
        let (eid_b, block_b) = sorted_points[1];
        let (eid_c, block_c) = sorted_points[2];
        let (eid_d, block_d) = sorted_points[3];

        // After sorting, if we make ranges from the pairs (a,c) and (b,d)
        // then they are guaranteed to overlap.
        let iblocks_a = ImpactedBlocks::new(
            ImpactedAddr {
                extent_id: eid_a,
                block: block_a,
            },
            ImpactedAddr {
                extent_id: eid_c,
                block: block_c,
            },
        );

        let iblocks_b = ImpactedBlocks::new(
            ImpactedAddr {
                extent_id: eid_b,
                block: block_b,
            },
            ImpactedAddr {
                extent_id: eid_d,
                block: block_d,
            },
        );

        // And thus they should conflict
        prop_assert!(iblocks_a.conflicts(&iblocks_b),
            "These overlapping ImpactedBlocks should conflict, but don't:\n    {:?}\n    {:?}",
            iblocks_a,
            iblocks_b);
    }

    #[proptest]
    fn nothing_contains_empty(
        #[strategy(any_impacted_blocks_strategy())] iblocks: ImpactedBlocks,
    ) {
        prop_assert!(!iblocks.fully_contains(&ImpactedBlocks::Empty));
    }

    #[proptest]
    fn empty_contains_nothing(
        #[strategy(any_impacted_blocks_strategy())] iblocks: ImpactedBlocks,
    ) {
        prop_assert!(!ImpactedBlocks::Empty.fully_contains(&iblocks));
    }

    #[proptest]
    fn subregions_are_contained(
        // proptest doesn't implement strategies for tuple-ranges for some
        // reason, so I'm using a u128 in place of a (u64, u64)
        #[strategy(0..=u128::MAX)] outer_start: u128,
        #[strategy(#outer_start..=u128::MAX)] outer_end: u128,

        // Inner must be within outer
        #[strategy(#outer_start ..= #outer_end)] inner_start: u128,
        #[strategy(#inner_start ..= #outer_end)] inner_end: u128,
    ) {
        let (outer_eid_a, outer_block_a) =
            ((outer_start >> 64) as u64, outer_start as u64);
        let (outer_eid_b, outer_block_b) =
            ((outer_end >> 64) as u64, outer_end as u64);
        let (inner_eid_a, inner_block_a) =
            ((inner_start >> 64) as u64, inner_start as u64);
        let (inner_eid_b, inner_block_b) =
            ((inner_end >> 64) as u64, inner_end as u64);

        let outer_iblocks = ImpactedBlocks::new(
            ImpactedAddr {
                extent_id: outer_eid_a,
                block: outer_block_a,
            },
            ImpactedAddr {
                extent_id: outer_eid_b,
                block: outer_block_b,
            },
        );

        let inner_iblocks = ImpactedBlocks::new(
            ImpactedAddr {
                extent_id: inner_eid_a,
                block: inner_block_a,
            },
            ImpactedAddr {
                extent_id: inner_eid_b,
                block: inner_block_b,
            },
        );

        prop_assert!(outer_iblocks.fully_contains(&inner_iblocks),
            "Outer ImpactedBlocks does not contain inner ImpactedBlocks:\n    Outer: {:?}\n    Inner: {:?}",
            outer_iblocks,
            inner_iblocks);
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

        let shift = ddef.block_size().trailing_zeros();
        let (first_eid, first_block) = iblocks.blocks(&ddef).next().unwrap();
        let first = Block::new(
            first_block.value + first_eid * ddef.extent_size().value,
            shift,
        );
        let num_blocks = Block::new(iblocks.len(&ddef) as u64, shift);

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
        let shift = ddef.block_size().trailing_zeros();
        let first = Block::new(first_block, shift);
        let num_blocks = Block::new(n_blocks, shift);

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
        let shift = ddef.block_size().trailing_zeros();
        let first = Block::new(first_block, shift);
        let num_blocks = Block::new(n_blocks, shift);

        prop_should_panic(|| extent_from_offset(&ddef, first, num_blocks))?;
    }

    #[proptest]
    fn iblocks_extents_returns_correct_extents(
        eid_a: u64,
        block_a: u64,

        #[strategy(#eid_a..=u64::MAX)] eid_b: u64,
        #[strategy(#block_a..=u64::MAX)] block_b: u64,
    ) {
        let addr_a = ImpactedAddr {
            extent_id: eid_a,
            block: block_a,
        };
        let addr_b = ImpactedAddr {
            extent_id: eid_b,
            block: block_b,
        };
        let iblocks = ImpactedBlocks::new(addr_a, addr_b);
        prop_assert_eq!(iblocks.extents(), Some(eid_a..=eid_b));
    }
}
