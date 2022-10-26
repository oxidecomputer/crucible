// Copyright 2022 Oxide Computer Company

use super::*;

use bitvec::prelude::*;
use itertools::Itertools;
use std::ops::BitAnd;

/// Store a list of impacted blocks for later job dependency calculation
#[derive(Debug, Default, PartialEq)]
pub struct ImpactedBlocks {
    ddef: RegionDefinition,

    // extent id -> list of block offsets
    blocks: HashMap<u64, BitVec>,
}

impl ImpactedBlocks {
    pub fn new(ddef: RegionDefinition) -> Self {
        ImpactedBlocks {
            ddef,
            blocks: HashMap::default(),
        }
    }

    pub fn blocks_in_extent(&self) -> usize {
        self.ddef.extent_size().value as usize
    }

    pub fn add(&mut self, extent_id: u64, block: Block) {
        let blocks_in_extent = self.blocks_in_extent();
        let bv = self.blocks.entry(extent_id).or_insert_with(|| {
            let mut bv = BitVec::with_capacity(blocks_in_extent);
            bv.resize(blocks_in_extent, false);
            bv
        });

        bv.set(block.value as usize, true);
    }

    /// Return true if this list of impacted blocks overlaps with another.
    pub fn conflicts(&self, other: &ImpactedBlocks) -> bool {
        for shared_key in self
            .blocks
            .keys()
            .filter(|key| other.blocks.contains_key(key))
        {
            // TODO any way to avoid this clone?
            let bv = self.blocks[shared_key].clone();
            if bv.bitand(&other.blocks[shared_key]).any() {
                return true;
            }
        }

        false
    }

    /// Returns (extent id, block) tuples
    pub fn tuples(&self) -> Vec<(u64, Block)> {
        let mut result = Vec::with_capacity(self.len());

        let sorted_keys = self.blocks.keys().sorted();
        for eid in sorted_keys {
            let block_offsets =
                self.blocks[eid].iter_ones().collect::<Vec<usize>>();
            for i in block_offsets.iter().sorted() {
                result
                    .push((*eid, Block::new_with_ddef(*i as u64, &self.ddef)));
            }
        }

        result
    }

    /// Returns the number of impacted blocks
    pub fn len(&self) -> usize {
        let mut len = 0;

        for bv in self.blocks.values() {
            len += bv.iter_ones().count();
        }

        len
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
    ddef: RegionDefinition,
    offset: Block,
    num_blocks: Block,
) -> ImpactedBlocks {
    assert!(num_blocks.value > 0);
    assert!(
        (offset.value + num_blocks.value)
            <= (ddef.extent_size().value * ddef.extent_count() as u64)
    );
    assert_eq!(offset.block_size_in_bytes() as u64, ddef.block_size());

    let mut result = ImpactedBlocks::new(ddef);
    let mut o: u64 = offset.value;
    let mut blocks_left: u64 = num_blocks.value;

    while blocks_left > 0 {
        /*
         * XXX We only support a single region (downstairs). When we grow to
         * support a LBA size that is larger than a single region, then we
         * will need to write more code.
         */
        let eid: u64 = o / ddef.extent_size().value;
        assert!((eid as u32) < ddef.extent_count());

        let extent_offset: u64 = o % ddef.extent_size().value;
        let sz: u64 = 1; // one block at a time

        result.add(eid, Block::new_with_ddef(extent_offset, &ddef));

        match blocks_left.checked_sub(sz) {
            Some(v) => {
                blocks_left = v;
            }
            None => {
                break;
            }
        }

        o += sz;
    }

    result
}

#[cfg(test)]
mod test {
    use super::*;

    fn extent_tuple(eid: u64, offset: u64) -> (u64, Block) {
        (eid, Block::new_512(offset))
    }

    #[test]
    fn test_extent_from_offset() {
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(2));
        ddef.set_extent_count(10);

        // Test block size, less than extent size
        assert_eq!(
            extent_from_offset(ddef, Block::new_512(0), Block::new_512(1))
                .tuples(),
            vec![extent_tuple(0, 0)],
        );

        // Test greater than block size, less than extent size
        assert_eq!(
            extent_from_offset(ddef, Block::new_512(0), Block::new_512(2))
                .tuples(),
            vec![extent_tuple(0, 0), extent_tuple(0, 1),],
        );

        // Test greater than extent size
        assert_eq!(
            extent_from_offset(ddef, Block::new_512(0), Block::new_512(4))
                .tuples(),
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
                .tuples(),
            vec![
                extent_tuple(0, 1),
                extent_tuple(1, 0),
                extent_tuple(1, 1),
                extent_tuple(2, 0),
            ],
        );

        assert_eq!(
            extent_from_offset(ddef, Block::new_512(2), Block::new_512(4))
                .tuples(),
            vec![
                extent_tuple(1, 0),
                extent_tuple(1, 1),
                extent_tuple(2, 0),
                extent_tuple(2, 1),
            ],
        );

        assert_eq!(
            extent_from_offset(ddef, Block::new_512(2), Block::new_512(16))
                .tuples(),
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
        let mut ddef = RegionDefinition::default();
        ddef.set_block_size(512);
        ddef.set_extent_size(Block::new_512(2));
        ddef.set_extent_count(10);

        assert_eq!(
            extent_from_offset(
                ddef,
                Block::new_512(2), // offset
                Block::new_512(1), // num_blocks
            )
            .tuples(),
            vec![extent_tuple(1, 0),]
        );

        assert_eq!(
            extent_from_offset(
                ddef,
                Block::new_512(2), // offset
                Block::new_512(2), // num_blocks
            )
            .tuples(),
            vec![extent_tuple(1, 0), extent_tuple(1, 1),]
        );

        assert_eq!(
            extent_from_offset(
                ddef,
                Block::new_512(2), // offset
                Block::new_512(3), // num_blocks
            )
            .tuples(),
            vec![extent_tuple(1, 0), extent_tuple(1, 1), extent_tuple(2, 0),]
        );
    }
}
