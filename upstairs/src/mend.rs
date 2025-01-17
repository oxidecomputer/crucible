// Copyright 2023 Oxide Computer Company
use super::*;

/*
 * These structures and methods are used to compare information received
 * from the downstairs in a region set and from that build a list of extents
 * that need repair.
 */

/**
 * This information is collected from each downstairs region in the same
 * region set.  It is used to find differences between them.
 */
#[derive(Debug, Clone)]
pub struct RegionMetadata {
    pub generation: Vec<u64>,
    pub flush_numbers: Vec<u64>,
    pub dirty: Vec<bool>,
}

impl RegionMetadata {
    fn get(&self, i: usize) -> Option<ExtentMetadata> {
        let gen = *self.generation.get(i)?;
        let flush = *self.flush_numbers.get(i)?;
        let dirty = *self.dirty.get(i)?;
        Some(ExtentMetadata { gen, flush, dirty })
    }
}

/// Extent metadata for a single extent
///
/// Note that fields are ordered in reconciliation priority order, so sorting
/// works to pick the highest-priority extent.
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
struct ExtentMetadata {
    gen: u64,
    flush: u64,
    dirty: bool,
}

/**
 * The source client ID of valid data in an extent with a mis-compare, and
 * at least one destination client ID where that data should go.
 */
#[derive(Debug)]
pub struct ExtentFix {
    pub source: ClientId,
    pub dest: Vec<ClientId>,
}

/**
 * A hashmap of extents that need repair, indexed by extent numbers.
 */
#[derive(Debug)]
pub struct DownstairsMend {
    // Index by extent ID
    pub mend: HashMap<ExtentId, ExtentFix>,
}

impl DownstairsMend {
    /*
     * Use the data provided from each downstairs to build a list of extents
     * that need repair.
     */
    pub fn new(
        c0: &RegionMetadata,
        c1: &RegionMetadata,
        c2: &RegionMetadata,
        log: Logger,
    ) -> Option<DownstairsMend> {
        let mut dsm = DownstairsMend {
            mend: HashMap::new(),
        };

        /*
         * Sanity check that all fields of the RegionMetadata struct have the
         * same length.  Pick one vec as the standard and compare.
         */
        let match_len = c0.generation.len();

        if match_len != c1.generation.len() || match_len != c2.generation.len()
        {
            panic!(
                "Downstairs: vec len mismatch for generation: {} {} {}",
                c0.generation.len(),
                c1.generation.len(),
                c2.generation.len()
            );
        }

        if match_len != c0.flush_numbers.len()
            || match_len != c1.flush_numbers.len()
            || match_len != c2.flush_numbers.len()
        {
            panic!(
                "Downstairs: vec len mismatch for flush_number: {} {} {}",
                c0.flush_numbers.len(),
                c1.flush_numbers.len(),
                c2.flush_numbers.len()
            );
        }

        if match_len != c0.dirty.len()
            || match_len != c1.dirty.len()
            || match_len != c2.dirty.len()
        {
            panic!(
                "Downstairs: vec len mismatch for dirty: {} {} {}",
                c0.dirty.len(),
                c1.dirty.len(),
                c2.dirty.len()
            );
        }

        for i in 0..match_len {
            let m0 = c0.get(i).unwrap();
            let m1 = c1.get(i).unwrap();
            let m2 = c2.get(i).unwrap();
            if m0.dirty || m1.dirty || m2.dirty || m0 != m1 || m1 != m2 {
                info!(
                    log,
                    "extent {i} needs reconciliation: {m0:?} {m1:?} {m2:?}"
                );
                let ef = make_repair_list(i, c0, c1, c2, &log);
                dsm.mend.insert(ExtentId(i as u32), ef);
            }
        }

        if dsm.mend.is_empty() {
            None
        } else {
            Some(dsm)
        }
    }
}

/*
 * Given the index of an extent with a mis-compare, pick the source and
 * destination extents to correct the problem.  There will always be one
 * source and at least one destination.
 */
fn make_repair_list(
    i: usize,
    c0: &RegionMetadata,
    c1: &RegionMetadata,
    c2: &RegionMetadata,
    log: &Logger,
) -> ExtentFix {
    let source = find_source(i, c0, c1, c2, log);
    let dest = find_dest(i, source, c0, c1, c2, log);

    ExtentFix { source, dest }
}

/// Find the client ID which should be the source for reconciliation
///
/// This function chooses the source with the
///
/// - Highest generation number
/// - Highest flush number (if generation numbers are equal)
/// - Dirty bit set (if generation and flush numbers are equal)
///
/// If there is still a tie at the end, then the earliest source is chosen
fn find_source(
    i: usize,
    c0: &RegionMetadata,
    c1: &RegionMetadata,
    c2: &RegionMetadata,
    log: &Logger,
) -> ClientId {
    let vs = [c0, c1, c2].map(|c| c.get(i).unwrap());

    let (i, _v) = vs
        .iter()
        .enumerate()
        .rev() // pick the lowest ClientId
        .max_by_key(|(_i, v)| **v) // ExtentMetadata has priority-sorted fields
        .unwrap();
    let out = ClientId::new(i as u8);

    info!(log, "extent:{i} {vs:?} => {out}",);
    out
}

/// Given the source for data from an extent mismatch, figure out which
/// of the remaining extents need updating.
fn find_dest(
    i: usize,
    source: ClientId,
    c0: &RegionMetadata,
    c1: &RegionMetadata,
    c2: &RegionMetadata,
    log: &Logger,
) -> Vec<ClientId> {
    let c = ClientData::from_fn(|cid| {
        [c0, c1, c2][cid.get() as usize].get(i).unwrap()
    });
    let s = c[source];

    let out = ClientId::iter()
        .filter(|&i| i != source && (s.dirty || c[i] != s))
        .collect::<Vec<_>>();

    info!(
        log,
        "find dest for source {source} for extent at index {i} => {out:?}"
    );
    out
}

#[cfg(test)]
mod test {
    use super::*;

    // Create a simple logger
    fn csl() -> Logger {
        build_logger()
    }

    #[test]
    fn reconcile_one() {
        // Verify simple reconcile

        let dsr = RegionMetadata {
            generation: vec![1, 1, 1],
            flush_numbers: vec![3, 3, 3],
            dirty: vec![false, false, false],
        };
        let to_fix = DownstairsMend::new(&dsr, &dsr, &dsr, csl());
        assert!(to_fix.is_none());
    }

    #[test]
    #[should_panic]
    fn reconcile_gen_length_bad() {
        // Verify reconcile fails when generation vec length does
        // not agree between downstairs.
        let dsr = RegionMetadata {
            generation: vec![1, 1, 1],
            flush_numbers: vec![3, 3, 3, 3],
            dirty: vec![false, false, false, false],
        };
        let dsr_long = RegionMetadata {
            generation: vec![1, 1, 1, 1],
            flush_numbers: vec![3, 3, 3, 3],
            dirty: vec![false, false, false, false],
        };
        let _fix = DownstairsMend::new(&dsr, &dsr, &dsr_long, csl());
    }

    #[test]
    #[should_panic]
    fn reconcile_flush_length_bad() {
        // Verify reconcile fails when flush vec length does not
        // agree between downstairs.
        let d1 = RegionMetadata {
            generation: vec![0, 0, 0, 1],
            flush_numbers: vec![0, 0, 0, 0],
            dirty: vec![false, false, false, false],
        };

        let d2 = RegionMetadata {
            generation: vec![0, 0, 0, 1],
            flush_numbers: vec![0, 0, 0],
            dirty: vec![false, false, false, false],
        };
        let _fix = DownstairsMend::new(&d1, &d1, &d2, csl());
    }

    #[test]
    #[should_panic]
    fn reconcile_dirty_length_bad() {
        // Verify reconcile fails when dirty vec length does not
        // agree between downstairs.
        let d1 = RegionMetadata {
            generation: vec![0, 0, 0, 1],
            flush_numbers: vec![0, 0, 0, 0],
            dirty: vec![false, false, false],
        };

        let d2 = RegionMetadata {
            generation: vec![0, 0, 0, 1],
            flush_numbers: vec![0, 0, 0, 0],
            dirty: vec![false, false, false, false],
        };
        let _fix = DownstairsMend::new(&d1, &d2, &d1, csl());
    }

    #[test]
    #[should_panic]
    fn reconcile_length_mismatch() {
        // Verify reconcile fails when the length of the fields don't agree.
        let d1 = RegionMetadata {
            generation: vec![0, 0, 0, 1],
            flush_numbers: vec![0, 0, 0],
            dirty: vec![false, false, false],
        };
        let _fix = DownstairsMend::new(&d1, &d1, &d1, csl());
    }

    #[test]
    fn reconcile_to_repair() {
        // Verify reconcile to_repair returns None when no mismatch

        let dsr = RegionMetadata {
            generation: vec![1, 2, 3, 0],
            flush_numbers: vec![4, 5, 4, 0],
            dirty: vec![false, false, false, false],
        };

        let fix = DownstairsMend::new(&dsr, &dsr, &dsr, csl());
        assert!(fix.is_none());
    }

    #[test]
    fn reconcile_dirty_mismatch_c0() {
        // Verify reconcile reports a mismatch when the dirty
        // numbers for c0 do not agree.  The gens all match, so we
        // look to the flush numbers to break the tie.

        // Build the common elements to share.
        let generation = vec![9, 8, 7, 7];
        let flush_numbers = vec![2, 1, 2, 1];

        let d1 = RegionMetadata {
            generation: generation.clone(),
            flush_numbers: flush_numbers.clone(),
            dirty: vec![false, false, false, false],
        };

        let d2 = RegionMetadata {
            generation: generation.clone(),
            flush_numbers,
            dirty: vec![false, false, true, false],
        };

        let d3 = RegionMetadata {
            generation,
            flush_numbers: vec![2, 1, 3, 1],
            dirty: vec![false, false, true, false],
        };
        let mut fix = DownstairsMend::new(&d1, &d2, &d3, csl()).unwrap();

        // Extent 2 has the mismatch
        let mut ef = fix.mend.remove(&ExtentId(2)).unwrap();

        // As gen agree, we look to flush to break the tie.
        assert_eq!(ef.source, ClientId::new(2));

        // Both extents should be candidates for destination.
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_dirty_mismatch_c2() {
        // Verify reconcile reports a mismatch when the dirty
        // numbers do not agree. In addition, the c0 and c1 have a higher
        // generation number, so only one is the source and the other will
        // not be part of the destination list.

        // Build the common elements to share.
        let flush_numbers = vec![2, 1, 2, 1];

        let d1 = RegionMetadata {
            generation: vec![9, 8, 7, 7],
            flush_numbers: flush_numbers.clone(),
            dirty: vec![false, false, false, false],
        };

        let d2 = RegionMetadata {
            generation: vec![9, 7, 7, 7],
            flush_numbers,
            dirty: vec![false, true, false, false],
        };
        let mut fix = DownstairsMend::new(&d1, &d1, &d2, csl()).unwrap();

        // Extent 1 has the mismatch, so we should find in the HM.
        let mut ef = fix.mend.remove(&ExtentId(1)).unwrap();

        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());
        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_dirty_mismatch_c1() {
        // Verify reconcile reports a mismatch when the dirty
        // numbers for c1 do not agree.

        // Build the common elements to share.
        let generation = vec![9, 8, 7, 7];
        let flush_numbers = vec![2, 1, 2, 1];

        let d1 = RegionMetadata {
            generation: generation.clone(),
            flush_numbers: flush_numbers.clone(),
            dirty: vec![false, false, false, false],
        };

        let d2 = RegionMetadata {
            generation,
            flush_numbers,
            dirty: vec![false, false, true, false],
        };
        let mut fix = DownstairsMend::new(&d1, &d2, &d1, csl()).unwrap();

        // Extent 2 has the mismatch
        let mut ef = fix.mend.remove(&ExtentId(2)).unwrap();

        // As gen and flush agree, we pick the dirty extent for source.
        assert_eq!(ef.source, ClientId::new(1));

        // Both extents should be candidates for destination.
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_dirty_true() {
        // Verify reconcile reports repair to do even if all three
        // downstairs agree on true dirty bits.
        // This test also has two extents with a mismatch, so we verify
        // that as well.
        //
        let d1 = RegionMetadata {
            generation: vec![9, 8, 7, 7],
            flush_numbers: vec![2, 1, 2, 1],
            dirty: vec![true, false, false, true],
        };
        let mut fix = DownstairsMend::new(&d1, &d1, &d1, csl()).unwrap();

        // Extents 0 and 3 have the mismatch
        let mut ef = fix.mend.remove(&ExtentId(0)).unwrap();

        assert_eq!(ef.source, ClientId::new(0));
        println!("ef.dest {:#?}", ef.dest);
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        let mut ef = fix.mend.remove(&ExtentId(3)).unwrap();
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_generation_mismatch_c0() {
        // Verify reconcile reports a mismatch when the c0 generation
        // numbers do not agree.

        let flush_numbers = vec![2, 1, 2, 1];
        let dirty = vec![false, false, false, false];

        let d1 = RegionMetadata {
            generation: vec![9, 8, 7, 0],
            flush_numbers: flush_numbers.clone(),
            dirty: dirty.clone(),
        };

        let d2 = RegionMetadata {
            generation: vec![8, 8, 7, 0],
            flush_numbers,
            dirty,
        };

        let mut fix = DownstairsMend::new(&d1, &d2, &d2, csl()).unwrap();
        let mut ef = fix.mend.remove(&ExtentId(0)).unwrap();

        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_generation_mismatch_c1() {
        // Verify reconcile reports a mismatch when the c1 generation
        // numbers do not agree.

        let flush_numbers = vec![2, 1, 2, 3];
        let dirty = vec![false, false, false, false];

        let d1 = RegionMetadata {
            generation: vec![9, 8, 7, 0],
            flush_numbers: flush_numbers.clone(),
            dirty: dirty.clone(),
        };

        let d2 = RegionMetadata {
            generation: vec![8, 8, 7, 0],
            flush_numbers,
            dirty,
        };

        let mut fix = DownstairsMend::new(&d1, &d2, &d1, csl()).unwrap();

        // Extent 0 has the mismatch
        let mut ef = fix.mend.remove(&ExtentId(0)).unwrap();
        println!("my ef is: {:?}", ef);

        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));

        // Verify there are no more items on the dest list
        assert!(ef.dest.is_empty());

        // Verify there is nothing else to fix
        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_generation_mismatch_c2() {
        // Verify reconcile reports a mismatch when the generation
        // numbers for multiple clients do not agree.
        // Three extents have a mismatch here, but all have different
        // solutions for where to source the correction from.
        //
        let flush_numbers = vec![2, 1, 2, 3];
        let dirty = vec![false, false, false, false];

        let d1 = RegionMetadata {
            generation: vec![7, 8, 7, 5],
            flush_numbers: flush_numbers.clone(),
            dirty: dirty.clone(),
        };

        let d2 = RegionMetadata {
            generation: vec![8, 9, 7, 4],
            flush_numbers: flush_numbers.clone(),
            dirty: dirty.clone(),
        };

        let d3 = RegionMetadata {
            generation: vec![8, 10, 7, 3],
            flush_numbers,
            dirty,
        };

        let mut fix = DownstairsMend::new(&d1, &d2, &d3, csl()).unwrap();

        // Extent 0 has the first mismatch
        let mut ef = fix.mend.remove(&ExtentId(0)).unwrap();

        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert!(ef.dest.is_empty());

        // Extent 1 has the 2nd mismatch
        let mut ef = fix.mend.remove(&ExtentId(1)).unwrap();

        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 3 has the 3rd mismatch
        let mut ef = fix.mend.remove(&ExtentId(3)).unwrap();

        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_flush_mismatch_c0() {
        // Verify reconcile reports a mismatch when the c0 flush
        // numbers do not agree.

        // Generate some shared reconciliation data for all clients
        let generation = vec![9, 8, 7, 7];
        let dirty = vec![false, false, false, false];

        let d1 = RegionMetadata {
            generation: generation.clone(),
            flush_numbers: vec![1, 1, 2, 1],
            dirty: dirty.clone(),
        };

        let d2 = RegionMetadata {
            generation,
            flush_numbers: vec![2, 1, 2, 1],
            dirty,
        };

        let mut fix = DownstairsMend::new(&d1, &d2, &d2, csl()).unwrap();

        // Extent 0 has the first mismatch
        let mut ef = fix.mend.remove(&ExtentId(0)).unwrap();

        // Client 1 (and 2) have the higher flush numbers.
        // Only client 0 needs repair.
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert!(ef.dest.is_empty());

        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_flush_mismatch_c1() {
        // Verify reconcile reports a mismatch when the flush
        // numbers do not agree.  This test has six extents that need
        // repair, and each with a different solution.

        // Generate some shared reconciliation data for all clients
        let generation = vec![9, 8, 7, 7, 6, 5];
        let dirty = vec![false, false, false, false, false, false];

        let d1 = RegionMetadata {
            generation: generation.clone(),
            flush_numbers: vec![1, 2, 3, 3, 1, 2],
            dirty: dirty.clone(),
        };

        let d2 = RegionMetadata {
            generation: generation.clone(),
            flush_numbers: vec![2, 1, 2, 2, 3, 3],
            dirty: dirty.clone(),
        };

        let d3 = RegionMetadata {
            generation,
            flush_numbers: vec![3, 3, 3, 1, 3, 2],
            dirty,
        };

        let mut fix = DownstairsMend::new(&d1, &d2, &d3, csl()).unwrap();

        // Extent 0 has the first mismatch
        let mut ef = fix.mend.remove(&ExtentId(0)).unwrap();

        // Client 2 has the higher flush numbers.
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 1 has the next mismatch
        let mut ef = fix.mend.remove(&ExtentId(1)).unwrap();

        // Client 2 has the higher flush numbers.
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 2 has the next mismatch
        let mut ef = fix.mend.remove(&ExtentId(2)).unwrap();

        // Client 0,2 have the higher flush numbers.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 3 has the next mismatch
        let mut ef = fix.mend.remove(&ExtentId(3)).unwrap();

        // Client 0 has the higher flush numbers. 1,2 need repair
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 4 has the next mismatch
        let mut ef = fix.mend.remove(&ExtentId(4)).unwrap();

        // Client 1,2 have the higher flush numbers. 0 needs repair
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert!(ef.dest.is_empty());

        // Extent 5 has the final mismatch
        let mut ef = fix.mend.remove(&ExtentId(5)).unwrap();

        // Client 1 has the higher flush numbers. 0,2 needs repair
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_flush_mismatch_c2() {
        // Verify reconcile reports a mismatch when the c2 flush
        // numbers do not agree.  There are two extents that are mismatched
        // and the source for both in c2.  Both c0 and c1 need repair.

        // Generate some shared reconciliation data for all clients
        let generation = vec![9, 8, 7, 7];
        let dirty = vec![false, false, false, false];

        let d1 = RegionMetadata {
            generation: generation.clone(),
            flush_numbers: vec![1, 1, 2, 1],
            dirty: dirty.clone(),
        };

        let d2 = RegionMetadata {
            generation,
            flush_numbers: vec![2, 1, 2, 3],
            dirty,
        };

        let mut fix = DownstairsMend::new(&d1, &d1, &d2, csl()).unwrap();
        // Extent 0 has the first mismatch
        let mut ef = fix.mend.remove(&ExtentId(0)).unwrap();

        // Client 2 has the higher flush numbers. 0,1 need repair
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 3 has the last mismatch
        let mut ef = fix.mend.remove(&ExtentId(3)).unwrap();

        // Client 2 has the higher flush numbers. 0,1 need repair
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_one_of_each() {
        // Verify reconcile reports all the correct work to do when
        // there are multiple differences in multiple fields.

        // Generate some reconciliation data
        let d1 = RegionMetadata {
            generation: vec![9, 8, 7, 7],
            flush_numbers: vec![2, 1, 2, 1],
            dirty: vec![false, false, false, true],
        };
        let d2 = RegionMetadata {
            generation: vec![9, 7, 7, 7],
            flush_numbers: vec![2, 1, 2, 1],
            dirty: vec![false, false, true, true],
        };
        let d3 = RegionMetadata {
            generation: vec![9, 8, 8, 7],
            flush_numbers: vec![3, 1, 2, 1],
            dirty: vec![true, false, false, true],
        };

        let mut fix = DownstairsMend::new(&d1, &d2, &d3, csl()).unwrap();
        // Extent 0 has a flush mismatch
        let mut ef = fix.mend.remove(&ExtentId(0)).unwrap();

        // Client 2 has the higher flush numbers. 0,1 need repair
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 1 has gen mismatch
        let mut ef = fix.mend.remove(&ExtentId(1)).unwrap();

        // Client 0 has the higher flush numbers. Only 1 needs repair
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 2 has a gen mismatch, with dirty bits set.
        let mut ef = fix.mend.remove(&ExtentId(2)).unwrap();

        // Client 2 has the higher gen number. 0,1 need repair
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 3 has the last mismatch
        let mut ef = fix.mend.remove(&ExtentId(3)).unwrap();

        // All extents have the dirty bit, but everything else is the same.
        // Use 0 for source, and fix 1,2
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_multiple_source() {
        // Verify reconcile reports a single thing to fix even if the same
        // index is bad in multiple fields.
        // We also test source selection, where the flush number needs to be
        // consulted to break a tie when generation numbers are matching on
        // two extents.

        // Generate some reconciliation data
        let d1 = RegionMetadata {
            generation: vec![9, 7, 7, 7],
            flush_numbers: vec![1, 1, 2, 5],
            dirty: vec![false, false, false, false],
        };
        let d2 = RegionMetadata {
            generation: vec![9, 8, 9, 8],
            flush_numbers: vec![2, 1, 1, 4],
            dirty: vec![false, false, false, false],
        };
        let d3 = RegionMetadata {
            generation: vec![8, 8, 7, 9],
            flush_numbers: vec![3, 2, 3, 3],
            dirty: vec![false, false, false, false],
        };

        let mut fix = DownstairsMend::new(&d1, &d2, &d3, csl()).unwrap();
        // Extent 0 has a flush mismatch
        let mut ef = fix.mend.remove(&ExtentId(0)).unwrap();

        // Client 0 and 1 have the higher gen numbers, but client 1
        // has a higher flush number.  We need that 2nd level check to pick
        // the proper source.
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 1 has a gen mismatch.
        let mut ef = fix.mend.remove(&ExtentId(1)).unwrap();

        // Client 1,2 has the higher gen number.
        // Client 2 has the higher flush number.
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 2 has a gen mismatch.
        let mut ef = fix.mend.remove(&ExtentId(2)).unwrap();

        // Client 1 has the higher gen number. 0,2 need repair
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 3 has the last mismatch
        let mut ef = fix.mend.remove(&ExtentId(3)).unwrap();

        // Client 2 has the highest generation, with 0,1 having higher
        // flush numbers.  Verify C2 is the source.
        // Use 0 for source, and fix 1,2
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_gen_a() {
        // Exhaustive test of all possible generation values (1-3) in
        // first and second rows with a 1 in the third row.
        // flush numbers and dirty bits are the same for all downstairs.

        // Extent ------  0  1  2  3  4  5  6  7  8
        let gen0 = vec![1, 2, 3, 1, 2, 3, 1, 2, 3];
        let gen1 = vec![1, 1, 1, 2, 2, 2, 3, 3, 3];
        let gen2 = vec![1, 1, 1, 1, 1, 1, 1, 1, 1];

        let flush = vec![1, 1, 1, 1, 1, 1, 1, 1, 1];

        let dirty = vec![
            false, false, false, false, false, false, false, false, false,
        ];

        let d0 = RegionMetadata {
            generation: gen0,
            flush_numbers: flush.clone(),
            dirty: dirty.clone(),
        };
        let d1 = RegionMetadata {
            generation: gen1,
            flush_numbers: flush.clone(),
            dirty: dirty.clone(),
        };
        let d2 = RegionMetadata {
            generation: gen2,
            flush_numbers: flush,
            dirty,
        };
        let mut fix = DownstairsMend::new(&d0, &d1, &d2, csl()).unwrap();

        // Extent 0 has no mismatch
        // Extent 1 has a mismatch, so we should find it in the HM.
        let mut ef = fix.mend.remove(&ExtentId(1)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 2
        let mut ef = fix.mend.remove(&ExtentId(2)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));

        // Extent 3
        let mut ef = fix.mend.remove(&ExtentId(3)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));

        // Extent 4
        let mut ef = fix.mend.remove(&ExtentId(4)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 5
        let mut ef = fix.mend.remove(&ExtentId(5)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 6
        let mut ef = fix.mend.remove(&ExtentId(6)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 7
        let mut ef = fix.mend.remove(&ExtentId(7)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 8
        let mut ef = fix.mend.remove(&ExtentId(8)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_gen_b() {
        // Exhaustive test of all possible generation values (1-3) in
        // first and second rows with a 2 in the third row.
        // flush numbers and dirty bits are the same for all downstairs.

        // Extent ------  0  1  2  3  4  5  6  7  8
        let gen0 = vec![1, 2, 3, 1, 2, 3, 1, 2, 3];
        let gen1 = vec![1, 1, 1, 2, 2, 2, 3, 3, 3];
        let gen2 = vec![2, 2, 2, 2, 2, 2, 2, 2, 2];

        let flush = vec![1, 1, 1, 1, 1, 1, 1, 1, 1];

        let dirty = vec![
            false, false, false, false, false, false, false, false, false,
        ];

        let d0 = RegionMetadata {
            generation: gen0,
            flush_numbers: flush.clone(),
            dirty: dirty.clone(),
        };
        let d1 = RegionMetadata {
            generation: gen1,
            flush_numbers: flush.clone(),
            dirty: dirty.clone(),
        };
        let d2 = RegionMetadata {
            generation: gen2,
            flush_numbers: flush,
            dirty,
        };
        let mut fix = DownstairsMend::new(&d0, &d1, &d2, csl()).unwrap();

        // Extent 0 has a mismatch
        let mut ef = fix.mend.remove(&ExtentId(0)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 1 has a mismatch, so we should find it in the HM.
        let mut ef = fix.mend.remove(&ExtentId(1)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 2
        let mut ef = fix.mend.remove(&ExtentId(2)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));

        // Extent 3
        let mut ef = fix.mend.remove(&ExtentId(3)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));

        // Extent 4 has no mismatch

        // Extent 5
        let mut ef = fix.mend.remove(&ExtentId(5)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 6
        let mut ef = fix.mend.remove(&ExtentId(6)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 7
        let mut ef = fix.mend.remove(&ExtentId(7)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 8
        let mut ef = fix.mend.remove(&ExtentId(8)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_gen_c() {
        // Exhaustive test of all possible generation values (1-3) in
        // first and second rows with a 3 in the third row.
        // flush numbers and dirty bits are the same for all downstairs.

        // Extent ------  0  1  2  3  4  5  6  7  8
        let gen0 = vec![1, 2, 3, 1, 2, 3, 1, 2, 3];
        let gen1 = vec![1, 1, 1, 2, 2, 2, 3, 3, 3];
        let gen2 = vec![3, 3, 3, 3, 3, 3, 3, 3, 3];

        let flush = vec![1, 1, 1, 1, 1, 1, 1, 1, 1];

        let dirty = vec![
            false, false, false, false, false, false, false, false, false,
        ];

        let d0 = RegionMetadata {
            generation: gen0,
            flush_numbers: flush.clone(),
            dirty: dirty.clone(),
        };
        let d1 = RegionMetadata {
            generation: gen1,
            flush_numbers: flush.clone(),
            dirty: dirty.clone(),
        };
        let d2 = RegionMetadata {
            generation: gen2,
            flush_numbers: flush,
            dirty,
        };
        let mut fix = DownstairsMend::new(&d0, &d1, &d2, csl()).unwrap();

        // Extent 0 has a mismatch
        let mut ef = fix.mend.remove(&ExtentId(0)).unwrap();
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 1 has a mismatch
        let mut ef = fix.mend.remove(&ExtentId(1)).unwrap();
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 2
        let mut ef = fix.mend.remove(&ExtentId(2)).unwrap();
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 3
        let mut ef = fix.mend.remove(&ExtentId(3)).unwrap();
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 4
        let mut ef = fix.mend.remove(&ExtentId(4)).unwrap();
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 5
        let mut ef = fix.mend.remove(&ExtentId(5)).unwrap();
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 6
        let mut ef = fix.mend.remove(&ExtentId(6)).unwrap();
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert!(ef.dest.is_empty());

        // Extent 7
        let mut ef = fix.mend.remove(&ExtentId(7)).unwrap();
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert!(ef.dest.is_empty());

        // Extent 8  No mismatch

        assert!(fix.mend.is_empty());
    }

    // Now, just do gen matching resolves?  Make sure higher
    // numbered flush with lower gen does not pass also.

    #[test]
    fn reconcile_flush_a() {
        // Exhaustive test of all possible flush values (1-3) in
        // first and second rows with a 1 in the third row.
        // Generation and dirty bits are the same for all downstairs.

        let gen = vec![1, 1, 1, 1, 1, 1, 1, 1, 1];

        // Extent --------  0  1  2  3  4  5  6  7  8
        let flush0 = vec![1, 2, 3, 1, 2, 3, 1, 2, 3];
        let flush1 = vec![1, 1, 1, 2, 2, 2, 3, 3, 3];
        let flush2 = vec![1, 1, 1, 1, 1, 1, 1, 1, 1];

        let dirty = vec![
            false, false, false, false, false, false, false, false, false,
        ];

        let d0 = RegionMetadata {
            generation: gen.clone(),
            flush_numbers: flush0,
            dirty: dirty.clone(),
        };
        let d1 = RegionMetadata {
            generation: gen.clone(),
            flush_numbers: flush1,
            dirty: dirty.clone(),
        };
        let d2 = RegionMetadata {
            generation: gen,
            flush_numbers: flush2,
            dirty,
        };
        let mut fix = DownstairsMend::new(&d0, &d1, &d2, csl()).unwrap();

        // Extent 0 has no mismatch
        // Extent 1 has a mismatch, so we should find it in the HM.
        let mut ef = fix.mend.remove(&ExtentId(1)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 2
        let mut ef = fix.mend.remove(&ExtentId(2)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));

        // Extent 3
        let mut ef = fix.mend.remove(&ExtentId(3)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));

        // Extent 4
        let mut ef = fix.mend.remove(&ExtentId(4)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 5
        let mut ef = fix.mend.remove(&ExtentId(5)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 6
        let mut ef = fix.mend.remove(&ExtentId(6)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 7
        let mut ef = fix.mend.remove(&ExtentId(7)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 8
        let mut ef = fix.mend.remove(&ExtentId(8)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_flush_b() {
        // Exhaustive test of all possible flush values (1-3) in
        // first and second rows with a 2 in the third row.
        // Generation and dirty bits are the same for all downstairs.

        let gen = vec![1, 1, 1, 1, 1, 1, 1, 1, 1];
        // Extent --------- 0  1  2  3  4  5  6  7  8
        let flush0 = vec![1, 2, 3, 1, 2, 3, 1, 2, 3];
        let flush1 = vec![1, 1, 1, 2, 2, 2, 3, 3, 3];
        let flush2 = vec![2, 2, 2, 2, 2, 2, 2, 2, 2];

        let dirty = vec![
            false, false, false, false, false, false, false, false, false,
        ];

        let d0 = RegionMetadata {
            generation: gen.clone(),
            flush_numbers: flush0,
            dirty: dirty.clone(),
        };
        let d1 = RegionMetadata {
            generation: gen.clone(),
            flush_numbers: flush1,
            dirty: dirty.clone(),
        };
        let d2 = RegionMetadata {
            generation: gen,
            flush_numbers: flush2,
            dirty,
        };
        let mut fix = DownstairsMend::new(&d0, &d1, &d2, csl()).unwrap();

        // Extent 0 has a mismatch
        let mut ef = fix.mend.remove(&ExtentId(0)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 1 has a mismatch, so we should find it in the HM.
        let mut ef = fix.mend.remove(&ExtentId(1)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 2
        let mut ef = fix.mend.remove(&ExtentId(2)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));

        // Extent 3
        let mut ef = fix.mend.remove(&ExtentId(3)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));

        // Extent 4 has no mismatch

        // Extent 5
        let mut ef = fix.mend.remove(&ExtentId(5)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 6
        let mut ef = fix.mend.remove(&ExtentId(6)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 7
        let mut ef = fix.mend.remove(&ExtentId(7)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        // Extent 8
        let mut ef = fix.mend.remove(&ExtentId(8)).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(2));
        assert!(ef.dest.is_empty());

        assert!(fix.mend.is_empty());
    }

    #[test]
    fn reconcile_flush_c() {
        // Exhaustive test of all possible flush values (1-3) in
        // first and second rows with 3 in the third row.
        // Generation and dirty bits are the same for all downstairs.

        let gen = vec![1, 1, 1, 1, 1, 1, 1, 1, 1];

        // Extent --------- 0  1  2  3  4  5  6  7  8
        let flush0 = vec![1, 2, 3, 1, 2, 3, 1, 2, 3];
        let flush1 = vec![1, 1, 1, 2, 2, 2, 3, 3, 3];
        let flush2 = vec![3, 3, 3, 3, 3, 3, 3, 3, 3];

        let dirty = vec![
            false, false, false, false, false, false, false, false, false,
        ];

        let d0 = RegionMetadata {
            generation: gen.clone(),
            flush_numbers: flush0,
            dirty: dirty.clone(),
        };
        let d1 = RegionMetadata {
            generation: gen.clone(),
            flush_numbers: flush1,
            dirty: dirty.clone(),
        };
        let d2 = RegionMetadata {
            generation: gen,
            flush_numbers: flush2,
            dirty,
        };
        let mut fix = DownstairsMend::new(&d0, &d1, &d2, csl()).unwrap();

        // Extent 0 has a mismatch
        let mut ef = fix.mend.remove(&ExtentId(0)).unwrap();
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 1 has a mismatch
        let mut ef = fix.mend.remove(&ExtentId(1)).unwrap();
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 2
        let mut ef = fix.mend.remove(&ExtentId(2)).unwrap();
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 3
        let mut ef = fix.mend.remove(&ExtentId(3)).unwrap();
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 4
        let mut ef = fix.mend.remove(&ExtentId(4)).unwrap();
        assert_eq!(ef.source, ClientId::new(2));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 5
        let mut ef = fix.mend.remove(&ExtentId(5)).unwrap();
        assert_eq!(ef.source, ClientId::new(0));
        assert_eq!(ef.dest.remove(0), ClientId::new(1));
        assert!(ef.dest.is_empty());

        // Extent 6
        let mut ef = fix.mend.remove(&ExtentId(6)).unwrap();
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert!(ef.dest.is_empty());

        // Extent 7
        let mut ef = fix.mend.remove(&ExtentId(7)).unwrap();
        assert_eq!(ef.source, ClientId::new(1));
        assert_eq!(ef.dest.remove(0), ClientId::new(0));
        assert!(ef.dest.is_empty());

        // Extent 8  No mismatch
        assert!(fix.mend.is_empty());
    }
}
