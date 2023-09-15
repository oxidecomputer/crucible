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
    pub mend: HashMap<usize, ExtentFix>,
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

        /*
         * As we walk the extents in our RegionMetadata vec, keep track
         * of which extents we did not find a dirty bit set so we can
         * continue to check for differences in gen/flush but only for
         * extents we don't already know are a mismatch.
         */
        let mut to_check = Vec::new();

        /*
         * Any dirty bit set means that extent needs repair.
         * Pick our source extent, and from that we decide which of the other
         * two extents also need repair.
         */
        for (i, dirty0) in c0.dirty.iter().enumerate() {
            if *dirty0 || c1.dirty[i] || c2.dirty[i] {
                info!(log, "Extents {} dirty", i);
                let log_mrl = log.new(o!("mrl" => "dirty".to_string()));
                let ef = make_repair_list(i, c0, c1, c2, log_mrl);
                dsm.mend.insert(i, ef);
            } else {
                to_check.push(i);
            }
        }

        /*
         * Find the index for any flush numbers that don't agree and
         * add that to our to fix list.  If an index is okay, then add
         * it to the final check for generation number.
         */
        let mut second_check = Vec::new();
        for i in to_check.iter() {
            if c0.flush_numbers[*i] != c1.flush_numbers[*i]
                || c1.flush_numbers[*i] != c2.flush_numbers[*i]
            {
                info!(log, "Extent {} has flush number mismatch", i);
                let log_mrl =
                    log.new(o!("mrl" => "flush_mismatch".to_string()));
                let ef = make_repair_list(*i, c0, c1, c2, log_mrl);
                dsm.mend.insert(*i, ef);
            } else {
                second_check.push(*i);
            }
        }

        /*
         * Walk all remaining indexes and add to the to_fix list
         * any that are not all the same.
         */
        for i in second_check.iter() {
            if c0.generation[*i] != c1.generation[*i]
                || c1.generation[*i] != c2.generation[*i]
            {
                info!(log, "generation number mismatch {}", i);
                let log_mrl = log.new(o!("mrl" => "gen_mismatch".to_string()));
                let ef = make_repair_list(*i, c0, c1, c2, log_mrl);
                dsm.mend.insert(*i, ef);
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
    log: Logger,
) -> ExtentFix {
    let source = find_source(i, c0, c1, c2, &log);
    let dest = find_dest(i, source, c0, c1, c2, &log);

    ExtentFix { source, dest }
}

/*
 * Given the index of an extent that has a mismatch, return the client ID
 * (0, 1, or 2) for the extent that should be the source of the repair
 * This requires that you have a mismatch at the provided extent, or the
 * dirty bit is set somewhere.
 *
 * Source selection is done by filtering the extent info in this order, until
 * we have a single extent remaining:
 *
 * Highest generation number.
 * Highest flush number.
 * Dirty bit set.
 *
 * If there is still a tie at the end, then just pick one.
 */
fn find_source(
    i: usize,
    c0: &RegionMetadata,
    c1: &RegionMetadata,
    c2: &RegionMetadata,
    log: &Logger,
) -> ClientId {
    /*
     * All three client IDs are candidates for the max generation number,
     * remove a client ID as we find it has a lower value than the others.
     */
    let mut max_gen = vec![ClientId(0), ClientId(1), ClientId(2)];

    info!(log, "First source client ID for extent {}", i);

    let gen0 = c0.generation[i];
    let gen1 = c1.generation[i];
    let gen2 = c2.generation[i];

    info!(log, "extent:{}  gens: {} {} {}", i, gen0, gen1, gen2);

    if gen0 != gen1 || gen1 != gen2 {
        /*
         * We know at least one of the generation numbers is greater,
         * so this first check will see if one is the largest.
         */
        if gen0 > gen1 && gen0 > gen2 {
            return ClientId(0);
        } else if gen1 > gen0 && gen1 > gen2 {
            return ClientId(1);
        } else if gen2 > gen0 && gen2 > gen1 {
            return ClientId(2);
        }

        /*
         * If we are here, then two gen numbers match, and we need to take
         * the third lower generation number off the list for comparison
         * before we move on to try to break the tie with flush numbers.
         */
        if gen0 == gen1 {
            max_gen.remove(2);
        } else if gen0 == gen2 {
            max_gen.remove(1);
        } else {
            assert_eq!(gen1, gen2);
            max_gen.remove(0);
        }
    }

    /*
     * Our generation numbers did not break the tie, either they are all
     * the same, or we have just two that are the same and have removed
     * the lower one.  Now look for a flush number that is greater.
     *
     * Put the three downstairs RegionMetadata structs into an array with the
     * index being the client ID.  We use the max_gen vec to see if any
     * of the remaining client IDs have a higher flush number.
     */
    let rec = ClientData([c0, c1, c2]);

    info!(
        log,
        "extent:{}  flush: {} {} {} scs: {:?}",
        i,
        c0.flush_numbers[i],
        c1.flush_numbers[i],
        c2.flush_numbers[i],
        max_gen,
    );

    let mut max = 0;
    let mut max_flush = Vec::new();

    for sc in max_gen.iter() {
        if rec[*sc].flush_numbers[i] > max {
            max = rec[*sc].flush_numbers[i];
        }
    }
    for sc in max_gen.iter() {
        if rec[*sc].flush_numbers[i] == max {
            max_flush.push(*sc);
        }
    }

    info!(log, "max_flush now has: {:?}", max_flush);
    if max_flush.len() == 1 {
        return max_flush[0];
    }

    /*
     * To get here we have at least two (and possibly three) extents where
     * the gen and flush are the same.  All that remains to break the tie is
     * an extent with the dirty bit set.
     */
    info!(
        log,
        "extent:{}  dirty: {} {} {}", i, c0.dirty[i], c1.dirty[i], c2.dirty[i],
    );
    for sc in max_flush.iter() {
        if rec[*sc].dirty[i] {
            return *sc;
        }
    }

    /*
     * To get here, the mismatch has to be a dirty bit set on an extent
     * that had lower gen or flush numbers and is no longer under
     * consideration, with the remaining two client extents having
     * matching gen and flush numbers.
     */
    info!(log, "No maxes found, left with: {:?}", max_flush);
    assert_eq!(max_flush.len(), 2);

    /*
     * Note that by always returning the lowest element in the vec, we
     * are biasing the repair to select it.  It is also expected in the
     * tests, so if you change it here, you will have to change it
     * there as well.
     */
    max_flush[0]
}

/*
 * Given the source for data from an extent mismatch, figure out which
 * of the remaining extents need updating.
 */
fn find_dest(
    i: usize,
    source: ClientId,
    c0: &RegionMetadata,
    c1: &RegionMetadata,
    c2: &RegionMetadata,
    log: &Logger,
) -> Vec<ClientId> {
    assert!(source.0 < 3);
    let mut dest: Vec<ClientId> = Vec::new();

    /*
     * Put the three downstairs RegionMetadata structs into an array with the
     * index being the client ID.
     */
    let rec = ClientData([c0, c1, c2]);

    info!(
        log,
        "find dest for source {} for extent at index {}", source, i
    );

    let to_check = match source {
        ClientId(0) => vec![ClientId(1), ClientId(2)],
        ClientId(1) => vec![ClientId(0), ClientId(2)],
        ClientId(_) => vec![ClientId(0), ClientId(1)],
    };

    for dc in to_check.iter() {
        if rec[source].generation[i] != rec[*dc].generation[i] {
            dest.push(*dc);
            info!(log, "source {}, add dest {} gen", source, dc);
            continue;
        }
        if rec[source].flush_numbers[i] != rec[*dc].flush_numbers[i] {
            dest.push(*dc);
            info!(log, "source {}, add dest {} flush", source, dc);
            continue;
        }
        if rec[source].dirty[i] {
            dest.push(*dc);
            info!(log, "source {}, add dest {} source flush", source, dc);
            continue;
        }
        if rec[*dc].dirty[i] {
            dest.push(*dc);
            info!(log, "source {}, add dest {} dc flush", source, dc);
            continue;
        }
    }
    dest
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
        let mut ef = fix.mend.remove(&2).unwrap();

        // As gen agree, we look to flush to break the tie.
        assert_eq!(ef.source, ClientId(2));

        // Both extents should be candidates for destination.
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
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
        let mut ef = fix.mend.remove(&1).unwrap();

        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
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
        let mut ef = fix.mend.remove(&2).unwrap();

        // As gen and flush agree, we pick the dirty extent for source.
        assert_eq!(ef.source, ClientId(1));

        // Both extents should be candidates for destination.
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
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
        let mut ef = fix.mend.remove(&0).unwrap();

        assert_eq!(ef.source, ClientId(0));
        println!("ef.dest {:#?}", ef.dest);
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        let mut ef = fix.mend.remove(&3).unwrap();
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));
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
        let mut ef = fix.mend.remove(&0).unwrap();

        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));
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
        let mut ef = fix.mend.remove(&0).unwrap();
        println!("my ef is: {:?}", ef);

        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));

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
        let mut ef = fix.mend.remove(&0).unwrap();

        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert!(ef.dest.is_empty());

        // Extent 1 has the 2nd mismatch
        let mut ef = fix.mend.remove(&1).unwrap();

        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 3 has the 3rd mismatch
        let mut ef = fix.mend.remove(&3).unwrap();

        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));
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
        let mut ef = fix.mend.remove(&0).unwrap();

        // Client 1 (and 2) have the higher flush numbers.
        // Only client 0 needs repair.
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
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
        let mut ef = fix.mend.remove(&0).unwrap();

        // Client 2 has the higher flush numbers.
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 1 has the next mismatch
        let mut ef = fix.mend.remove(&1).unwrap();

        // Client 2 has the higher flush numbers.
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 2 has the next mismatch
        let mut ef = fix.mend.remove(&2).unwrap();

        // Client 0,2 have the higher flush numbers.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 3 has the next mismatch
        let mut ef = fix.mend.remove(&3).unwrap();

        // Client 0 has the higher flush numbers. 1,2 need repair
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 4 has the next mismatch
        let mut ef = fix.mend.remove(&4).unwrap();

        // Client 1,2 have the higher flush numbers. 0 needs repair
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert!(ef.dest.is_empty());

        // Extent 5 has the final mismatch
        let mut ef = fix.mend.remove(&5).unwrap();

        // Client 1 has the higher flush numbers. 0,2 needs repair
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
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
        let mut ef = fix.mend.remove(&0).unwrap();

        // Client 2 has the higher flush numbers. 0,1 need repair
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 3 has the last mismatch
        let mut ef = fix.mend.remove(&3).unwrap();

        // Client 2 has the higher flush numbers. 0,1 need repair
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
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
        let mut ef = fix.mend.remove(&0).unwrap();

        // Client 2 has the higher flush numbers. 0,1 need repair
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 1 has gen mismatch
        let mut ef = fix.mend.remove(&1).unwrap();

        // Client 0 has the higher flush numbers. Only 1 needs repair
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 2 has a gen mismatch, with dirty bits set.
        let mut ef = fix.mend.remove(&2).unwrap();

        // Client 2 has the higher gen number. 0,1 need repair
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 3 has the last mismatch
        let mut ef = fix.mend.remove(&3).unwrap();

        // All extents have the dirty bit, but everything else is the same.
        // Use 0 for source, and fix 1,2
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));
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
        let mut ef = fix.mend.remove(&0).unwrap();

        // Client 0 and 1 have the higher gen numbers, but client 1
        // has a higher flush number.  We need that 2nd level check to pick
        // the proper source.
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 1 has a gen mismatch.
        let mut ef = fix.mend.remove(&1).unwrap();

        // Client 1,2 has the higher gen number.
        // Client 2 has the higher flush number.
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 2 has a gen mismatch.
        let mut ef = fix.mend.remove(&2).unwrap();

        // Client 1 has the higher gen number. 0,2 need repair
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 3 has the last mismatch
        let mut ef = fix.mend.remove(&3).unwrap();

        // Client 2 has the highest generation, with 0,1 having higher
        // flush numbers.  Verify C2 is the source.
        // Use 0 for source, and fix 1,2
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
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
        let mut ef = fix.mend.remove(&1).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 2
        let mut ef = fix.mend.remove(&2).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));

        // Extent 3
        let mut ef = fix.mend.remove(&3).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));

        // Extent 4
        let mut ef = fix.mend.remove(&4).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 5
        let mut ef = fix.mend.remove(&5).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 6
        let mut ef = fix.mend.remove(&6).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 7
        let mut ef = fix.mend.remove(&7).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 8
        let mut ef = fix.mend.remove(&8).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
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
        let mut ef = fix.mend.remove(&0).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 1 has a mismatch, so we should find it in the HM.
        let mut ef = fix.mend.remove(&1).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 2
        let mut ef = fix.mend.remove(&2).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));

        // Extent 3
        let mut ef = fix.mend.remove(&3).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));

        // Extent 4 has no mismatch

        // Extent 5
        let mut ef = fix.mend.remove(&5).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 6
        let mut ef = fix.mend.remove(&6).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 7
        let mut ef = fix.mend.remove(&7).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 8
        let mut ef = fix.mend.remove(&8).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
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
        let mut ef = fix.mend.remove(&0).unwrap();
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 1 has a mismatch
        let mut ef = fix.mend.remove(&1).unwrap();
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 2
        let mut ef = fix.mend.remove(&2).unwrap();
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 3
        let mut ef = fix.mend.remove(&3).unwrap();
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 4
        let mut ef = fix.mend.remove(&4).unwrap();
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 5
        let mut ef = fix.mend.remove(&5).unwrap();
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 6
        let mut ef = fix.mend.remove(&6).unwrap();
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert!(ef.dest.is_empty());

        // Extent 7
        let mut ef = fix.mend.remove(&7).unwrap();
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
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
        let mut ef = fix.mend.remove(&1).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 2
        let mut ef = fix.mend.remove(&2).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));

        // Extent 3
        let mut ef = fix.mend.remove(&3).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));

        // Extent 4
        let mut ef = fix.mend.remove(&4).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 5
        let mut ef = fix.mend.remove(&5).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 6
        let mut ef = fix.mend.remove(&6).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 7
        let mut ef = fix.mend.remove(&7).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 8
        let mut ef = fix.mend.remove(&8).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
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
        let mut ef = fix.mend.remove(&0).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 1 has a mismatch, so we should find it in the HM.
        let mut ef = fix.mend.remove(&1).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 2
        let mut ef = fix.mend.remove(&2).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));

        // Extent 3
        let mut ef = fix.mend.remove(&3).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));

        // Extent 4 has no mismatch

        // Extent 5
        let mut ef = fix.mend.remove(&5).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 6
        let mut ef = fix.mend.remove(&6).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 7
        let mut ef = fix.mend.remove(&7).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
        assert!(ef.dest.is_empty());

        // Extent 8
        let mut ef = fix.mend.remove(&8).unwrap();
        // Pick the higher gen for source.
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(2));
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
        let mut ef = fix.mend.remove(&0).unwrap();
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 1 has a mismatch
        let mut ef = fix.mend.remove(&1).unwrap();
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 2
        let mut ef = fix.mend.remove(&2).unwrap();
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 3
        let mut ef = fix.mend.remove(&3).unwrap();
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 4
        let mut ef = fix.mend.remove(&4).unwrap();
        assert_eq!(ef.source, ClientId(2));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 5
        let mut ef = fix.mend.remove(&5).unwrap();
        assert_eq!(ef.source, ClientId(0));
        assert_eq!(ef.dest.remove(0), ClientId(1));
        assert!(ef.dest.is_empty());

        // Extent 6
        let mut ef = fix.mend.remove(&6).unwrap();
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert!(ef.dest.is_empty());

        // Extent 7
        let mut ef = fix.mend.remove(&7).unwrap();
        assert_eq!(ef.source, ClientId(1));
        assert_eq!(ef.dest.remove(0), ClientId(0));
        assert!(ef.dest.is_empty());

        // Extent 8  No mismatch
        assert!(fix.mend.is_empty());
    }
}
