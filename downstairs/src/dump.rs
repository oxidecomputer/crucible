// Copyright 2021 Oxide Computer Company
use super::*;
use crate::region::ExtentMeta;

#[derive(Debug, Default)]
struct ExtInfo {
    ei_hm: HashMap<u32, ExtentMeta>,
}

/*
 * Dump the metadata for one or more region directories.
 *
 * If a specific extent is requested, only dump info on that extent.
 */
pub fn dump_region(
    region_dir: Vec<PathBuf>,
    cmp_extent: Option<u32>,
    only_show_differences: bool,
) -> Result<()> {
    /*
     * We are building a two level hashmap.
     * The first level index is the extent number.
     * The second level index is the region_dir index in the region_dir
     * Vec passed to us.
     * We build this first because it makes it easier to print it all out
     * at the end.
     */
    let mut all_extents: HashMap<u32, ExtInfo> = HashMap::new();
    let dir_count = region_dir.len();
    let mut blocks_per_extent = 0;
    let mut total_extents = 0;

    for (index, dir) in region_dir.iter().enumerate() {
        let region = Region::open(&dir, Default::default(), false)?;

        blocks_per_extent = region.def().extent_size().value;
        total_extents = region.def().extent_count();

        /*
         * The extent number is the index in the overall hashmap.
         * For each entry in all_extents hashmap, we have an ExtInfo
         * struct, which is another hashmap where the index is the region
         * directory index and the value is the ExtentMeta for that region.
         */
        for e in &region.extents {
            let en = e.number();

            /*
             * If we are looking at one extent in detail, we skip all the
             * others.
             */
            if let Some(ce) = cmp_extent {
                if en != ce {
                    continue;
                }
            }
            let inner = e.inner();

            /*
             * Create the ExtentMeta struct for this directory's extent
             * number
             */
            let extent_info = ExtentMeta {
                ext_version: 0,
                gen_number: inner.gen_number().unwrap(),
                flush_number: inner.flush_number().unwrap(),
                dirty: inner.dirty().unwrap(),
            };

            /*
             * If we have an entry already, then add this at our directory
             * index.  If we don't the create the hashmap for this index,
             * then add the extent_info to it.
             */
            let ei = all_extents.entry(en).or_default();
            ei.ei_hm.insert(index as u32, extent_info);
        }
    }

    /*
     * If we just want one extent, then go and handle that now.
     * TODO: Support a list of extents to display.
     * TODO: Support a specific compare option, as maybe we don't want to
     *       compare every time.
     */
    if let Some(ce) = cmp_extent {
        if ce >= total_extents {
            bail!(
                "Requested extent {} is a higher index than valid ({})",
                ce,
                total_extents,
            );
        }
        if dir_count < 2 {
            bail!("Need more than one region directory to compare data");
        }
        let en = all_extents.get(&ce).unwrap();
        show_extent(
            region_dir,
            &en.ei_hm,
            ce,
            blocks_per_extent,
            only_show_differences,
        )?;

        return Ok(());
    };

    /*
     * Print out the extent info one extent at a time, in order
     */
    let mut ext_num = all_extents.keys().collect::<Vec<&u32>>();
    ext_num.sort_unstable();

    print!("EXT");
    for _ in 0..dir_count {
        print!("      GEN FLUSH_ID D");
    }
    println!();

    let mut difference_found = false;
    for en in ext_num.iter() {
        if let Some(ei) = all_extents.get(en) {
            let mut columns: [String; 3] =
                ["".to_string(), "".to_string(), "".to_string()];

            for (dir_index, column) in
                columns.iter_mut().enumerate().take(dir_count)
            {
                if let Some(em) = ei.ei_hm.get(&(dir_index as u32)) {
                    let dirty = if em.dirty {
                        "D".to_string()
                    } else {
                        " ".to_string()
                    };
                    *column = format!(
                        "{:8} {:8} {} ",
                        em.gen_number, em.flush_number, dirty
                    );
                } else {
                    *column = "-".to_string();
                }
            }

            let mut different = false;

            for dir_index in 1..dir_count {
                if columns[dir_index - 1] != columns[dir_index] {
                    different = true;
                    break;
                }
            }

            if !only_show_differences || different {
                print!("{:3} ", en);
                for column in columns.iter().take(dir_count) {
                    print!("{}", column);
                }
                println!();
            }

            difference_found |= different;
        } else {
            print!("{:3} ", en);
            println!("No data for {}", en);
        }
    }

    if difference_found {
        bail!("Difference found!");
    }

    Ok(())
}

/*
 * Show the metadata and a block by block diff of a single extent
 * We need at least two directories to compare, and no more than three.
 */
fn show_extent(
    region_dir: Vec<PathBuf>,
    ei_hm: &HashMap<u32, ExtentMeta>,
    cmp_extent: u32,
    blocks_per_extent: u64,
    only_show_differences: bool,
) -> Result<()> {
    /*
     * First, print out the Generation number, the flush ID,
     * and the dirty bit (if set).  We are printing in columns now instead
     * of rows for each region directory.
     */
    let dir_count = region_dir.len();

    println!("           Extent {}", cmp_extent);

    print!("GEN      ");
    for dir_index in 0..dir_count {
        if let Some(em) = ei_hm.get(&(dir_index as u32)) {
            print!("{:8} ", em.gen_number);
        } else {
            print!("- ");
        }
    }
    println!();

    print!("FLUSH_ID ");
    for dir_index in 0..dir_count {
        if let Some(em) = ei_hm.get(&(dir_index as u32)) {
            print!("{:8} ", em.flush_number);
        } else {
            print!("-");
        }
    }
    println!();

    print!("DIRTY    ");
    for dir_index in 0..dir_count {
        if let Some(em) = ei_hm.get(&(dir_index as u32)) {
            let dirty;
            if em.dirty {
                dirty = "D".to_string();
            } else {
                dirty = " ".to_string();
            }
            print!("{:>8} ", dirty);
        } else {
            print!("- ");
        }
    }
    println!();
    println!();

    print!("{0:10}", "");
    for (index, _) in region_dir.iter().enumerate() {
        print!(" {0:^11}", format!("Data {}", index));
    }
    for (index, _) in region_dir.iter().enumerate() {
        print!(" {0:^11}", format!("Nonce {}", index));
    }
    for (index, _) in region_dir.iter().enumerate() {
        print!(" {0:^11}", format!("Tag {}", index));
    }
    print!(" {0:^7}", "DIFF");
    println!();

    /*
     * Compare the data from each block.
     * Print a letter representing the data for each block.
     */
    let mut difference_found = false;
    for block in 0..blocks_per_extent {
        let mut data_columns: [String; 3] =
            ["".to_string(), "".to_string(), "".to_string()];
        let mut nonce_columns: [String; 3] =
            ["".to_string(), "".to_string(), "".to_string()];
        let mut tag_columns: [String; 3] =
            ["".to_string(), "".to_string(), "".to_string()];

        /*
         * Build a Vector to hold our responses, one for each
         * region we are comparing.
         */
        let mut dvec: Vec<ReadResponse> = Vec::with_capacity(dir_count);

        /*
         * Read the requested block in from the extent.  Store it
         * in the Vec based on index.
         */
        for (index, dir) in region_dir.iter().enumerate() {
            let region = Region::open(&dir, Default::default(), false)?;

            dvec.insert(
                index,
                region.single_block_region_read(ReadRequest {
                    eid: cmp_extent as u64,
                    offset: Block::new_with_ddef(block, &region.def()),
                    num_blocks: 1,
                })?,
            );
        }

        /*
         * Compare all the responses to each other.
         *
         * A,B,C all represent unique values in a block. If blocks match,
         * they will share the same letter.
         *
         * Each row is a new block and the values are unrelated to the
         * previous block.
         */

        let mut different = false;

        // first compare data
        let mut status_letters = vec![String::new(); 3];

        if dvec[0].data == dvec[1].data {
            status_letters[0] += "A";
            status_letters[1] += "A";

            if dir_count > 2 {
                if dvec[0].data == dvec[2].data {
                    status_letters[2] += "A";
                } else {
                    status_letters[2] += "C";
                    different = true;
                }
            }
        } else {
            different = true;
            status_letters[0] += "A";
            status_letters[1] += "B";

            if dir_count > 2 {
                if dvec[0].data == dvec[2].data {
                    status_letters[2] += "A";
                } else if dvec[1].data == dvec[2].data {
                    status_letters[2] += "B";
                } else {
                    status_letters[2] += "C";
                }
            }
        }

        for dir_index in 0..dir_count {
            data_columns[dir_index] =
                format!("{0:^11} ", status_letters[dir_index]);
        }

        // then, compare nonces
        let mut status_letters = vec![String::new(); 3];

        if dvec[0].nonce == dvec[1].nonce {
            status_letters[0] += "A";
            status_letters[1] += "A";

            if dir_count > 2 {
                if dvec[0].nonce == dvec[2].nonce {
                    status_letters[2] += "A";
                } else {
                    status_letters[2] += "C";
                    different = true;
                }
            }
        } else {
            different = true;
            status_letters[0] += "A";
            status_letters[1] += "B";

            if dir_count > 2 {
                if dvec[0].nonce == dvec[2].nonce {
                    status_letters[2] += "A";
                } else if dvec[1].nonce == dvec[2].nonce {
                    status_letters[2] += "B";
                } else {
                    status_letters[2] += "C";
                }
            }
        }

        for dir_index in 0..dir_count {
            nonce_columns[dir_index] =
                format!("{0:^11} ", status_letters[dir_index]);
        }

        // then, compare tags
        let mut status_letters = vec![String::new(); 3];

        if dvec[0].tag == dvec[1].tag {
            status_letters[0] += "A";
            status_letters[1] += "A";

            if dir_count > 2 {
                if dvec[0].tag == dvec[2].tag {
                    status_letters[2] += "A";
                } else {
                    status_letters[2] += "C";
                    different = true;
                }
            }
        } else {
            different = true;
            status_letters[0] += "A";
            status_letters[1] += "B";

            if dir_count > 2 {
                if dvec[0].tag == dvec[2].tag {
                    status_letters[2] += "A";
                } else if dvec[1].tag == dvec[2].tag {
                    status_letters[2] += "B";
                } else {
                    status_letters[2] += "C";
                }
            }
        }

        for dir_index in 0..dir_count {
            tag_columns[dir_index] =
                format!("{0:^11} ", status_letters[dir_index]);
        }

        if !only_show_differences || different {
            print!("Block {:4}", block);

            for column in data_columns.iter().take(dir_count) {
                print!("{}", column);
            }
            for column in nonce_columns.iter().take(dir_count) {
                print!("{}", column);
            }
            for column in tag_columns.iter().take(dir_count) {
                print!("{}", column);
            }

            print!("{0:^7}", if different { "<-------" } else { "" });

            println!();
        }

        difference_found |= different;
    }

    if difference_found {
        bail!("Difference found!");
    }

    Ok(())
}
