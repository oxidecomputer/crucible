// Copyright 2021 Oxide Computer Company
use super::*;
use crate::extent::ExtentMeta;
use std::convert::TryInto;

use sha2::{Digest, Sha256};

#[derive(Debug, Default)]
struct ExtInfo {
    ei_hm: HashMap<u32, ExtentMeta>,
}

/*
 * Dump the metadata for one or more region directories.
 *
 * If a specific extent is requested, only dump info on that extent. If a
 * specific block offset is supplied, show details for only that block.
 *
 * If you don't want color, then set nc to true.
 */
pub async fn dump_region(
    region_dir: Vec<PathBuf>,
    mut cmp_extent: Option<u32>,
    block: Option<u64>,
    only_show_differences: bool,
    nc: bool,
    log: Logger,
) -> Result<()> {
    if cmp_extent.is_some() && block.is_some() {
        bail!("Either a specific block, or a specific extent, not both");
    }
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

    assert!(!region_dir.is_empty());
    for (index, dir) in region_dir.iter().enumerate() {
        // Open Region read only
        let region =
            Region::open(dir, Default::default(), false, true, &log).await?;

        blocks_per_extent = region.def().extent_size().value;
        total_extents = region.def().extent_count();

        let max_block = total_extents as u64 * blocks_per_extent;
        /*
         * The extent number is the index in the overall hashmap.
         * For each entry in all_extents hashmap, we have an ExtInfo
         * struct, which is another hashmap where the index is the region
         * directory index and the value is the ExtentMeta for that region.
         */
        for e in &region.extents {
            let e = match e {
                extent::ExtentState::Opened(extent) => extent,
                extent::ExtentState::Closed => panic!("dump on closed extent!"),
            };
            let en = e.number();

            /*
             * If we want just a specific block, then figure out what extent
             * that block belongs to so we can just display the
             * requested information. We only need to do this
             * once.
             */
            if cmp_extent.is_none() {
                if let Some(b) = block {
                    let ce = (b / blocks_per_extent) as u32;
                    if ce >= total_extents {
                        bail!(
                            "Requested block {} > max block {}",
                            b,
                            max_block - 1,
                        );
                    }
                    cmp_extent = Some(ce);
                }
            }

            /*
             * If we are looking at one extent in detail, we skip all the
             * others.
             */
            if let Some(ce) = cmp_extent {
                if en != ce {
                    continue;
                }
            }

            let extent_info = e.get_meta_info().await;

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

        /*
         * If we only want details about one block, show that
         */
        if let Some(block) = block {
            return show_extent_block(
                region_dir,
                ce,
                block,
                blocks_per_extent,
                only_show_differences,
                nc,
                log,
            )
            .await;
        }

        show_extent(
            region_dir,
            &en.ei_hm,
            ce,
            blocks_per_extent,
            only_show_differences,
            nc,
            log,
        )
        .await?;

        return Ok(());
    };

    /*
     * Print out the extent info one extent at a time, in order
     */
    let mut ext_num = all_extents.keys().collect::<Vec<&u32>>();
    ext_num.sort_unstable();

    // Width for EXT column.
    let ext_width = std::cmp::max(3, total_extents.to_string().len());
    // Width for BLOCKS column
    let max_block = total_extents as u64 * blocks_per_extent;
    // Get the max possible width for a single block
    let block_width = std::cmp::max(3, max_block.to_string().len());
    // Now compute width for BLOCK-BLOCK
    let block_header = std::cmp::max(7, block_width * 2 + 1);

    // We don't know these yet, but we will have a good guess
    // once we start the loop.
    let mut gen_width = 4;
    let mut fl_width = 3;

    // If our extent is invalid, then there is some other problem,
    // but, whomever is using this tool is probably trying to figure
    // out what is wrong, and we should make an effort to show what
    // we can.
    let mut difference_found = false;
    let mut max_gen = 0;
    let mut max_flush = 0;
    let mut print_header = true;
    for en in ext_num.iter() {
        if let Some(ei) = all_extents.get(en) {
            let mut bad_extent = false;
            let mut different = false;
            let mut gen_vec: Vec<u64> = Vec::with_capacity(dir_count);
            let mut flush_vec: Vec<u64> = Vec::with_capacity(dir_count);
            let mut dirty_vec: Vec<bool> = Vec::with_capacity(dir_count);

            for dir_index in 0..dir_count {
                if let Some(em) = ei.ei_hm.get(&(dir_index as u32)) {
                    gen_vec.insert(dir_index, em.gen_number);
                    if gen_vec[0] != em.gen_number {
                        different = true;
                    }
                    if em.gen_number > max_gen {
                        max_gen = em.gen_number;
                    }
                    flush_vec.insert(dir_index, em.flush_number);
                    if flush_vec[0] != em.flush_number {
                        different = true;
                    }
                    if em.flush_number > max_flush {
                        max_flush = em.flush_number;
                    }
                    dirty_vec.insert(dir_index, em.dirty);
                    if dirty_vec[0] != em.dirty {
                        different = true;
                    }
                } else {
                    println!(
                        "{:>width$} column {} bad ",
                        en,
                        dir_index,
                        width = ext_width
                    );
                    bad_extent = true;
                }
            }
            if bad_extent || (only_show_differences && !different) {
                continue;
            }

            if print_header {
                // Because we don't know how large gen or flush is yet, we
                // wait to print the header until we have at least one
                // value to base our guess on.  We add one column to
                // whatever max we have found so far with the hope that this
                // will cover most cases.
                print!("{:>0width$}", "EXT", width = ext_width);

                print!(" {:>0width$}", "BLOCKS", width = block_header);

                // Width for GEN columns
                gen_width =
                    std::cmp::max(gen_width, max_gen.to_string().len() + 1);
                for i in 0..dir_count {
                    print!(" {:>0width$}{}", "GEN", i, width = (gen_width - 1));
                }

                // Width for Flush columns
                fl_width =
                    std::cmp::max(fl_width, max_flush.to_string().len() + 1);
                print!(" ");
                for i in 0..dir_count {
                    print!(" {:>0width$}{}", "FL", i, width = (fl_width - 1));
                }

                // Dirty bit is always the same width
                print!(" ");
                for i in 0..dir_count {
                    print!(" D{}", i);
                }

                if nc {
                    print!(" DIFF");
                }
                println!();

                print_header = false;
            }

            // Ext #
            print!("{:>width$} ", en, width = ext_width);

            // Blocks
            print!(
                "{:0width$}-{:0width$}",
                blocks_per_extent * (**en as u64),
                blocks_per_extent * (**en as u64) + blocks_per_extent - 1,
                width = block_width,
            );

            // Gen
            let color = color_vec(&gen_vec);
            for i in 0..dir_count {
                print!(
                    " {}{:>width$}",
                    sgr(color[i], nc),
                    gen_vec[i],
                    width = gen_width
                );
            }
            // Clear color
            print!("{} ", sgr(0, nc));

            // Flush
            let color = color_vec(&flush_vec);
            for i in 0..dir_count {
                print!(
                    " {}{:>width$}",
                    sgr(color[i], nc),
                    flush_vec[i],
                    width = fl_width
                );
            }
            // Clear color
            print!("{} ", sgr(0, nc));

            // Dirty bit T or F
            for dv in dirty_vec.iter().take(dir_count) {
                if *dv {
                    print!("  {}T", sgr(31, nc));
                } else {
                    print!("  {}F", sgr(32, nc));
                }
            }
            if nc && different {
                println!(" <---");
            } else {
                // Clear color
                println!("{}", sgr(0, nc));
            }

            difference_found |= different;
        } else {
            print!("{:>width$} ", en, width = ext_width);
            println!("No data for {}", en);
        }
    }

    println!("Max gen: {},  Max flush: {}", max_gen, max_flush);
    if difference_found {
        bail!("Difference in extent metadata found!");
    }

    Ok(())
}

// Print the ASCII color code of the given value
// Clear: 0, Green: 32, Red: 31, Blue: 34
// If we don't want to print any color, then set no_color to true when
// calling and we just return an empty string.
fn sgr(n: u8, no_color: bool) -> String {
    if no_color {
        String::new()
    } else {
        format!("\x1b[{}m", n)
    }
}

// Return a Vec of the display color selections for an input vec.
// The determination for color is based on the highest value found in
// the Vec.  If everything is the same, then all colors are green, if
// there is any inequality, then the highest value is green and all others
// are red.
// The resulting vec can be used by the sgr() function to print the proper
// color during display.
fn color_vec(compare: &[u64]) -> Vec<u8> {
    let len = compare.len();
    let mut colors;

    // Set everything to 32 (green) then switch any to 31 (red) that
    // are less.
    assert!(len <= 3);
    if len == 1 {
        return vec![32];
    } else if len == 2 {
        colors = vec![32, 32];
    } else {
        colors = vec![32, 32, 32];
    }

    if compare[0] < compare[1] {
        colors[0] = 31;
    }
    if compare[1] < compare[0] {
        colors[1] = 31;
    }

    if len == 3 {
        if compare[0] < compare[2] {
            colors[0] = 31;
        }
        if compare[1] < compare[2] {
            colors[1] = 31;
        }
        if compare[2] < compare[0] {
            colors[2] = 31;
        }
        if compare[2] < compare[1] {
            colors[2] = 31;
        }
    }
    colors
}

fn return_status_letters<'a, U: std::cmp::PartialEq>(
    items: &'a [RegionReadResponse],
    accessor: fn(&'a RegionReadResponse) -> U,
    nc: bool,
) -> ([String; 3], bool) {
    let mut status_letters = vec![String::new(); 3];
    let mut different = false;

    let count = items.len();

    if accessor(&items[0]) == accessor(&items[1]) {
        status_letters[0] = format!("{}A{}", sgr(32, nc), sgr(0, nc));
        status_letters[1] = format!("{}A{}", sgr(32, nc), sgr(0, nc));

        if count > 2 {
            if accessor(&items[0]) == accessor(&items[2]) {
                status_letters[2] = format!("{}A{}", sgr(32, nc), sgr(0, nc));
            } else {
                status_letters[2] = format!("{}C{}", sgr(31, nc), sgr(0, nc));
                different = true;
            }
        }
    } else {
        different = true;
        status_letters[0] = format!("{}A{}", sgr(32, nc), sgr(0, nc));
        status_letters[1] = format!("{}B{}", sgr(34, nc), sgr(0, nc));

        if count > 2 {
            if accessor(&items[0]) == accessor(&items[2]) {
                status_letters[2] = format!("{}A{}", sgr(32, nc), sgr(0, nc));
            } else if accessor(&items[1]) == accessor(&items[2]) {
                status_letters[2] = format!("{}B{}", sgr(34, nc), sgr(0, nc));
            } else {
                status_letters[2] = format!("{}C{}", sgr(31, nc), sgr(0, nc));
            }
        }
    }

    (status_letters.try_into().unwrap(), different)
}

/*
 * Show the metadata and a block by block diff of a single extent
 * We need at least two directories to compare, and no more than three.
 */
async fn show_extent(
    region_dir: Vec<PathBuf>,
    ei_hm: &HashMap<u32, ExtentMeta>,
    cmp_extent: u32,
    blocks_per_extent: u64,
    only_show_differences: bool,
    nc: bool,
    log: Logger,
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
            let dirty = if em.dirty {
                "D".to_string()
            } else {
                " ".to_string()
            };
            print!("{:>8} ", dirty);
        } else {
            print!("- ");
        }
    }
    println!();
    println!();
    // Width for BLOCKS column
    let max_block =
        blocks_per_extent * cmp_extent as u64 + blocks_per_extent - 1;
    // Get the max possible width for a single block
    let block_width = std::cmp::max(3, max_block.to_string().len());

    // Print the header
    print!("{:>0width$} ", "BLOCK", width = block_width);
    for (index, _) in region_dir.iter().enumerate() {
        print!(" {0:^2}", format!("D{}", index));
    }
    print!(" ");
    for (index, _) in region_dir.iter().enumerate() {
        print!(" {0:^2}", format!("C{}", index));
    }
    if !only_show_differences {
        print!(" {0:^5}", "DIFF");
    }
    println!();

    /*
     * Compare the data from each block.
     * Print a letter representing the data for each block.
     */
    let mut difference_found = false;
    for block in 0..blocks_per_extent {
        let mut data_columns: [String; 3] =
            ["".to_string(), "".to_string(), "".to_string()];
        let mut block_context_columns: [String; 3] =
            ["".to_string(), "".to_string(), "".to_string()];

        /*
         * Build a Vector to hold our responses, one for each
         * region we are comparing.
         */
        let mut dvec = Vec::with_capacity(dir_count);

        /*
         * Read the requested block in from the extent.  Store it
         * in the Vec based on index.
         */
        for (index, dir) in region_dir.iter().enumerate() {
            // Open Region read only
            let mut region =
                Region::open(dir, Default::default(), false, true, &log)
                    .await?;

            let response = region
                .region_read(
                    &RegionReadRequest(vec![RegionReadReq {
                        extent: cmp_extent as u64,
                        offset: Block::new_with_ddef(block, &region.def()),
                        count: NonZeroUsize::new(1).unwrap(),
                    }]),
                    JobId(0),
                )
                .await?;
            assert_eq!(response.blocks.len(), 1);
            dvec.insert(index, response);
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

        // first compare data
        let (status_letters, data_different) =
            return_status_letters(&dvec, |r| &r.data, nc);

        // Print the data status letters
        for dir_index in 0..dir_count {
            data_columns[dir_index] = status_letters[dir_index].to_string();
        }

        // then, compare block_context_columns
        let (status_letters, bc_different) =
            return_status_letters(&dvec, |r| &r.blocks[0], nc);

        // Print block context status letters
        for dir_index in 0..dir_count {
            block_context_columns[dir_index] =
                status_letters[dir_index].to_string();
        }

        let different = data_different || bc_different;

        // Now that we have collected all the results, print them
        let real_block = (blocks_per_extent * cmp_extent as u64) + block;
        if !only_show_differences || different {
            print!("{:0width$} ", real_block, width = block_width);

            for column in data_columns.iter().take(dir_count) {
                print!("  {}", column);
            }
            print!(" ");
            for column in block_context_columns.iter().take(dir_count) {
                print!("  {}", column);
            }

            if !only_show_differences {
                print!(" {0:^4}", if different { "<---" } else { "" });
            }

            println!();
        }

        difference_found |= different;
    }

    if difference_found {
        bail!("Difference found!");
    }

    Ok(())
}

fn is_all_same<T: PartialEq>(slice: &[T]) -> bool {
    slice.windows(2).all(|x| x[0] == x[1])
}

/*
 * Show detailed comparison of different region's blocks
 */
async fn show_extent_block(
    region_dir: Vec<PathBuf>,
    cmp_extent: u32,
    block: u64,
    blocks_per_extent: u64,
    only_show_differences: bool,
    nc: bool,
    log: Logger,
) -> Result<()> {
    let block_in_extent = block % blocks_per_extent;
    println!(
        "Extent {} Block in extent {}  Actual block {}",
        cmp_extent, block_in_extent, block,
    );
    println!();

    let dir_count = region_dir.len();

    /*
     * Build a Vector to hold our responses, one for each
     * region we are comparing.
     */
    let mut dvec = Vec::with_capacity(dir_count);

    /*
     * Read the requested block in from the extent.  Store it
     * in the Vec based on index.
     */
    for (index, dir) in region_dir.iter().enumerate() {
        // Open Region read only
        let mut region =
            Region::open(dir, Default::default(), false, true, &log).await?;

        let response = region
            .region_read(
                &RegionReadRequest(vec![RegionReadReq {
                    extent: cmp_extent as u64,
                    offset: Block::new_with_ddef(
                        block_in_extent,
                        &region.def(),
                    ),
                    count: NonZeroUsize::new(1).unwrap(),
                }]),
                JobId(0),
            )
            .await?;
        assert_eq!(response.blocks.len(), 1);
        dvec.insert(index, response);
    }

    /*
     * Compare data
     */
    let (status_letters, different) =
        return_status_letters(&dvec, |r| &r.data, nc);

    if !only_show_differences || different {
        println!("{:>6}  {:<64}  {:3}", "DATA", "SHA256", "VER");
        println!(
            "{}  {}  {}",
            String::from_utf8(vec![b'-'; 6])?,
            String::from_utf8(vec![b'-'; 64])?,
            String::from_utf8(vec![b'-'; 3])?,
        );
        for dir_index in 0..dir_count {
            let mut hasher = Sha256::new();
            hasher.update(&dvec[dir_index].data[..]);
            println!(
                "{:>6}  {:64}  {:^3}",
                dir_index,
                hex::encode(hasher.finalize()),
                status_letters[dir_index],
            );
        }
        println!();
    }

    /*
     * Compare block contexts
     */
    let (_, different) = return_status_letters(&dvec, |r| &r.blocks[0], nc);

    if !only_show_differences || different {
        /*
         * Compare nonces (formatting assumes 12 byte nonces)
         */
        print!("{:>6}  ", "NONCES");

        let mut max_nonce_depth = 0;

        for (dir_index, r) in dvec.iter().enumerate() {
            print!("{:^24} ", dir_index);

            max_nonce_depth =
                std::cmp::max(max_nonce_depth, r.encryption_contexts(0).len());
        }
        if !only_show_differences {
            print!(" {:<5}", "DIFF");
        }
        println!();

        print!("{}  ", String::from_utf8(vec![b'-'; 6])?);
        for _ in &dvec {
            print!("{} ", String::from_utf8(vec![b'-'; 24])?);
        }
        if !only_show_differences {
            print!("{} ", String::from_utf8(vec![b'-'; 5])?);
        }
        println!();

        for depth in 0..max_nonce_depth {
            print!("{:>6}  ", depth);

            let mut all_same_len = true;
            let mut nonces = Vec::with_capacity(dir_count);
            for r in dvec.iter() {
                let ctxs = r.encryption_contexts(0);
                print!(
                    "{:^24} ",
                    if depth < ctxs.len() {
                        if let Some(ec) = ctxs[depth] {
                            nonces.push(&ec.nonce);
                            hex::encode(ec.nonce)
                        } else {
                            all_same_len = false;
                            "".to_string()
                        }
                    } else {
                        all_same_len = false;
                        "".to_string()
                    }
                );
            }
            if !all_same_len || !is_all_same(&nonces) {
                print!(" {:<5}", "<---");
            }
            println!();
        }
        println!();

        /*
         * Compare tags (formatting assumes 16 byte tags)
         */
        print!("{:>6}  ", "TAGS");

        let mut max_tag_depth = 0;

        for (dir_index, r) in dvec.iter().enumerate() {
            print!("{:^32} ", dir_index);

            max_tag_depth =
                std::cmp::max(max_tag_depth, r.encryption_contexts(0).len());
        }
        if !only_show_differences {
            print!(" {:<5}", "DIFF");
        }
        println!();

        print!("{}  ", String::from_utf8(vec![b'-'; 6])?);
        for _ in &dvec {
            print!("{} ", String::from_utf8(vec![b'-'; 32])?);
        }
        if !only_show_differences {
            print!("{} ", String::from_utf8(vec![b'-'; 5])?);
        }
        println!();

        for depth in 0..max_tag_depth {
            print!("{:>6}  ", depth);

            let mut all_same_len = true;
            let mut tags = Vec::with_capacity(dir_count);
            for r in dvec.iter() {
                let ctxs = r.encryption_contexts(0);
                print!(
                    "{:^32} ",
                    if depth < ctxs.len() {
                        if let Some(ec) = ctxs[depth] {
                            tags.push(&ec.tag);
                            hex::encode(ec.tag)
                        } else {
                            all_same_len = false;
                            "".to_string()
                        }
                    } else {
                        all_same_len = false;
                        "".to_string()
                    }
                );
            }
            if !all_same_len || !is_all_same(&tags) {
                print!(" {:<5}", "<---");
            }
            println!();
        }
        println!();
    }

    /*
     * Compare integrity hashes
     */
    let (_, different) = return_status_letters(&dvec, |r| r.hashes(0), nc);

    if !only_show_differences || different {
        /*
         * note formatting assumes 8 byte integrity hashes
         */
        print!("{:>6}  ", "HASHES");

        let mut max_hash_depth = 0;

        for (dir_index, r) in dvec.iter().enumerate() {
            print!("{:^16} ", dir_index);

            max_hash_depth = std::cmp::max(max_hash_depth, r.hashes(0).len());
        }
        if !only_show_differences {
            print!(" {:<5}", "DIFF");
        }
        println!();

        print!("{}  ", String::from_utf8(vec![b'-'; 6])?);
        for _ in &dvec {
            print!("{} ", String::from_utf8(vec![b'-'; 16])?);
        }
        if !only_show_differences {
            print!("{} ", String::from_utf8(vec![b'-'; 5])?);
        }
        println!();

        for depth in 0..max_hash_depth {
            print!("{:>6}  ", depth);

            let mut all_same_len = true;
            let mut hashes = Vec::with_capacity(dir_count);
            for r in dvec.iter() {
                let block_hashes = r.hashes(0);
                print!(
                    "{:^16} ",
                    if depth < block_hashes.len() {
                        hashes.push(block_hashes[depth]);
                        hex::encode(block_hashes[depth].to_le_bytes())
                    } else {
                        all_same_len = false;
                        "".to_string()
                    }
                );
            }
            if !all_same_len || !is_all_same(&hashes) {
                print!(" {:<5}", "<---");
            }
            println!();
        }
        println!();
    }

    Ok(())
}
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn color_compare() {
        // All the same, all green
        let cm = vec![2, 2, 2];
        let colors = color_vec(&cm);
        assert_eq!(colors, vec![32, 32, 32]);
    }
    #[test]
    fn color_compare_one() {
        // All the same, all green, size 1
        let cm = vec![2];
        let colors = color_vec(&cm);
        assert_eq!(colors, vec![32]);
    }
    #[test]
    fn color_compare_two() {
        // All the same, all green size 2
        let cm = vec![2, 2];
        let colors = color_vec(&cm);
        assert_eq!(colors, vec![32, 32]);
    }
    #[test]
    fn color_compare_two_red0() {
        let cm = vec![2, 3];
        let colors = color_vec(&cm);
        assert_eq!(colors, vec![31, 32]);
    }
    #[test]
    fn color_compare_two_red1() {
        let cm = vec![4, 2];
        let colors = color_vec(&cm);
        assert_eq!(colors, vec![32, 31]);
    }
    #[test]
    fn color_compare_red0() {
        let cm = vec![1, 2, 2];
        let colors = color_vec(&cm);
        assert_eq!(colors, vec![31, 32, 32]);
    }
    #[test]
    fn color_compare_red1() {
        let cm = vec![4, 2, 4];
        let colors = color_vec(&cm);
        assert_eq!(colors, vec![32, 31, 32]);
    }
    #[test]
    fn color_compare_red2() {
        let cm = vec![8, 8, 2];
        let colors = color_vec(&cm);
        assert_eq!(colors, vec![32, 32, 31]);
    }
    #[test]
    fn color_compare_red02() {
        let cm = vec![1, 3, 2];
        let colors = color_vec(&cm);
        assert_eq!(colors, vec![31, 32, 31]);
    }
    #[test]
    fn color_compare_red02_2() {
        let cm = vec![2, 3, 1];
        let colors = color_vec(&cm);
        assert_eq!(colors, vec![31, 32, 31]);
    }
    #[test]
    fn color_compare_red01() {
        let cm = vec![3, 2, 4];
        let colors = color_vec(&cm);
        assert_eq!(colors, vec![31, 31, 32]);
    }
    #[test]
    fn color_compare_red01_2() {
        let cm = vec![2, 3, 4];
        let colors = color_vec(&cm);
        assert_eq!(colors, vec![31, 31, 32]);
    }
    #[test]
    fn color_compare_red12() {
        let cm = vec![5, 3, 4];
        let colors = color_vec(&cm);
        assert_eq!(colors, vec![32, 31, 31]);
    }
    #[test]
    fn color_compare_red12_2() {
        let cm = vec![5, 4, 3];
        let colors = color_vec(&cm);
        assert_eq!(colors, vec![32, 31, 31]);
    }
}
