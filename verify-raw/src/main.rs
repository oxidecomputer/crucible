use anyhow::{Result, anyhow, bail};
use clap::Parser;
use crucible_common::integrity_hash;

use crucible_raw_extent::{
    BLOCK_CONTEXT_SLOT_SIZE_BYTES, BLOCK_META_SIZE_BYTES,
    OnDiskDownstairsBlockContext, OnDiskMeta,
};

#[derive(Debug, Parser)]
#[clap(name = "crucible-verify-raw", term_width = 80)]
#[clap(about = "Verifier for raw extent files", long_about = None)]
struct Args {
    /// Raw extent file to check
    file: std::path::PathBuf,

    #[clap(long, short)]
    verbose: bool,

    /// Output CSV to stdout with one row per block; metadata is
    /// written as #-prefixed comment lines
    #[clap(long, conflicts_with = "verbose")]
    verbose_csv: bool,

    /// Block size in the extent (usually autodetected)
    #[clap(long)]
    block_size: Option<usize>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    check_one(&args.file, args.block_size, args.verbose, args.verbose_csv)
}

fn check_one(
    p: &std::path::Path,
    block_size: Option<usize>,
    verbose: bool,
    verbose_csv: bool,
) -> Result<()> {
    let data = std::fs::read(p)?;
    let data_len = data.len() - BLOCK_META_SIZE_BYTES as usize;
    let meta: OnDiskMeta = bincode::deserialize(&data[data_len..])?;
    if meta.ext_version != 2 {
        bail!("expected extent version 2, got {}", meta.ext_version);
    }

    let (block_size, block_count) = block_size
        .map(|bs| {
            get_block_count(data_len, bs)
                .map(|bc| (bs, bc))
                .ok_or_else(|| anyhow!("could not find block count"))
        })
        .unwrap_or_else(|| {
            // Check a range of block sizes to find which one has a valid block
            // count.  This should usually work!
            use crucible_common::{MAX_SHIFT, MIN_SHIFT};
            let mut valid = vec![];
            for shift in MIN_SHIFT..=MAX_SHIFT {
                let block_size = 1 << shift;
                if let Some(bc) = get_block_count(data_len, block_size) {
                    valid.push((block_size, bc));
                }
            }
            if valid.len() == 1 {
                Ok(valid[0])
            } else {
                bail!(
                    "Found multiple valid block size / count combinations: \
                     {valid:?}; specify --block-size to disambiguate"
                );
            }
        })?;

    if verbose_csv {
        println!(
            "# file:{} bs:{block_size} bytes  bc:{block_count}  \
             dirty:{} gen:{} flush_number:{} ext_ver:{} \
             bonus_sync:{} defrag:{}",
            p.display(),
            meta.dirty,
            meta.gen_number,
            meta.flush_number,
            meta.ext_version,
            meta.bonus_sync_count,
            meta.defrag_count,
        );
        println!(
            "file,block,status,\
             slot_a_result,slot_a_selected,slot_a_flush_id,slot_a_hash,\
             slot_b_result,slot_b_selected,slot_b_flush_id,slot_b_hash,\
             data_hash,all_zeros"
        );
    } else {
        print!("bs:{block_size} bytes  bc:{block_count:>6}");
        println!(
            "  dirty:{:>5} gen:{} flush_number:{:>6} ext_ver:{} \
             bonus_sync:{} defrag:{}",
            meta.dirty,
            meta.gen_number,
            meta.flush_number,
            meta.ext_version,
            meta.bonus_sync_count,
            meta.defrag_count,
        );
    }

    let slot_selected = if !meta.dirty {
        let mut selected = vec![];
        for d in &data[data_len - block_count.div_ceil(8)..data_len] {
            for i in 0..8 {
                selected.push((d & (1 << i)) == 0);
            }
        }
        Some(selected)
    } else {
        None
    };

    let context_slots = (0..block_count * 2)
        .map(|i| {
            let offset = block_size * block_count
                + i * BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize;
            let chunk =
                &data[offset..][..BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize];
            bincode::deserialize(chunk).unwrap()
        })
        .collect::<Vec<Option<OnDiskDownstairsBlockContext>>>();
    let (ctx_a, ctx_b) = context_slots.split_at(block_count);

    let filename = csv_quote(&p.display().to_string());
    let mut failed = false;
    // Check each block and emit one output line per block
    for (i, chunk) in data[..block_size * block_count]
        .chunks_exact(block_size)
        .enumerate()
    {
        let hash = integrity_hash(&[chunk]);
        let ra = check_block(chunk, hash, ctx_a[i]);
        let rb = check_block(chunk, hash, ctx_b[i]);
        let all_zeros = chunk.iter().all(|b| *b == 0u8);

        if verbose_csv {
            let sel_a = slot_selected.as_ref().map(|s| s[i]);
            let sel_b = slot_selected.as_ref().map(|s| !s[i]);
            let status = match (&slot_selected, ra, rb) {
                (Some(s), ra, _) if s[i] && ra.is_err() => "error",
                (Some(s), _, rb) if !s[i] && rb.is_err() => "error",
                (None, Err(_), Err(_)) => "error",
                _ => "ok",
            };
            if status == "error" {
                failed = true;
            }
            println!(
                "{filename},{i},{status},{},{},{},{},{},{},{},{},{},{all_zeros}",
                ra.map_or_else(|e| format!("{e:?}"), |s| format!("{s:?}")),
                sel_a.map_or("unknown".to_owned(), |s| s.to_string()),
                ctx_a[i].map_or(String::new(), |c| c.flush_id.to_string()),
                ctx_a[i].map_or(String::new(), |c| c.on_disk_hash.to_string()),
                rb.map_or_else(|e| format!("{e:?}"), |s| format!("{s:?}")),
                sel_b.map_or("unknown".to_owned(), |s| s.to_string()),
                ctx_b[i].map_or(String::new(), |c| c.flush_id.to_string()),
                ctx_b[i].map_or(String::new(), |c| c.on_disk_hash.to_string()),
                hash,
            );
        } else {
            let mut printed = false;

            if let Some(slot_selected) = &slot_selected {
                // If the slot selected array is valid (i.e. the extent file
                // is not dirty), then it must be correct.
                let s = slot_selected[i];
                let ctx = if s { ctx_a[i] } else { ctx_b[i] };
                let r = if s { ra } else { rb };
                if r.is_err() {
                    failed = true;
                    printed = true;
                    print!("Error at block {:>6}:", i);
                    print!(
                        "  slot {} [selected]: {r:?}{}",
                        if s { "A" } else { "B" },
                        if let Some(ctx) = ctx {
                            format!(", flush id: {}", ctx.flush_id)
                        } else {
                            "".to_owned()
                        }
                    );
                    let other_ctx = if s { ctx_b[i] } else { ctx_a[i] };
                    let other_r = if s { rb } else { ra };
                    print!(
                        " | slot {} [deselected]: {:?}{}",
                        if s { "B" } else { "A" },
                        other_r,
                        if let Some(ctx) = other_ctx {
                            format!(", flush id: {}", ctx.flush_id)
                        } else {
                            "".to_owned()
                        }
                    );
                    if all_zeros {
                        print!("  Block is all zeros");
                    }
                    println!();
                }
            } else if let Err(ea) = ra
                && let Err(eb) = rb
            {
                // Otherwise, both context slots are invalid, so print that
                failed = true;
                printed = true;
                print!("Error at block {i}:");
                print!(
                    "  slot A: {ea:?}{}",
                    if let Some(ctx) = ctx_a[i] {
                        format!(", flush id: {}", ctx.flush_id)
                    } else {
                        "".to_owned()
                    }
                );
                print!(
                    " | slot B: {eb:?}{}",
                    if let Some(ctx) = ctx_b[i] {
                        format!(", flush id: {}", ctx.flush_id)
                    } else {
                        "".to_owned()
                    }
                );
                if all_zeros {
                    print!("  Block is all zeros");
                }
                println!();
            }

            // Print a log line for each block if the verbose flag is set
            if verbose && !printed {
                print!("Success at block {i}:");
                print!(
                    "  slot A{}: {ra:?}{}",
                    if let Some(slot_selected) = &slot_selected {
                        if slot_selected[i] {
                            " [selected]"
                        } else {
                            " [deselected]"
                        }
                    } else {
                        ""
                    },
                    if let Some(ctx) = ctx_a[i] {
                        format!(", flush id: {}", ctx.flush_id)
                    } else {
                        "".to_owned()
                    }
                );
                print!(
                    " | slot B{}: {rb:?}{}",
                    if let Some(slot_selected) = &slot_selected {
                        if !slot_selected[i] {
                            " [selected]"
                        } else {
                            " [deselected]"
                        }
                    } else {
                        ""
                    },
                    if let Some(ctx) = ctx_b[i] {
                        format!(", flush id: {}", ctx.flush_id)
                    } else {
                        "".to_owned()
                    }
                );
                if all_zeros {
                    print!("  Block is all zeros");
                }
                println!();
            }
        }
    }

    if failed {
        bail!("verification failed, see logs for details");
    }
    Ok(())
}

fn check_block(
    block: &[u8],
    hash: u64,
    ctx: Option<OnDiskDownstairsBlockContext>,
) -> Result<Success, Failure> {
    if let Some(ctx) = ctx {
        if ctx.on_disk_hash == hash {
            Ok(Success::HashMatch)
        } else {
            Err(Failure::SlotHashMismatch)
        }
    } else if block.iter().all(|v| *v == 0u8) {
        Ok(Success::BlockEmpty)
    } else {
        Err(Failure::EmptySlotWithNonzeroData)
    }
}

#[derive(Copy, Clone, Debug)]
enum Success {
    HashMatch,
    BlockEmpty,
}

#[derive(Copy, Clone, Debug)]
enum Failure {
    SlotHashMismatch,
    EmptySlotWithNonzeroData,
}

/// Wrap a string in double quotes, escaping internal double quotes for CSV
fn csv_quote(s: &str) -> String {
    if s.contains([',', '"', '\n']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_owned()
    }
}

/// Brute force strategy to get block count
fn get_block_count(data_len: usize, block_size: usize) -> Option<usize> {
    let estimated_block_count =
        data_len / (block_size + BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize * 2);
    for i in 0..estimated_block_count {
        let block_count = estimated_block_count - i;
        let actual = block_count
            * (block_size + BLOCK_CONTEXT_SLOT_SIZE_BYTES as usize * 2)
            + block_count.div_ceil(8);
        if actual == data_len {
            return Some(block_count);
        } else if actual < data_len {
            return None;
        }
    }
    None
}
