use anyhow::{Result, anyhow, bail};
use clap::Parser;
use crucible_common::integrity_hash;
use crucible_protocol::BlockContext;
use serde::{Deserialize, Serialize};

/// dsc  DownStairs Controller
#[derive(Debug, Parser)]
#[clap(name = "crucible-verify-raw", term_width = 80)]
#[clap(about = "Verifier for raw extent files", long_about = None)]
struct Args {
    /// Raw extent file to check
    file: std::path::PathBuf,

    /// Block size in the extent (usually autodetected)
    #[clap(long)]
    block_size: Option<usize>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    check_one(&args.file, args.block_size)
}

fn check_one(p: &std::path::Path, block_size: Option<usize>) -> Result<()> {
    let data = std::fs::read(p)?;
    let data_len = data.len() - BLOCK_META_SIZE_BYTES;
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
    print!("bs:{block_size} bytes  bc:{block_count:>6}");
    println!(
        "  dirty:{:>5} gen:{} flush_number:{:>6} ext_ver:{}",
        meta.dirty, meta.gen_number, meta.flush_number, meta.ext_version
    );

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
            let offset =
                block_size * block_count + i * BLOCK_CONTEXT_SLOT_SIZE_BYTES;
            let chunk = &data[offset..][..BLOCK_CONTEXT_SLOT_SIZE_BYTES];
            bincode::deserialize(chunk).unwrap()
        })
        .collect::<Vec<Option<OnDiskDownstairsBlockContext>>>();
    let (ctx_a, ctx_b) = context_slots.split_at(block_count);

    let mut failed = false;
    for (i, chunk) in data[..block_size * block_count]
        .chunks_exact(block_size)
        .enumerate()
    {
        let hash = integrity_hash(&[chunk]);
        if let Some(slot_selected) = &slot_selected {
            // If the slot selected array is valid (i.e. the extent file is not
            // dirty), then it must be correct.
            let s = slot_selected[i];
            let ctx = if s { ctx_a[i] } else { ctx_b[i] };
            let r = check_block(chunk, hash, ctx);
            if r.is_err() {
                failed = true;
                print!("Error at block {:>6}:", i);
                print!(
                    "  slot {} [selected]: {r:?}",
                    if s { "A" } else { "B" }
                );
                print!(
                    " | slot {} [deselected]: {:?}",
                    if s { "B" } else { "A" },
                    check_block(
                        chunk,
                        hash,
                        if s { ctx_b[i] } else { ctx_a[i] }
                    )
                );
                if chunk.iter().all(|b| *b == 0u8) {
                    print!("  Block is all zeros");
                }
                println!();
            }
        } else if let Err(ea) = check_block(chunk, hash, ctx_a[i])
            && let Err(eb) = check_block(chunk, hash, ctx_b[i])
        {
            // Otherwise, check both context slots to see if either is valid
            failed = true;
            print!("Error at block {i}:");
            print!("  slot A: {ea:?}");
            print!(" | slot B: {eb:?}");
            if chunk.iter().all(|b| *b == 0u8) {
                print!("  Block is all zeros");
            }
            println!();
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

/// Brute force strategy to get block count
fn get_block_count(data_len: usize, block_size: usize) -> Option<usize> {
    let estimated_block_count =
        data_len / (block_size + BLOCK_CONTEXT_SLOT_SIZE_BYTES * 2);
    for i in 0..estimated_block_count {
        let block_count = estimated_block_count - i;
        let actual = block_count
            * (block_size + BLOCK_CONTEXT_SLOT_SIZE_BYTES * 2)
            + block_count.div_ceil(8);
        if actual == data_len {
            return Some(block_count);
        } else if actual < data_len {
            return None;
        }
    }
    None
}

////////////////////////////////////////////////////////////////////////////////
// Types copied from `extent_inner_raw.rs`
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
struct OnDiskDownstairsBlockContext {
    block_context: BlockContext,
    on_disk_hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OnDiskMeta {
    pub dirty: bool,
    pub gen_number: u64,
    pub flush_number: u64,
    pub ext_version: u32,
}

const BLOCK_CONTEXT_SLOT_SIZE_BYTES: usize = 48;
const BLOCK_META_SIZE_BYTES: usize = 32;
