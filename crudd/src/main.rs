// Copyright 2022 Oxide Computer Company

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::SocketAddr;
use std::os::unix::io::FromRawFd;
use std::sync::Arc;
use std::{cmp, io};

use anyhow::{bail, Result};
use clap::Parser;
use futures::stream::StreamExt;
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use tokio::sync::oneshot;

use crucible::*;

#[derive(Debug, Copy, Clone, clap::Subcommand)]
enum CruddAct {
    /// Read data from the region and write to file descriptor 3
    Read,

    /// Write data to the region and read from STDIN
    Write,
}

#[derive(Debug, Parser)]
#[clap(about = "dd for crudd")]
pub struct Opt {
    /// Target downstairses. Must provide IP:PORT, and you need at least 3 of
    /// them. Specify this option multiple times.
    #[clap(short, long, action)]
    target: Vec<SocketAddr>,

    /// Encryption key, base64-encoded
    #[clap(short, long, action)]
    key: Option<String>,

    /// Generation
    #[clap(short, long, default_value = "0", action)]
    gen: u64,

    /// TLS certificate
    #[clap(long, action)]
    cert_pem: Option<String>,

    /// TLS key
    #[clap(long, action)]
    key_pem: Option<String>,

    /// TLS root certificate
    #[clap(long, action)]
    root_cert_pem: Option<String>,

    /// Start upstairs control http server
    #[clap(long, action)]
    control: Option<SocketAddr>,

    // crudd-specific options
    /// Number of bytes to read or write. If omitted, will be bounded by the
    /// region size and input/output stream automatically
    #[clap(short, long, action)]
    num_bytes: Option<u64>,

    /// Byte offset within the region to start IO
    #[clap(short, long, default_value = "0", action)]
    byte_offset: u64,

    /// Number of blocks to access with each read/write command
    #[clap(short, long, default_value = "8192", action)]
    iocmd_block_count: u64,

    /// Max number of read/write requests to dispatch to Upstairs before
    /// blocking
    #[clap(short, long, default_value = "2", action)]
    pipeline_length: usize,

    /// Puts crudd into benchmarking mode. In benchmarking mode, crudd will
    /// exit early if it receives SIGUSR1. It will do its best to cleanly exit,
    /// but will not make any guarantees about the state of data it has sent
    /// downstairs. It will write out how many bytes were processed to the
    /// filepath provided as an argument to this function. It will
    /// use /dev/zero as source and /dev/null as a sink, instead of any
    /// user-provided file source or sink.
    #[clap(long, action)]
    benchmarking_mode: Option<String>,

    #[clap(subcommand)]
    subcommand: CruddAct,
}

pub fn opts() -> Result<Opt> {
    let opt: Opt = Opt::parse();
    eprintln!("raw options: {:?}", opt);

    if opt.target.is_empty() {
        bail!("must specify at least one --target");
    }

    Ok(opt)
}

async fn cmd_read<T: BlockIO>(
    opt: &Opt,
    crucible: Arc<T>,
    mut early_shutdown: oneshot::Receiver<()>,
) -> Result<usize> {
    let volume_size = crucible.total_size().await?;

    // Count of all bytes we've read from the downstairs
    let mut total_bytes_read = 0;

    // If num_bytes is None, we take that to mean "read to the end of the
    // region"
    let num_bytes = match opt.num_bytes {
        None => volume_size - opt.byte_offset,
        Some(x) => x,
    };

    // A quick check- if we're supposed to read 0 bytes we should just stop now
    if num_bytes == 0 {
        return Ok(0);
    }

    let native_block_size = crucible.get_block_size().await?;

    // Check that the read is fully within the region
    if opt.byte_offset + num_bytes > volume_size {
        bail!(
            "you're trying to read beyond the volume size of {}",
            volume_size
        );
    }

    let mut output: Box<dyn Write> = if opt.benchmarking_mode.is_some() {
        Box::new(io::sink())
    } else {
        // Right now we just use fd3 as a file. This relies on the user actually
        // providing an FD3 in their shell. We should do something better than
        // this later, but for now im doing this because crucible internals are
        // writing to stdout. TODO: maybe command line arg for output file
        Box::new(BufWriter::new(unsafe { File::from_raw_fd(3) }))
    };

    // let mut output = BufWriter::new(output_file);

    // ring buffers
    // The only reason we have a dynamic pipeline length is because I'm
    // interested in seeing how the pipeline length affects performance. I
    // don't think it'll make a difference beyond 2, (one reading from
    // crucible, one writing to output), but we'll see!
    //
    // TODO this is no longer true after the Upstairs is async, multiple
    // requests can be submitted and await'ed on at the same time.
    let mut buffers = VecDeque::with_capacity(opt.pipeline_length);
    let mut futures = VecDeque::with_capacity(opt.pipeline_length);

    // First, align our offset to the underlying blocks with an initial read

    // What's our offset into the block?
    let offset_misalignment = opt.byte_offset % native_block_size;

    // How many bytes will we be writing to the output from our alignment read?
    // This will normally be (native_block_size - misalignment), but if we do a
    // read which is smaller than a block size, with a misaligned offset, it
    // will be smaller. Later bits of the code will then see they have no
    // work to do and will do nothing.
    let alignment_bytes =
        cmp::min(num_bytes, native_block_size - offset_misalignment);
    if offset_misalignment != 0 {
        // Read the full block
        let buffer = Buffer::new(native_block_size as usize);
        let block_idx = opt.byte_offset / native_block_size;
        let offset = Block::new(block_idx, native_block_size.trailing_zeros());
        crucible.read(offset, buffer.clone()).await?;

        // write only (block size - misalignment) bytes
        // So say we have an offset of 5. we're misaligned by 5 bytes, so we
        // read 5 bytes we don't need. we skip those 5 bytes then write
        // the rest to the output
        let bytes = buffer.into_vec().unwrap();
        output.write_all(
            &bytes[offset_misalignment as usize
                ..(offset_misalignment + alignment_bytes) as usize],
        )?;
        total_bytes_read += alignment_bytes as usize;
    }

    // we need to account for bytes we just read for offset alignment
    let (num_bytes, block_offset) = if offset_misalignment == 0 {
        // The simple case with no misalignment
        (num_bytes, opt.byte_offset / native_block_size)
    } else {
        // Account for the bytes we already read, and add one to the block
        // offset
        (
            num_bytes - alignment_bytes,
            opt.byte_offset / native_block_size + 1,
        )
    };

    // How many full commands we can issue, and then what our remainder bytes
    // will be. Note we'll round up the remainder later to get the number of
    // blocks for the last iocmd
    let cmd_count = num_bytes / (opt.iocmd_block_count * native_block_size);
    let remainder = num_bytes % (opt.iocmd_block_count * native_block_size);

    // Issue all of our read commands
    for i in 0..cmd_count {
        // which blocks in the underlying store are we accessing?
        let block_idx = block_offset + (i * opt.iocmd_block_count);
        let offset = Block::new(block_idx, native_block_size.trailing_zeros());

        // Send the read command with whichever buffer is at the back of the
        // queue. We re-use the buffers to avoid lots of allocations
        let w_buf =
            Buffer::new((opt.iocmd_block_count * native_block_size) as usize);
        let w_future = crucible.read(offset, w_buf.clone());
        buffers.push_back(w_buf);
        futures.push_back(w_future);
        total_bytes_read +=
            (opt.iocmd_block_count * native_block_size) as usize;

        // If we have a queue of futures, drain the oldest one to output.
        if futures.len() == opt.pipeline_length {
            futures.pop_front().unwrap().await?;
            let r_buf = buffers.pop_front().unwrap();
            output.write_all(&r_buf.as_vec().await)?;
        }

        if early_shutdown.try_recv().is_ok() {
            eprintln!("shutting down early in response to SIGUSR1");
            join_all(futures).await?;
            return Ok(total_bytes_read);
        }
    }

    // Drain the outstanding commands
    if !futures.is_empty() {
        crucible::join_all(futures).await?;

        // drain the buffer to the output file
        while !buffers.is_empty() {
            // unwrapping is safe because of the length check
            let r_buf = buffers.pop_front().unwrap();
            output.write_all(&r_buf.as_vec().await)?;
        }
    }

    // Issue our final read command, if any. This could be interleaved with
    // draining the outstanding commands but it's more complicated and
    // unlikely to make a significant impact on timings.
    if remainder > 0 {
        // let block_remainder = remainder % native_block_size;
        // round up
        let blocks = (remainder + native_block_size - 1) / native_block_size;
        let buffer = Buffer::new((blocks * native_block_size) as usize);
        let block_idx = (cmd_count * opt.iocmd_block_count) + block_offset;
        let offset = Block::new(block_idx, native_block_size.trailing_zeros());
        crucible.read(offset, buffer.clone()).await?;
        total_bytes_read += remainder as usize;
        output.write_all(&buffer.as_vec().await[0..remainder as usize])?;
    }

    Ok(total_bytes_read)
}

// We need to run this at the end to handle the remainder bytes, but we also
// need to do the exact same thing if the input stream ends early. So
// let's not copy-paste
// - write all block-aligned data remaining
// - read/mod/write the last block if necessary
// - issue a flush
// - block on all futures
async fn write_remainder_and_finalize<'a, T: BlockIO>(
    crucible: &Arc<T>,
    mut w_buf: BytesMut,
    offset: Block,
    n_read: usize,
    native_block_size: u64,
    mut futures: VecDeque<crucible::CrucibleBlockIOFuture<'a>>,
) -> Result<()> {
    // the input stream ended,
    // - read/mod/write for alignment
    // - block on all futures

    // uflow short for underflow, as we're underflowing are normal iocmd
    // block
    let uflow_blocks = n_read as u64 / native_block_size;
    let uflow_remainder = n_read as u64 % native_block_size;

    if uflow_remainder == 0 {
        // no need to RMW, just write
        w_buf.resize(n_read, 0);
        let w_future = crucible.write(offset, w_buf.freeze());
        futures.push_back(w_future);
    } else {
        // RMW oof

        // Adjust buffer to be block-aligned to our data
        w_buf.resize(((uflow_blocks + 1) * native_block_size) as usize, 0);

        // How many bytes we need to copy into the end of w_buf
        let uflow_backfill = (native_block_size - uflow_remainder) as usize;

        // First, read the final partial-block
        let uflow_offset = Block::new(
            offset.value + uflow_blocks,
            native_block_size.trailing_zeros(),
        );
        let uflow_r_buf = Buffer::new(native_block_size as usize);
        crucible.read(uflow_offset, uflow_r_buf.clone()).await?;

        // Copy it into w_buf
        let r_bytes = uflow_r_buf.into_vec().unwrap();
        w_buf[n_read..n_read + uflow_backfill]
            .copy_from_slice(&r_bytes[uflow_remainder as usize..]);

        // Issue the write
        let w_future = crucible.write(offset, w_buf.freeze());
        futures.push_back(w_future);
    }

    // Flush
    let flush_future = crucible.flush(None);
    futures.push_back(flush_future);

    // Wait for all the writes
    join_all(futures).await?;

    Ok(())
}

/// Returns the number of bytes written
async fn cmd_write<T: BlockIO>(
    opt: &Opt,
    crucible: Arc<T>,
    mut early_shutdown: oneshot::Receiver<()>,
) -> Result<usize> {
    let mut total_bytes_written = 0;

    // A lot of this is going to be repeat from cmd_read, but it is
    // subtly different to handle things like read-modify-write for partial
    // blocks.
    let volume_size = crucible.total_size().await?;

    // If num_bytes is None, we take that to mean "write to the end of the
    // region" Of course, if the input stream ends first, we'll stop early.
    let num_bytes = match opt.num_bytes {
        None => volume_size - opt.byte_offset,
        Some(x) => x,
    };

    // A quick check- if we're supposed to write 0 bytes we should just stop now
    if num_bytes == 0 {
        return Ok(0);
    }

    let native_block_size = crucible.get_block_size().await?;

    // Check that the write is fully within the region
    if opt.byte_offset + num_bytes > volume_size {
        bail!(
            "you're trying to write beyond the volume size of {}",
            volume_size
        );
    }

    let mut input: Box<dyn Read> = if opt.benchmarking_mode.is_some() {
        // Write a value that isn't zero, because some things special-case all
        // zero data in this world. Value chosen by a fair dice roll ;)
        Box::new(io::repeat(17u8))
    } else {
        Box::new(BufReader::new(io::stdin()))
    };

    // ring buffers
    let mut futures = VecDeque::with_capacity(opt.pipeline_length);

    // First, align our offset to the underlying blocks with an initial read

    // What's our offset into the block?
    let offset_misalignment = opt.byte_offset % native_block_size;

    // How many bytes will we be writing to the output from our alignment read?
    // This will normally be (native_block_size - misalignment), but if we do a
    // write which is smaller than a block size, with a misaligned offset,
    // it will be smaller. Later bits of the code will then see they have no
    // work to do and will do nothing.
    let alignment_bytes =
        cmp::min(num_bytes, native_block_size - offset_misalignment);
    if offset_misalignment != 0 {
        // We need to read-modify-write here.

        // Read the full block
        let buffer = Buffer::new(native_block_size as usize);
        let block_idx = opt.byte_offset / native_block_size;
        let offset = Block::new(block_idx, native_block_size.trailing_zeros());
        crucible.read(offset, buffer.clone()).await?;

        let mut w_vec = buffer.into_vec().unwrap();
        // Write our data into the buffer
        let bytes_read = input.read(
            &mut w_vec[offset_misalignment as usize
                ..(offset_misalignment + alignment_bytes) as usize],
        )?;
        total_bytes_written += bytes_read;

        let w_bytes = Bytes::from(w_vec);

        crucible.write(offset, w_bytes).await?;

        if bytes_read != alignment_bytes as usize {
            // underrun, exit early
            return Ok(total_bytes_written);
        }
    }

    // we need to account for bytes we just read for offset alignment
    let (num_bytes, block_offset) = if offset_misalignment == 0 {
        // The simple case with no misalignment
        (num_bytes, opt.byte_offset / native_block_size)
    } else {
        // Account for the bytes we already read, and add one to the block
        // offset
        (
            num_bytes - alignment_bytes,
            opt.byte_offset / native_block_size + 1,
        )
    };

    // How many full commands we can issue, and then what our remainder bytes
    // will be. Note we'll round up the remainder later to get the number of
    // blocks for the last iocmd
    let cmd_count = num_bytes / (opt.iocmd_block_count * native_block_size);
    let remainder = num_bytes % (opt.iocmd_block_count * native_block_size);

    // Issue all of our write commands
    for i in 0..cmd_count {
        // which blocks in the underlying store are we accessing?
        let block_idx = block_offset + (i * opt.iocmd_block_count);
        let offset = Block::new(block_idx, native_block_size.trailing_zeros());

        // I don't think we can re-use Bytes so I'm just living with it.
        let mut w_buf = BytesMut::with_capacity(
            (opt.iocmd_block_count * native_block_size) as usize,
        );
        w_buf.resize(w_buf.capacity(), 0);

        let mut n_read = 0;
        while n_read < w_buf.capacity() {
            match input.read(&mut w_buf[n_read..])? {
                0 => break, // early EOF
                n => {
                    n_read += n;
                }
            }
        }
        total_bytes_written += n_read;

        if n_read < w_buf.capacity() {
            eprintln!("n_read was {}, returning early", n_read);
            write_remainder_and_finalize(
                &crucible,
                w_buf,
                offset,
                n_read,
                native_block_size,
                futures,
            )
            .await?;
            return Ok(total_bytes_written);
        } else {
            // good to go for a write
            let w_future = crucible.write(offset, w_buf.freeze());
            futures.push_back(w_future);
        }

        // Block on oldest future so we dont get a backlog
        if futures.len() == opt.pipeline_length {
            futures.pop_front().unwrap().await?;
        }

        if early_shutdown.try_recv().is_ok() {
            eprintln!("shutting down early in response to SIGUSR1");
            join_all(futures).await?;
            crucible.flush(None).await?;
            return Ok(total_bytes_written);
        }
    }

    // Finalize. RMW if needed for alignment
    if remainder > 0 {
        eprintln!("remainder is {}, finalizing", remainder);
        let block_idx = block_offset + (cmd_count * opt.iocmd_block_count);
        let offset = Block::new(block_idx, native_block_size.trailing_zeros());
        let mut w_buf = BytesMut::with_capacity(
            (opt.iocmd_block_count * native_block_size) as usize,
        );
        w_buf.resize(remainder as usize, 0);

        // Here we dont care if n_read is less than remainder, just go with it.
        let mut n_read = 0;
        while n_read < w_buf.capacity() {
            match input.read(&mut w_buf[n_read..])? {
                0 => break, // early EOF
                n => {
                    n_read += n;
                }
            }
        }
        total_bytes_written += n_read;

        write_remainder_and_finalize(
            &crucible,
            w_buf,
            offset,
            n_read,
            native_block_size,
            futures,
        )
        .await?;
    } else {
        join_all(futures).await?;
        crucible.flush(None).await?;
    }

    Ok(total_bytes_written)
}

/// Signal handler for to stop early and print read/write statistics
/// This is intended for benchmarking
async fn handle_signals(
    mut signals: Signals,
    early_shutdown: oneshot::Sender<()>,
) {
    while let Some(signal) = signals.next().await {
        match signal {
            SIGUSR1 => {
                early_shutdown.send(()).unwrap();
                break;
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = opts()?;
    let crucible_opts = CrucibleOpts {
        target: opt.target.clone(),
        lossy: false,
        flush_timeout: None,
        key: opt.key.clone(),
        cert_pem: opt.cert_pem.clone(),
        key_pem: opt.key_pem.clone(),
        root_cert_pem: opt.root_cert_pem.clone(),
        control: opt.control,
        ..Default::default()
    };

    // TODO: volumes?
    let guest = Arc::new(Guest::new());

    let _join_handle =
        up_main(crucible_opts, opt.gen, None, guest.clone(), None, None)
            .await?;
    eprintln!("Crucible runtime is spawned");

    // IO time
    guest.activate().await?;

    let (early_shutdown_sender, early_shutdown_receiver) = oneshot::channel();

    // Only start the SIGUSR1 handler if we're in benchmarking mode
    if opt.benchmarking_mode.is_some() {
        let signals = Signals::new([SIGUSR1])?;
        tokio::spawn(handle_signals(signals, early_shutdown_sender));
    }

    let act_result = match opt.subcommand {
        CruddAct::Write => {
            cmd_write(&opt, guest.clone(), early_shutdown_receiver).await
        }
        CruddAct::Read => {
            cmd_read(&opt, guest.clone(), early_shutdown_receiver).await
        }
    };
    match act_result {
        Ok(total_bytes_processed) => {
            // Write the number of bytes processed so whatever is using us to
            // benchmark can do something useful with it.
            if let Some(filename) = opt.benchmarking_mode {
                let mut file = File::create(filename)?;
                write!(file, "{}", total_bytes_processed)?;
            }
        }
        Err(e) => {
            eprintln!("Encountered error while performing IO: {}. gracefully cleaning up.", e);
        }
    };

    guest.deactivate().await?;
    Ok(())
}
