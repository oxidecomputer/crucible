// Copyright 2021 Oxide Computer Company
use std::net::SocketAddr;

use anyhow::{Result, bail};
use clap::Parser;

use crucible::*;

use nbd::server::{Export, handshake, transmission};
use std::net::{TcpListener, TcpStream as NetTcpStream};

/*
 * NBD server commands translate through the CruciblePseudoFile and turn
 * into Guest work ops.
 */

fn handle_nbd_client<T: crucible::BlockIO>(
    cpf: &mut crucible::CruciblePseudoFile<T>,
    mut stream: NetTcpStream,
) -> Result<()> {
    let e: Export<()> = Export {
        size: cpf.sz(),
        readonly: false,
        ..Default::default()
    };
    handshake(&mut stream, |_name| Ok(e))?;
    transmission(&mut stream, cpf)?;
    Ok(())
}

#[derive(Debug, Parser)]
#[clap(about = "volume-side storage component")]
pub struct Opt {
    #[clap(short, long, default_value = "127.0.0.1:9000", action)]
    target: Vec<SocketAddr>,

    #[clap(short, long, action)]
    key: Option<String>,

    #[clap(short, long = "gen", default_value = "0", action)]
    generation: u64,

    // TLS options
    #[clap(long, action)]
    cert_pem: Option<String>,
    #[clap(long, action)]
    key_pem: Option<String>,
    #[clap(long, action)]
    root_cert_pem: Option<String>,

    // Start upstairs control http server
    #[clap(long, action)]
    control: Option<SocketAddr>,

    /// Hex iroh secret key. When set, dial Downstairs over the seed mesh
    /// (`seed/crucible/v1`) instead of TCP.
    #[clap(long, env = "SEED_IROH_SECRET")]
    seed_iroh_secret: Option<String>,

    /// iroh target(s) `<node_id>@<addr>`, positionally parallel to `--target`.
    #[clap(long, action)]
    iroh_target: Vec<String>,

    /// Serve the activated volume as a ublk userspace block device
    /// (`/dev/ublkbN`) instead of NBD over TCP. Requires the `ublk_drv`
    /// kernel module and `/dev/ublk-control` (Linux >= 6.0).
    #[clap(long, action)]
    ublk: bool,
}

pub fn opts() -> Result<Opt> {
    let opt: Opt = Opt::parse();
    println!("raw options: {:?}", opt);

    if opt.target.is_empty() {
        bail!("must specify at least one --target");
    }

    Ok(opt)
}

/*
 * Crucible needs a runtime as it will create several async tasks to handle
 * adding new IOs, communication with the three downstairs instances, and
 * completing IOs.
 */
#[tokio::main]
async fn main() -> Result<()> {
    let opt = opts()?;
    let serve_ublk = opt.ublk;
    let crucible_opts = CrucibleOpts {
        target: opt.target,
        lossy: false,
        flush_timeout: None,
        key: opt.key,
        cert_pem: opt.cert_pem,
        key_pem: opt.key_pem,
        root_cert_pem: opt.root_cert_pem,
        control: opt.control,
        ..Default::default()
    };

    // Seed mesh: build the iroh endpoint + target map so the Upstairs dials
    // Downstairs over `seed/crucible/v1` instead of TCP.
    if let Some(hex) = &opt.seed_iroh_secret {
        let secret = crucible_common::seed_iroh::secret_from_hex(hex)?;
        let endpoint =
            crucible_common::seed_iroh::build_endpoint(secret).await?;
        let mut targets = Vec::new();
        for (sock, iroh) in
            crucible_opts.target.iter().zip(opt.iroh_target.iter())
        {
            targets.push((
                *sock,
                crucible_common::seed_iroh::parse_target(iroh)?,
            ));
        }
        crucible_common::seed_iroh::init(endpoint, targets);
    }

    /*
     * The structure we use to send work from outside crucible into the
     * Upstairs main task.
     * We create this here instead of inside up_main() so we can use
     * the methods provided by guest to interact with Crucible.
     */
    let (guest, io) = Guest::new(None);

    let _join_handle = up_main(crucible_opts, opt.generation, None, io, None)?;
    println!("Crucible runtime is spawned");

    if serve_ublk {
        return ublk::serve(guest).await;
    }

    // NBD server

    let mut cpf = crucible::CruciblePseudoFile::from(guest)?;
    cpf.activate().await?;

    let listener = TcpListener::bind("127.0.0.1:10809").unwrap();

    // sent to NBD client during handshake through Export struct
    println!("NBD advertised size as {} bytes", cpf.sz());

    for stream in listener.incoming() {
        println!("waiting on nbd traffic");
        match stream {
            Ok(stream) => match handle_nbd_client(&mut cpf, stream) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("handle_nbd_client error: {}", e);
                }
            },
            Err(_) => {
                println!("Error");
            }
        }
    }

    Ok(())
}

/*
 * ublk userspace block device serving mode.
 *
 * Bridges libublk's per-tag queue handler (which runs on its own thread, off
 * the tokio runtime) to the crucible Guest's async BlockIO trait. The Guest is
 * Sync, so it is shared into the queue closure behind an Arc; each READ / WRITE
 * / FLUSH is driven to completion via a captured tokio runtime Handle's
 * block_on. The whole libublk loop is run on a dedicated blocking thread so
 * block_on is never called from a runtime worker.
 */
mod ublk {
    use std::sync::Arc;

    use anyhow::{Context, Result, bail};
    use bytes::BytesMut;
    use crucible::{BlockIO, Buffer, Guest};
    use crucible_common::BlockIndex;
    use libublk::io::{UblkDev, UblkIOCtx, UblkQueue};
    use libublk::{
        UblkError, UblkFlags, UblkIORes, ctrl::UblkCtrl,
        ctrl::UblkCtrlBuilder,
    };
    use tokio::runtime::Handle;

    /// Activate the volume, then serve it as `/dev/ublkbN`, blocking until the
    /// device is torn down.
    pub async fn serve(guest: Guest) -> Result<()> {
        guest
            .activate()
            .await
            .map_err(|e| anyhow::anyhow!("activate: {e}"))?;

        let total_size = guest
            .total_size()
            .await
            .map_err(|e| anyhow::anyhow!("total_size: {e}"))?;
        let block_size = guest
            .get_block_size()
            .await
            .map_err(|e| anyhow::anyhow!("get_block_size: {e}"))?;
        if block_size == 0 {
            bail!("crucible reported a zero block size");
        }

        let guest = Arc::new(guest);
        let handle = Handle::current();

        // run_target blocks and drives an io_uring loop whose handler calls
        // block_on on the captured handle, so it must execute off the tokio
        // worker pool.
        tokio::task::spawn_blocking(move || {
            run_target(guest, handle, total_size, block_size)
        })
        .await
        .context("ublk serving thread panicked")?
    }

    fn run_target(
        guest: Arc<Guest>,
        handle: Handle,
        total_size: u64,
        block_size: u64,
    ) -> Result<()> {
        let ctrl = UblkCtrlBuilder::default()
            .name("crucible")
            .nr_queues(1)
            .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
            .build()
            .map_err(|e| anyhow::anyhow!("UblkCtrl build: {e:?}"))?;

        let tgt_init = move |dev: &mut UblkDev| -> Result<(), UblkError> {
            dev.set_default_params(total_size);
            Ok(())
        };

        // q_fn must be Fn + Send + Sync + Clone + 'static; the shared crucible
        // state satisfies that (Arc<Guest> is Send+Sync, Handle is Send+Sync).
        let q_fn = move |qid: u16, dev: &UblkDev| {
            let guest = guest.clone();
            let handle = handle.clone();
            let bufs = dev.alloc_queue_io_bufs();

            let io_handler =
                move |q: &UblkQueue, tag: u16, _io: &UblkIOCtx| {
                    let iod = q.get_iod(tag);
                    let buf_addr = bufs[tag as usize].as_mut_ptr();
                    let res =
                        handle_io(&guest, &handle, block_size, iod, buf_addr);
                    q.complete_io_cmd(
                        tag,
                        buf_addr,
                        Ok(UblkIORes::Result(res)),
                    );
                };

            let q = UblkQueue::new(qid, dev).unwrap();
            q.wait_and_handle_io(io_handler);
        };

        ctrl.run_target(tgt_init, q_fn, move |ctrl: &UblkCtrl| {
            println!(
                "SEED_UBLK_DEVICE=/dev/ublkb{} advertised size as {} bytes",
                ctrl.dev_info().dev_id,
                total_size
            );
        })
        .map_err(|e| anyhow::anyhow!("run_target: {e:?}"))?;

        Ok(())
    }

    /// Translate a single ublk io descriptor into a crucible BlockIO op.
    /// Returns the byte count on success or a negative errno on failure,
    /// matching what libublk expects in `UblkIORes::Result`.
    fn handle_io(
        guest: &Arc<Guest>,
        handle: &Handle,
        block_size: u64,
        iod: &libublk::sys::ublksrv_io_desc,
        buf_addr: *mut u8,
    ) -> i32 {
        let op = iod.op_flags & 0xff;
        let offset = (iod.start_sector << 9) as u64;
        let bytes = (iod.nr_sectors << 9) as usize;

        // ublk advertises a 512-byte logical block, so it hands us
        // sector-granular offsets/lengths; crucible requires alignment to its
        // own block size. Guard rather than issue a misaligned op.
        if offset % block_size != 0 || (bytes as u64) % block_size != 0 {
            return -libc::EINVAL;
        }
        let block = BlockIndex(offset / block_size);
        let guest: &Guest = guest.as_ref();

        match op {
            libublk::sys::UBLK_IO_OP_READ => {
                let mut buffer = Buffer::new(
                    bytes / block_size as usize,
                    block_size as usize,
                );
                if handle.block_on(guest.read(block, &mut buffer)).is_err() {
                    return -libc::EIO;
                }
                // SAFETY: libublk owns this buffer for `bytes` bytes for the
                // duration of the in-flight tag; we only write within bounds.
                let dst =
                    unsafe { std::slice::from_raw_parts_mut(buf_addr, bytes) };
                dst.copy_from_slice(&buffer[..bytes]);
                bytes as i32
            }
            libublk::sys::UBLK_IO_OP_WRITE => {
                // SAFETY: libublk owns this buffer for `bytes` bytes for the
                // duration of the in-flight tag; we only read within bounds.
                let src =
                    unsafe { std::slice::from_raw_parts(buf_addr, bytes) };
                let data = BytesMut::from(src);
                if handle.block_on(guest.write(block, data)).is_err() {
                    return -libc::EIO;
                }
                bytes as i32
            }
            libublk::sys::UBLK_IO_OP_FLUSH => {
                if handle.block_on(guest.flush(None)).is_err() {
                    return -libc::EIO;
                }
                0
            }
            _ => -libc::EINVAL,
        }
    }
}
