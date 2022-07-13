// Copyright 2022 Oxide Computer Company
use std::borrow::Cow;
use std::net::SocketAddr;

use crossterm::style::Color;
use futures::{SinkExt, StreamExt};
use reedline::{
    FileBackedHistory, Prompt, PromptEditMode, PromptHistorySearch, Reedline,
    Signal,
};
use tokio::net::tcp::WriteHalf;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio_util::codec::{FramedRead, FramedWrite};

use super::*;
use protocol::*;

#[derive(Debug, Parser)]
#[clap(name = "Cli", term_width = 80, no_binary_name = true)]
pub struct CliAction {
    #[clap(subcommand)]
    cmd: CliCommand,
}
/*
 * Commands supported by the crucible CLI.  Most of these translate into
 * an actual BlockOpt, but some are processed locally, and some happen
 * on the cli_server side.
 */
#[derive(Debug, Parser, PartialEq)]
#[clap(name = "", term_width = 80, no_binary_name = true)]
enum CliCommand {
    /// Send an activation message to all the downstairs and block
    /// until all the downstairs answer
    Activate {
        /// Specify this generation number to use when requesting activation.
        #[clap(long, short, default_value = "1", action)]
        gen: u64,
    },
    /// Commit the current write_log data to the minimum expected counts.
    Commit,
    /// Deactivate the upstairs
    Deactivate,
    /// Report the expected read count for an offset.
    Expected {
        /// The desired offset to see the expected value for.
        #[clap(long, short, action)]
        offset: usize,
    },
    /// Export the current write count to the verify out file
    Export,
    /// Run the fill then verify test.
    Fill,
    /// Flush
    Flush,
    /// Run Generic workload
    Generic {
        /// Number of IOs to execute
        #[clap(long, short, default_value = "5000", action)]
        count: usize,
    },
    /// Request region information
    Info,
    /// Report if the Upstairs is ready for guest IO
    IsActive,
    /// Run the client perf test
    Perf {
        /// Number of IOs to execute for each test phase
        #[clap(long, short, default_value = "5000", action)]
        count: usize,
        /// Size in blocks of each IO
        #[clap(long, default_value = "1", action)]
        io_size: usize,
        /// Number of outstanding IOs at the same time
        #[clap(long, default_value = "1", action)]
        io_depth: usize,
        /// Number of read test loops to do.
        #[clap(long, default_value = "2", action)]
        read_loops: usize,
        /// Number of write test loops to do.
        #[clap(long, default_value = "2", action)]
        write_loops: usize,
    },
    /// Quit the CLI
    Quit,
    /// Read from a given block offset
    Read {
        /// The desired offset in blocks to read from.
        #[clap(long, short, action)]
        offset: usize,
        /// The number of blocks to read.
        #[clap(long, short, default_value = "1", action)]
        len: usize,
    },
    /// Issue a random read
    Rr,
    /// Issue a random write
    Rw,
    /// Show the work queues
    Show,
    /// Change the wait state between true and false
    Wait,
    /// Read and verify the whole volume.
    Verify,
    /// Write to a given block offset
    Write {
        /// The desired offset in blocks to write to.
        #[clap(short, action)]
        offset: usize,
        /// The number of blocks to write.
        #[clap(long, short, default_value = "1", action)]
        len: usize,
    },
    /// Get the upstairs UUID
    Uuid,
}

/*
 * Generate a read for the guest with the given offset/length.
 * Wait for the IO to return.
 * Verify the data is as we expect using the client based validation.
 * Note that if you have not written to a block yet and you are not
 * importing a verify file, this will default to passing.  Only when
 * there is non zero data in the ri.write_log will we have something
 * to verify against.
 *
 * After verify, we truncate the data to 10 fields and return that so
 * the cli server can send it back to the client for display.
 */
fn cli_read(
    guest: &Arc<Guest>,
    ri: &mut RegionInfo,
    block_index: usize,
    size: usize,
) -> Result<Vec<u8>, CrucibleError> {
    /*
     * Convert offset to its byte value.
     */
    let offset = Block::new(block_index as u64, ri.block_size.trailing_zeros());
    let length: usize = size * ri.block_size as usize;

    let vec: Vec<u8> = vec![255; length];
    let data = crucible::Buffer::from_vec(vec);

    println!("Read  at block {:5}, len:{:7}", offset.value, data.len());
    let mut waiter = guest.read(offset, data.clone())?;
    waiter.block_wait()?;

    let mut dl = data.as_vec().to_vec();
    match validate_vec(
        dl.clone(),
        block_index,
        &mut ri.write_log,
        ri.block_size,
        false,
    ) {
        ValidateStatus::Bad => {
            println!("Data mismatch error at {}", block_index);
            Err(CrucibleError::GenericError("Data mismatch".to_string()))
        }
        ValidateStatus::InRange => {
            println!("Data mismatch range error at {}", block_index);
            Err(CrucibleError::GenericError("Data range error".to_string()))
        }
        ValidateStatus::Good => {
            dl.truncate(10);
            Ok(dl)
        }
    }
}

/*
 * A wrapper around write that just picks a random offset.
 */
fn rand_write(
    guest: &Arc<Guest>,
    ri: &mut RegionInfo,
) -> Result<(), CrucibleError> {
    /*
     * TODO: Allow the user to specify a seed here.
     */
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();

    /*
     * Once we have our IO size, decide where the starting offset should
     * be, which is the total possible size minus the randomly chosen
     * IO size.
     */
    let size = 1;
    let block_max = ri.total_blocks - size + 1;
    let block_index = rng.gen_range(0..block_max) as usize;

    cli_write(guest, ri, block_index, size)
}

/*
 * Issue a write to the guest at the given offset/len.
 * Data is generated based on the value in the internal write counter.
 * Update the internal write counter so we have something to compare to.
 */
fn cli_write(
    guest: &Arc<Guest>,
    ri: &mut RegionInfo,
    block_index: usize,
    size: usize,
) -> Result<(), CrucibleError> {
    /*
     * Convert offset and length to their byte values.
     */
    let offset = Block::new(block_index as u64, ri.block_size.trailing_zeros());

    /*
     * Update the write count for the block we plan to write to.
     */
    ri.write_log.update_wc(block_index);

    let vec = fill_vec(block_index, size, &ri.write_log, ri.block_size);
    let data = Bytes::from(vec);

    println!("Write at block {:5}, len:{:7}", offset.value, data.len());

    let mut waiter = guest.write(offset, data)?;
    waiter.block_wait()?;

    Ok(())
}

/*
 * Take a CLI cmd coming from our client program and translate it into
 * an actual CliMessage to send to the cli server.
 *
 * At the moment, we ping pong here, where we send a command to the
 * cli_server, then we wait for the response.
 * Eventually we could make this async, but, yeah, I got things to do.
 */
async fn cmd_to_msg(
    cmd: CliCommand,
    fr: &mut FramedRead<tokio::net::tcp::ReadHalf<'_>, CliDecoder>,
    fw: &mut FramedWrite<WriteHalf<'_>, CliEncoder>,
) -> Result<()> {
    match cmd {
        CliCommand::Activate { gen } => {
            fw.send(CliMessage::Activate(gen)).await?;
        }
        CliCommand::Commit => {
            fw.send(CliMessage::Commit).await?;
        }
        CliCommand::Deactivate => {
            fw.send(CliMessage::Deactivate).await?;
        }
        CliCommand::Expected { offset } => {
            fw.send(CliMessage::Expected(offset)).await?;
        }
        CliCommand::Export => {
            fw.send(CliMessage::Export).await?;
        }
        CliCommand::Fill => {
            fw.send(CliMessage::Fill).await?;
        }
        CliCommand::Flush => {
            fw.send(CliMessage::Flush).await?;
        }
        CliCommand::Generic { count } => {
            fw.send(CliMessage::Generic(count)).await?;
        }
        CliCommand::IsActive => {
            fw.send(CliMessage::IsActive).await?;
        }
        CliCommand::Info => {
            fw.send(CliMessage::InfoPlease).await?;
        }
        CliCommand::Perf {
            count,
            io_size,
            io_depth,
            read_loops,
            write_loops,
        } => {
            fw.send(CliMessage::Perf(
                count,
                io_size,
                io_depth,
                read_loops,
                write_loops,
            ))
            .await?;
        }
        CliCommand::Quit => {
            println!("The quit command has nothing to send");
            return Ok(());
        }
        CliCommand::Read { offset, len } => {
            fw.send(CliMessage::Read(offset, len)).await?;
        }
        CliCommand::Rr => {
            fw.send(CliMessage::RandRead).await?;
        }
        CliCommand::Rw => {
            fw.send(CliMessage::RandWrite).await?;
        }
        CliCommand::Show => {
            fw.send(CliMessage::ShowWork).await?;
        }
        CliCommand::Uuid => {
            fw.send(CliMessage::Uuid).await?;
        }
        CliCommand::Verify => {
            fw.send(CliMessage::Verify).await?;
        }
        CliCommand::Wait => {
            println!("No support for Wait");
            return Ok(());
        }
        CliCommand::Write { offset, len } => {
            fw.send(CliMessage::Write(offset, len)).await?;
        }
    }
    /*
     * Now, wait for our response
     */
    let new_read = fr.next().await;
    match new_read.transpose()? {
        Some(CliMessage::MyUuid(uuid)) => {
            println!("uuid: {}", uuid);
        }
        Some(CliMessage::Info(es, bs, bl)) => {
            println!("Got info: {} {} {}", es, bs, bl);
        }
        Some(CliMessage::DoneOk) => {
            println!("Ok");
        }
        Some(CliMessage::ExpectedResponse(offset, data)) => {
            println!("[{}] Expt: {:?}", offset, data);
        }
        Some(CliMessage::ReadResponse(offset, resp)) => match resp {
            Ok(data) => {
                println!("[{}] Data: {:?}", offset, data);
            }
            Err(e) => {
                println!("ERROR: {:?}", e);
            }
        },
        Some(CliMessage::Error(e)) => {
            println!("ERROR: {:?}", e);
        }
        Some(CliMessage::ActiveIs(active)) => {
            println!("Active is: {}", active);
        }
        m => {
            println!("No code for this response {:?}", m);
        }
    }
    Ok(())
}

// Generic prompt stuff for reedline.
#[derive(Clone)]
pub struct CliPrompt;
impl Prompt for CliPrompt {
    fn render_prompt_left(&self) -> Cow<str> {
        Cow::Owned(String::from(">> "))
    }

    fn render_prompt_right(&self) -> Cow<str> {
        Cow::Owned(String::from(""))
    }

    fn render_prompt_indicator(&self, _edit_mode: PromptEditMode) -> Cow<str> {
        Cow::Owned(String::from(""))
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<str> {
        Cow::Owned(String::from(""))
    }

    fn render_prompt_history_search_indicator(
        &self,
        _history_search: PromptHistorySearch,
    ) -> Cow<str> {
        Cow::Owned(String::from(""))
    }
    fn get_prompt_color(&self) -> Color {
        Color::White
    }
}

impl Default for CliPrompt {
    fn default() -> Self {
        CliPrompt::new()
    }
}

impl CliPrompt {
    pub fn new() -> CliPrompt {
        CliPrompt {}
    }
}

/*
 * The CLI just sends commands to the cli_server where all the logic
 * lives, including any state about what blocks were written.
 */
pub async fn start_cli_client(attach: SocketAddr) -> Result<()> {
    'outer: loop {
        let sock = if attach.is_ipv4() {
            TcpSocket::new_v4().unwrap()
        } else {
            TcpSocket::new_v6().unwrap()
        };

        println!("cli connecting to {0}", attach);

        let deadline = tokio::time::sleep_until(deadline_secs(100));
        tokio::pin!(deadline);
        let tcp = sock.connect(attach);
        tokio::pin!(tcp);

        let mut tcp: TcpStream = loop {
            tokio::select! {
                _ = &mut deadline => {
                    println!("connect timeout");
                    continue 'outer;
                }
                tcp = &mut tcp => {
                    match tcp {
                        Ok(tcp) => {
                            println!("connected to {}", attach);
                            break tcp;
                        }
                        Err(e) => {
                            println!("connect to {0} failure: {1:?}",
                                attach, e);
                            tokio::time::sleep_until(deadline_secs(10)).await;
                            continue 'outer;
                        }
                    }
                }
            }
        };

        /*
         * Create the read/write endpoints so this client can send and
         * receive messages from the cli_server.
         */
        let (r, w) = tcp.split();
        let mut fr = FramedRead::new(r, CliDecoder::new());
        let mut fw = FramedWrite::new(w, CliEncoder::new());

        let history = Box::new(
            FileBackedHistory::with_file(50, "history.txt".into())
                .expect("Error configuring history with file"),
        );
        let mut line_editor = Reedline::create().with_history(history);
        let prompt = CliPrompt::new();

        loop {
            let sig = line_editor.read_line(&prompt)?;
            match sig {
                Signal::Success(buffer) => {
                    let cmds: Vec<&str> = buffer.trim().split(' ').collect();

                    // Empty command, just ignore it and loop.
                    if cmds[0].is_empty() {
                        continue;
                    }
                    match CliCommand::try_parse_from(cmds) {
                        Ok(CliCommand::Quit) => {
                            break;
                        }
                        Ok(vc) => {
                            cmd_to_msg(vc, &mut fr, &mut fw).await?;
                            // TODO: Handle this error
                        }
                        Err(e) => {
                            println!("{}", e);
                        }
                    }
                }
                Signal::CtrlD | Signal::CtrlC => {
                    println!("CTRL-C");
                    break;
                }
            }
        }
        // TODO: Figure out how to handle a disconnect from the crucible
        // side and let things continue.
        break;
    }
    Ok(())
}

/**
 * Process a CLI command from the client, we are the server side.
 */
async fn process_cli_command(
    guest: &Arc<Guest>,
    fw: &mut FramedWrite<tokio::net::tcp::OwnedWriteHalf, CliEncoder>,
    cmd: protocol::CliMessage,
    ri: &mut RegionInfo,
    wc_filled: &mut bool,
    verify_input: Option<PathBuf>,
    verify_output: Option<PathBuf>,
) -> Result<()> {
    match cmd {
        CliMessage::Activate(gen) => match guest.activate(gen) {
            Ok(_) => fw.send(CliMessage::DoneOk).await,
            Err(e) => fw.send(CliMessage::Error(e)).await,
        },
        CliMessage::Deactivate => match guest.deactivate() {
            Ok(mut waiter) => match waiter.block_wait() {
                Ok(_) => fw.send(CliMessage::DoneOk).await,
                Err(e) => fw.send(CliMessage::Error(e)).await,
            },
            Err(e) => fw.send(CliMessage::Error(e)).await,
        },
        CliMessage::Commit => {
            if ri.write_log.is_empty() {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Info not initialized".to_string(),
                )))
                .await
            } else {
                ri.write_log.commit();
                fw.send(CliMessage::DoneOk).await
            }
        }
        CliMessage::Expected(offset) => {
            if !*wc_filled {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Internal write count buffer not filled".to_string(),
                )))
                .await
            } else if ri.write_log.is_empty() {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Internal write count buffer empty".to_string(),
                )))
                .await
            } else {
                let mut vec: Vec<u8> = vec![255; 2];
                vec[0] = (offset % 255) as u8;
                vec[1] = (ri.write_log.get_seed(offset) % 255) as u8;
                fw.send(CliMessage::ExpectedResponse(offset, vec)).await
            }
        }
        CliMessage::Export => {
            if ri.write_log.is_empty() {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Info not initialized".to_string(),
                )))
                .await
            } else if let Some(vo) = verify_output {
                println!("Exporting write history to {:?}", vo);
                let cp = history_file(vo.clone());
                match write_json(&cp, &ri.write_log, true) {
                    Ok(_) => fw.send(CliMessage::DoneOk).await,
                    Err(e) => {
                        println!("Failed writing to {:?} with {}", vo, e);
                        fw.send(CliMessage::Error(CrucibleError::GenericError(
                            "Failed writing to file".to_string(),
                        )))
                        .await
                    }
                }
            } else {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "No verify-out file provided".to_string(),
                )))
                .await
            }
        }
        CliMessage::Generic(count) => {
            if ri.write_log.is_empty() {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Info not initialized".to_string(),
                )))
                .await
            } else {
                match generic_workload(guest, count, ri).await {
                    Ok(_) => fw.send(CliMessage::DoneOk).await,
                    Err(e) => {
                        let msg = format!("{}", e);
                        let e = CrucibleError::GenericError(msg);
                        fw.send(CliMessage::Error(e)).await
                    }
                }
            }
        }
        CliMessage::Fill => {
            if ri.write_log.is_empty() {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Info not initialized".to_string(),
                )))
                .await
            } else {
                match fill_workload(guest, ri).await {
                    Ok(_) => fw.send(CliMessage::DoneOk).await,
                    Err(e) => {
                        let msg = format!("Fill/Verify failed with {}", e);
                        let e = CrucibleError::GenericError(msg);
                        fw.send(CliMessage::Error(e)).await
                    }
                }
            }
        }
        CliMessage::Flush => {
            println!("Flush");
            match guest.flush(None) {
                Ok(_) => fw.send(CliMessage::DoneOk).await,
                Err(e) => fw.send(CliMessage::Error(e)).await,
            }
        }
        CliMessage::IsActive => match guest.query_is_active() {
            Ok(a) => fw.send(CliMessage::ActiveIs(a)).await,
            Err(e) => fw.send(CliMessage::Error(e)).await,
        },
        CliMessage::InfoPlease => {
            let new_ri = get_region_info(guest);
            match new_ri {
                Ok(new_ri) => {
                    let bs = new_ri.block_size;
                    let es = new_ri.extent_size.value;
                    let ts = new_ri.total_size;
                    *ri = new_ri;
                    /*
                     * We may only want to read input from the file once.
                     * Maybe make a command to specifically do it, but it
                     * seems like once we go active we won't need to run
                     * it again.
                     */
                    if !*wc_filled {
                        if let Some(vi) = verify_input {
                            load_write_log(guest, ri, vi, false)?;
                            *wc_filled = true;
                        }
                    }
                    fw.send(CliMessage::Info(bs, es, ts)).await
                }
                Err(e) => fw.send(CliMessage::Error(e)).await,
            }
        }
        CliMessage::Perf(count, io_size, io_depth, read_loops, write_loops) => {
            if ri.write_log.is_empty() {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Info not initialized".to_string(),
                )))
                .await
            } else {
                match perf_workload(
                    guest,
                    ri,
                    &mut None,
                    count,
                    io_size,
                    io_depth,
                    read_loops,
                    write_loops,
                )
                .await
                {
                    Ok(_) => fw.send(CliMessage::DoneOk).await,
                    Err(e) => {
                        let msg = format!("{}", e);
                        let e = CrucibleError::GenericError(msg);
                        fw.send(CliMessage::Error(e)).await
                    }
                }
            }
        }
        CliMessage::RandRead => {
            if ri.write_log.is_empty() {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Info not initialized".to_string(),
                )))
                .await
            } else {
                let mut rng = rand_chacha::ChaCha8Rng::from_entropy();
                let size = 1;
                let block_max = ri.total_blocks - size + 1;
                let offset = rng.gen_range(0..block_max);

                let res = cli_read(guest, ri, offset, size);
                fw.send(CliMessage::ReadResponse(offset, res)).await
            }
        }
        CliMessage::RandWrite => {
            if ri.write_log.is_empty() {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Info not initialized".to_string(),
                )))
                .await
            } else {
                match rand_write(guest, ri) {
                    Ok(_) => fw.send(CliMessage::DoneOk).await,
                    Err(e) => fw.send(CliMessage::Error(e)).await,
                }
            }
        }
        CliMessage::Read(offset, len) => {
            if ri.write_log.is_empty() {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Info not initialized".to_string(),
                )))
                .await
            } else {
                let res = cli_read(guest, ri, offset, len);
                fw.send(CliMessage::ReadResponse(offset, res)).await
            }
        }
        CliMessage::ShowWork => match guest.show_work() {
            Ok(_) => fw.send(CliMessage::DoneOk).await,
            Err(e) => fw.send(CliMessage::Error(e)).await,
        },
        CliMessage::Write(offset, len) => {
            if ri.write_log.is_empty() {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Info not initialized".to_string(),
                )))
                .await
            } else {
                match cli_write(guest, ri, offset, len) {
                    Ok(_) => fw.send(CliMessage::DoneOk).await,
                    Err(e) => fw.send(CliMessage::Error(e)).await,
                }
            }
        }
        CliMessage::Uuid => {
            let uuid = guest.query_upstairs_uuid()?;
            fw.send(CliMessage::MyUuid(uuid)).await
        }
        CliMessage::Verify => {
            if ri.write_log.is_empty() {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Info not initialized".to_string(),
                )))
                .await
            } else {
                match verify_volume(guest, ri, false) {
                    Ok(_) => fw.send(CliMessage::DoneOk).await,
                    Err(e) => {
                        println!("Verify failed with {:?}", e);
                        fw.send(CliMessage::Error(CrucibleError::GenericError(
                            "Verify failed".to_string(),
                        )))
                        .await
                    }
                }
            }
        }
        msg => {
            println!("No code written for {:?}", msg);
            Ok(())
        }
    }
}

/*
 * Server for a crucible client CLI.
 * This opens a network port and listens for commands from the cli_client.
 * When it receives one, it translates it into the crucible Guest command
 * and passes it on to the Upstairs.
 * State is kept here.
 * No checking is done.
 * Wait here if you want.
 */
pub async fn start_cli_server(
    guest: &Arc<Guest>,
    address: IpAddr,
    port: u16,
    verify_input: Option<PathBuf>,
    verify_output: Option<PathBuf>,
) -> Result<()> {
    let listen_on = match address {
        IpAddr::V4(ipv4) => SocketAddr::new(std::net::IpAddr::V4(ipv4), port),
        IpAddr::V6(ipv6) => SocketAddr::new(std::net::IpAddr::V6(ipv6), port),
    };

    /*
     * Establish a listen server on the port.
     */
    println!("Listening for a CLI connection on: {:?}", listen_on);
    let listener = TcpListener::bind(&listen_on).await?;

    /*
     * If write_log len is zero, then the RegionInfo has
     * not been filled.
     */
    let mut ri: RegionInfo = RegionInfo {
        block_size: 0,
        extent_size: Block::new_512(0),
        total_size: 0,
        total_blocks: 0,
        write_log: WriteLog::new(0),
        max_block_io: 0,
    };
    /*
     * If we have write info data from previous runs, we can't update our
     * internal region info struct until we actually connect to our
     * downstairs and get that region info. Once we have it, we can
     * populate it with what we expect for each block. If we have filled
     * the write count struct once, or we did not provide any previous
     * write counts, don't require it again.
     */
    let mut wc_filled = verify_input.is_none();
    loop {
        let (sock, raddr) = listener.accept().await?;
        println!("connection from {:?}", raddr);

        let (read, write) = sock.into_split();
        let mut fr = FramedRead::new(read, CliDecoder::new());
        let mut fw = FramedWrite::new(write, CliEncoder::new());

        loop {
            tokio::select! {
                new_read = fr.next() => {

                    match new_read.transpose()? {
                        None => {
                            println!("Got nothing from socket");
                            break;
                        },
                        Some(cmd) => {
                            process_cli_command(
                                guest,
                                &mut fw,
                                cmd,
                                &mut ri,
                                &mut wc_filled,
                                verify_input.clone(),
                                verify_output.clone()
                            ).await?;
                        }
                    }
                }
            }
        }
        println!("Exiting, wait for another connection");
    }
}
