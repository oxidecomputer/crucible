// Copyright 2023 Oxide Computer Company
use std::borrow::Cow;
use std::net::SocketAddr;

use dsc_client::Client;
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

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Parser, PartialEq)]
pub enum DscCommand {
    /// Connect to the default DSC server (http://127.0.0.1:9998)
    Connect,
    /// Disable random stopping of downstairs
    DisableRandomStop,
    /// Disable auto restart on the given downstairs client ID
    DisableRestart {
        #[clap(long, short, action)]
        cid: u32,
    },
    /// Disable auto restart on all downstairs
    DisableRestartAll,
    /// Enable restart on the given client ID
    EnableRestart {
        #[clap(long, short, action)]
        cid: u32,
    },
    /// Enable random stopping of downstairs
    EnableRandomStop,
    /// Set the minimum random stop time (in seconds)
    EnableRandomMin {
        #[clap(long, short, action)]
        min: u64,
    },
    /// Set the maximum random stop time (in seconds)
    EnableRandomMax {
        #[clap(long, short, action)]
        max: u64,
    },
    /// Enable auto restart on all downstairs
    EnableRestartAll,
    /// Shutdown all downstairs, then shutdown dsc itself.
    Shutdown,
    /// Start the downstairs at the given client ID
    Start {
        #[clap(long, short, action)]
        cid: u32,
    },
    /// Start all downstairs
    StartAll,
    /// Get the state of the given client ID
    State {
        #[clap(long, short, action)]
        cid: u32,
    },
    /// Stop the downstairs at the given client ID
    Stop {
        #[clap(long, short, action)]
        cid: u32,
    },
    /// Stop all the downstairs
    StopAll,
    /// Stop a random downstairs
    StopRand,
}
/*
 * Commands supported by the crucible CLI.  Most of these translate into
 * an actual BlockOpt, but some are processed locally, and some happen
 * on the cli_server side.
 */
#[allow(clippy::derive_partial_eq_without_eq)]
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
    /// DSC
    Dsc {
        /// Subcommand please
        #[clap(subcommand)]
        dsc_cmd: DscCommand,
    },
    /// Report the expected read count for an offset.
    Expected {
        /// The desired offset to see the expected value for.
        #[clap(long, short, action)]
        offset: usize,
    },
    /// Export the current write count to the verify out file
    Export,
    /// Run the fill then verify test.
    Fill {
        /// Don't do the verify step after filling the region.
        #[clap(long, action)]
        skip_verify: bool,
    },
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
    Replace {
        /// Replace a downstairs old (current) SocketAddr.
        #[clap(long, short, action)]
        old: SocketAddr,
        /// Replace a downstairs new SocketAddr.
        #[clap(long, short, action)]
        new: SocketAddr,
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
    /// WriteUnwritten to a given block offset.
    /// Note that the cli will decide if a block is written to based on
    /// the cli's internal write log.  This log may not match if the block
    /// actually has or has not been written to.
    WriteUnwritten {
        /// The desired offset in blocks to write to.
        #[clap(short, action)]
        offset: usize,
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
async fn cli_read(
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

    let data = crucible::Buffer::from_vec(vec![255; length]);

    println!("Read  at block {:5}, len:{:7}", offset.value, data.len());
    guest.read(offset, data.clone()).await?;

    let mut dl = data.into_vec().unwrap();
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
async fn rand_write(
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
    let block_index = rng.gen_range(0..block_max);

    cli_write(guest, ri, block_index, size).await
}

/*
 * Issue a write to the guest at the given offset/len.
 * Data is generated based on the value in the internal write counter.
 * Update the internal write counter so we have something to compare to.
 */
async fn cli_write(
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
     * Unless, we are trying to write off the end of the volume.
     * If so, then don't update any write counts and just make
     * the correct size buffer with all zeros.
     */
    let vec = if block_index + size > ri.total_blocks {
        println!("Skip write log for invalid size {}", ri.total_blocks);
        vec![0; size * ri.block_size as usize]
    } else {
        for bi in block_index..block_index + size {
            ri.write_log.update_wc(bi);
        }
        fill_vec(block_index, size, &ri.write_log, ri.block_size)
    };

    let data = Bytes::from(vec);

    println!("Write at block {:5}, len:{:7}", offset.value, data.len());

    guest.write(offset, data).await?;

    Ok(())
}

/*
 * Issue a write_unwritten to the guest at the given offset.
 * We first check our internal write counter.
 * If we believe the block has not been written to, then we update our
 * internal counter and we will expect the write to change the block.
 * If we do believe the block is written to, then we don't update our
 * internal counter and we don't expect our write to change the contents.
 */
async fn cli_write_unwritten(
    guest: &Arc<Guest>,
    ri: &mut RegionInfo,
    block_index: usize,
) -> Result<(), CrucibleError> {
    /*
     * Convert offset to its byte value.
     */
    let offset = Block::new(block_index as u64, ri.block_size.trailing_zeros());

    // To determine what we put into our write buffer, look to see if
    // we believe we have written to this block or not.
    let data = if ri.write_log.get_seed(block_index) == 0 {
        // We have not written to this block, so we create our write buffer
        // like normal and update our internal counter to reflect that.

        ri.write_log.update_wc(block_index);
        let vec = fill_vec(block_index, 1, &ri.write_log, ri.block_size);
        Bytes::from(vec)
    } else {
        println!("This block has been written");
        // Fill the write buffer with random data.  We don't expect this
        // to actually make it to disk.

        let mut vec = Vec::with_capacity(ri.block_size as usize);
        for _ in 0..ri.block_size {
            vec.push(rand::thread_rng().gen::<u8>());
        }
        Bytes::from(vec)
    };

    println!(
        "WriteUnwritten at block {:5}, len:{:7}",
        offset.value,
        data.len(),
    );

    guest.write_unwritten(offset, data).await?;

    Ok(())
}

// Handle dsc commands
async fn handle_dsc(
    dsc_client: &mut Option<dsc_client::Client>,
    dsc_cmd: DscCommand,
) {
    if let Some(dsc_client) = dsc_client {
        match dsc_cmd {
            DscCommand::Connect => {
                println!("Already connected");
            }
            DscCommand::DisableRandomStop => {
                let res = dsc_client.dsc_disable_random_stop().await;
                println!("Got res: {:?}", res);
            }
            DscCommand::DisableRestart { cid } => {
                let res = dsc_client.dsc_disable_restart(cid).await;
                println!("Got res: {:?}", res);
            }
            DscCommand::DisableRestartAll => {
                let res = dsc_client.dsc_disable_restart_all().await;
                println!("Got res: {:?}", res);
            }
            DscCommand::EnableRandomStop => {
                let res = dsc_client.dsc_enable_random_stop().await;
                println!("Got res: {:?}", res);
            }
            DscCommand::EnableRandomMin { min } => {
                let res = dsc_client.dsc_enable_random_min(min).await;
                println!("Got res: {:?}", res);
            }
            DscCommand::EnableRandomMax { max } => {
                let res = dsc_client.dsc_enable_random_max(max).await;
                println!("Got res: {:?}", res);
            }
            DscCommand::EnableRestart { cid } => {
                let res = dsc_client.dsc_enable_restart(cid).await;
                println!("Got res: {:?}", res);
            }
            DscCommand::EnableRestartAll => {
                let res = dsc_client.dsc_enable_restart_all().await;
                println!("Got res: {:?}", res);
            }
            DscCommand::Shutdown => {
                let res = dsc_client.dsc_shutdown().await;
                println!("Got res: {:?}", res);
            }
            DscCommand::Start { cid } => {
                let res = dsc_client.dsc_start(cid).await;
                println!("Got res: {:?}", res);
            }
            DscCommand::StartAll => {
                let res = dsc_client.dsc_start_all().await;
                println!("Got res: {:?}", res);
            }
            DscCommand::Stop { cid } => {
                let res = dsc_client.dsc_stop(cid).await;
                println!("Got res: {:?}", res);
            }
            DscCommand::StopAll => {
                let res = dsc_client.dsc_stop_all().await;
                println!("Got res: {:?}", res);
            }
            DscCommand::StopRand => {
                let res = dsc_client.dsc_stop_rand().await;
                println!("Got res: {:?}", res);
            }
            DscCommand::State { cid } => {
                let res = dsc_client.dsc_get_ds_state(cid).await;
                println!("Got res: {:?}", res);
            }
        }
    } else if dsc_cmd == DscCommand::Connect {
        let url = "http://127.0.0.1:9998".to_string();
        println!("Connect to {:?}", url);
        let rs = Client::new(&url);
        *dsc_client = Some(rs);
    } else {
        println!("dsc: Need to be connected first");
    }
}
/*
 * Take a CLI cmd coming from our client program and translate it into
 * an actual CliMessage to send to the cli server.
 *
 * At the moment, we ping pong here, where we send a command to the
 * cli_server, then we wait for the response.
 * Eventually we could make this async, but, yeah, I got things to do.
 *
 * Also, some commands (dsc for example) don't talk to the cliserver, so
 * those and commands like them should return status directly instead of
 * falling through the match and then entering the "wait for a response"
 * side where they will never get a response.
 */
async fn cmd_to_msg(
    cmd: CliCommand,
    fr: &mut FramedRead<tokio::net::tcp::ReadHalf<'_>, CliDecoder>,
    fw: &mut FramedWrite<WriteHalf<'_>, CliEncoder>,
    dsc_client: &mut Option<dsc_client::Client>,
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
        CliCommand::Dsc { dsc_cmd } => {
            handle_dsc(dsc_client, dsc_cmd).await;
            return Ok(());
        }
        CliCommand::Expected { offset } => {
            fw.send(CliMessage::Expected(offset)).await?;
        }
        CliCommand::Export => {
            fw.send(CliMessage::Export).await?;
        }
        CliCommand::Fill { skip_verify } => {
            fw.send(CliMessage::Fill(skip_verify)).await?;
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
        CliCommand::Replace { old, new } => {
            fw.send(CliMessage::Replace(old, new)).await?;
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
        CliCommand::WriteUnwritten { offset } => {
            fw.send(CliMessage::WriteUnwritten(offset)).await?;
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
        Some(CliMessage::ReplaceResult(resp)) => match resp {
            Ok(msg) => {
                println!("Replace returns: {:?}", msg);
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

        let deadline = tokio::time::sleep_until(deadline_secs(100.0));
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
                            tokio::time::sleep_until(deadline_secs(10.0)).await;
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
        let mut dsc_client = None;

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
                            cmd_to_msg(vc, &mut fr, &mut fw, &mut dsc_client)
                                .await?;
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
        CliMessage::Activate(gen) => match guest.activate_with_gen(gen).await {
            Ok(_) => fw.send(CliMessage::DoneOk).await,
            Err(e) => fw.send(CliMessage::Error(e)).await,
        },
        CliMessage::Deactivate => match guest.deactivate().await {
            Ok(_) => fw.send(CliMessage::DoneOk).await,
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
                vec[1] = ri.write_log.get_seed(offset) % 255;
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
                let mut wtq = WhenToQuit::Count { count };
                match generic_workload(guest, &mut wtq, ri, false).await {
                    Ok(_) => fw.send(CliMessage::DoneOk).await,
                    Err(e) => {
                        let msg = format!("{}", e);
                        let e = CrucibleError::GenericError(msg);
                        fw.send(CliMessage::Error(e)).await
                    }
                }
            }
        }
        CliMessage::Fill(skip_verify) => {
            if ri.write_log.is_empty() {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Info not initialized".to_string(),
                )))
                .await
            } else {
                match fill_workload(guest, ri, skip_verify).await {
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
            match guest.flush(None).await {
                Ok(_) => fw.send(CliMessage::DoneOk).await,
                Err(e) => fw.send(CliMessage::Error(e)).await,
            }
        }
        CliMessage::IsActive => match guest.query_is_active().await {
            Ok(a) => fw.send(CliMessage::ActiveIs(a)).await,
            Err(e) => fw.send(CliMessage::Error(e)).await,
        },
        CliMessage::InfoPlease => {
            let new_ri = get_region_info(guest).await;
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
                            load_write_log(guest, ri, vi, false).await?;
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
                perf_header();
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

                let res = cli_read(guest, ri, offset, size).await;
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
                match rand_write(guest, ri).await {
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
                let res = cli_read(guest, ri, offset, len).await;
                fw.send(CliMessage::ReadResponse(offset, res)).await
            }
        }
        CliMessage::Replace(old, new) => {
            let res = guest.replace_downstairs(Uuid::new_v4(), old, new).await;
            fw.send(CliMessage::ReplaceResult(res)).await
        }
        CliMessage::ShowWork => match guest.show_work().await {
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
                match cli_write(guest, ri, offset, len).await {
                    Ok(_) => fw.send(CliMessage::DoneOk).await,
                    Err(e) => fw.send(CliMessage::Error(e)).await,
                }
            }
        }
        CliMessage::WriteUnwritten(offset) => {
            if ri.write_log.is_empty() {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Info not initialized".to_string(),
                )))
                .await
            } else {
                match cli_write_unwritten(guest, ri, offset).await {
                    Ok(_) => fw.send(CliMessage::DoneOk).await,
                    Err(e) => fw.send(CliMessage::Error(e)).await,
                }
            }
        }
        CliMessage::Uuid => {
            let uuid = guest.get_uuid().await?;
            fw.send(CliMessage::MyUuid(uuid)).await
        }
        CliMessage::Verify => {
            if ri.write_log.is_empty() {
                fw.send(CliMessage::Error(CrucibleError::GenericError(
                    "Info not initialized".to_string(),
                )))
                .await
            } else {
                match verify_volume(guest, ri, false).await {
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
