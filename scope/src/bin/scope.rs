use anyhow::{anyhow, bail, Result};
use std::io::ErrorKind;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crucible_scope::messages;

fn ignore_error(e: &std::io::Error) -> bool {
    match e.kind() {
        ErrorKind::NotFound | ErrorKind::ConnectionRefused => {
            /*
             * The socket does not yet exist, or is a stale socket from
             * a previous run.  Just retry.
             */
            true
        }
        _ => false,
    }
}

async fn client(sock: &str, cons: &mut mpsc::Receiver<String>) -> Result<()> {
    let mut us = match UnixStream::connect(sock).await {
        Err(e) if ignore_error(&e) => return Ok(()),
        Err(e) => bail!(e),
        Ok(us) => us,
    };

    let (usr, usw) = us.split();
    let mut busw = BufWriter::new(usw);
    let busr = BufReader::new(usr);
    let mut l = busr.lines();

    /*
     * The first line from the server should be a banner.
     */
    let h: messages::Hello = {
        let l = l.next_line().await?;
        if let Some(l) = l {
            serde_json::from_str(&l)?
        } else {
            bail!("connection terminated before banner");
        }
    };
    println!("\x1b[7m *** connected (pid {}): {}\x1b[0m", h.pid, h.banner);

    /*
     * Subsequent messages are prompt requests.  When we receive a prompt, we
     * need to display the prompt to the user.  The next line of input we
     * receive from the user is sent to the server as a reply to the most recent
     * prompt.
     */
    let mut prompt: Option<messages::PromptOut> = None;
    loop {
        tokio::select! {
            l = cons.recv() => {
                match l {
                    Some(l) => {
                        if let Some(prompt) = prompt.take() {
                            let send = messages::PromptIn {
                                id: prompt.id,
                                reply: l,
                            };
                            let mut buf = serde_json::to_string(&send).unwrap();
                            buf += "\n";
                            busw.write_all(buf.as_bytes()).await?;
                            busw.flush().await?;
                        }
                    }
                    None => return Ok(()),
                }
            }
            l = l.next_line() => {
                match l {
                    Err(e) => bail!(e),
                    Ok(Some(l)) => {
                        match serde_json::from_str::<messages::PromptOut>(&l) {
                            Err(e) => bail!(e),
                            Ok(po) => {
                                println!("{}", po.msg);
                                prompt = Some(po);
                            }
                        }
                    }
                    Ok(None) => {
                        println!(
                            "\x1b[7m *** disconnected (pid {}): {}\x1b[0m\n",
                            h.pid, h.banner);
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let sockpath = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("need sockpath"))?;
    println!("\x1b[7m *** waiting for socket: {}\x1b[0m", sockpath);

    /*
     * Create a thread to read console input, and a tokio channel to get the
     * input from that thread into the asynchronous routines that manage the
     * IPC:
     */
    let (constx, mut consrx) = mpsc::channel::<String>(64);
    std::thread::spawn(move || {
        use std::io::BufRead;
        let stdin = std::io::stdin();
        let bufin = std::io::BufReader::new(stdin.lock());
        let mut lines = bufin.lines();

        loop {
            let line = if let Some(line) = lines.next() {
                if let Ok(line) = line {
                    line
                } else {
                    /*
                     * Ignore invalid UTF-8 for now.
                     */
                    continue;
                }
            } else {
                /*
                 * End of file on stdin.
                 */
                println!("\n\x1b[0m *** end of stdin; exiting");
                std::process::exit(0);
            };
            constx.blocking_send(line).unwrap();
        }
    });

    loop {
        /*
         * This tool is intended to run in a separate terminal from the main
         * software, so it reconnects promptly and forever in the face of any
         * kind of error.
         */
        if let Err(e) = client(&sockpath, &mut consrx).await {
            println!("\x1b[7m *** SCOPE ERROR: {:?}\x1b[0m", e);
        }

        sleep(Duration::from_millis(50)).await;
    }
}
