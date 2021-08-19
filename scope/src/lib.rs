// Copyright 2021 Oxide Computer Company
use anyhow::Result;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixListener;
use tokio::sync::{broadcast, watch};

pub mod messages {
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Serialize, Deserialize)]
    pub struct Hello {
        pub pid: u32,
        pub banner: String,
    }

    #[derive(Clone, Serialize, Deserialize)]
    pub struct PromptOut {
        pub id: u64,
        pub msg: String,
    }

    #[derive(Clone, Serialize, Deserialize)]
    pub struct PromptIn {
        pub id: u64,
        pub reply: String,
    }
}

struct Condition {
    next_val: Mutex<u64>,
    tx: watch::Sender<u64>,
    rx: watch::Receiver<u64>,
}

struct ConditionSubscription(watch::Receiver<u64>);

impl ConditionSubscription {
    async fn wait(&mut self) {
        self.0.changed().await.expect("condition subscription")
    }
}

impl Condition {
    fn new() -> Condition {
        let (tx, rx) = watch::channel(0u64);
        Condition {
            next_val: Mutex::new(1),
            tx,
            rx,
        }
    }

    fn subscribe(&self) -> ConditionSubscription {
        ConditionSubscription(self.rx.clone())
    }

    fn signal(&self) {
        let next_val = {
            let mut nto = self.next_val.lock().unwrap();
            let t = *nto;
            *nto += 1;
            t
        };
        self.tx.send(next_val).unwrap();
    }
}

struct Queue {
    next_id: u64,
    out: VecDeque<messages::PromptOut>,
}

struct Inner {
    banner: String,
    sock: UnixListener,
    client_ids: Mutex<u64>,
    queue: Mutex<Queue>,
    txin: broadcast::Sender<messages::PromptIn>,
    cv: Condition,
}

pub struct Server(Arc<Inner>);

async fn scope_connection(
    c: Arc<Inner>,
    mut us: tokio::net::UnixStream,
    mut cv: ConditionSubscription,
) {
    let cid = {
        let mut ncid = c.client_ids.lock().unwrap();
        let cid = *ncid;
        *ncid += 1;
        cid
    };

    println!("SCOPE[{}]: connect", cid);
    let (usr, usw) = us.split();
    let mut busw = BufWriter::new(usw);
    let busr = BufReader::new(usr);
    let mut l = busr.lines();

    /*
     * Send the opening banner that includes the pid of this process to the
     * scope client:
     */
    {
        let hello = messages::Hello {
            pid: std::process::id(),
            banner: c.banner.to_string(),
        };
        let mut buf = serde_json::to_string(&hello).unwrap();
        buf += "\n";
        if let Err(e) = busw.write_all(buf.as_bytes()).await {
            println!("SCOPE[{}]: write failure: {:?}", cid, e);
            return;
        }
        if let Err(e) = busw.flush().await {
            println!("SCOPE[{}]: flush failure: {:?}", cid, e);
            return;
        }
    }

    /*
     * Track the ID of the last prompt message that was sent to this scope
     * client.
     */
    let mut lastsent: Option<u64> = None;

    loop {
        let send = {
            /*
             * Look at the prompt at the head of the display queue if there is
             * one:
             */
            let q = c.queue.lock().unwrap();
            match (&lastsent, q.out.iter().next()) {
                (None, Some(po)) => {
                    /*
                     * We have not sent a prompt yet at all, so send whatever
                     * is available.
                     */
                    Some(po.clone())
                }
                (Some(lastsent), Some(po)) if *lastsent != po.id => {
                    /*
                     * If a new prompt is now at the head of the queue, send
                     * that to the scope for display.
                     */
                    Some(po.clone())
                }
                _ => None,
            }
        };

        if let Some(send) = send {
            let mut buf = serde_json::to_string(&send).unwrap();
            buf += "\n";
            if let Err(e) = busw.write_all(buf.as_bytes()).await {
                println!("SCOPE[{}]: write failure: {:?}", cid, e);
                return;
            }
            if let Err(e) = busw.flush().await {
                println!("SCOPE[{}]: flush failure: {:?}", cid, e);
                return;
            }
            lastsent = Some(send.id);
        }

        tokio::select! {
            _ = cv.wait() => continue,
            l = l.next_line() => {
                match l {
                    Ok(Some(l)) => {
                        match serde_json::from_str(&l) {
                            Ok(pi) => {
                                if c.txin.send(pi).is_err() {
                                    /*
                                     * XXX We don't seem to be able to unwrap
                                     * the error in this context for some
                                     * reason.
                                     */
                                    return;
                                }
                            }
                            Err(e) => {
                                println!("SCOPE[{}]: message: {:?}", cid, e);
                            }
                        };
                    }
                    Ok(None) => {
                        println!("SCOPE[{}]: stream done", cid);
                        return;
                    }
                    Err(e) => {
                        println!("SCOPE[{}]: read error: {:?}", cid, e);
                        return;
                    }
                }
            }
        }
    }
}

impl Server {
    pub async fn new<P: AsRef<Path>>(
        sockpath: P,
        banner: &str,
    ) -> Result<Server> {
        let sockpath = sockpath.as_ref();

        if sockpath.exists() {
            /*
             * Unlink the existing socket before we create a new one.
             */
            std::fs::remove_file(&sockpath)?;
        }

        let (txin, _) = broadcast::channel::<messages::PromptIn>(64);

        let s0 = Arc::new(Inner {
            banner: banner.to_string(),
            sock: UnixListener::bind(sockpath)?,
            client_ids: Mutex::new(1),
            queue: Mutex::new(Queue {
                next_id: 9000,
                out: VecDeque::new(),
            }),
            txin,
            cv: Condition::new(),
        });

        let s = Arc::clone(&s0);
        tokio::spawn(async move {
            loop {
                /*
                 * Handle connections from control clients:
                 */
                match s.sock.accept().await {
                    Ok((us, _)) => {
                        let s = Arc::clone(&s);
                        tokio::spawn(async move {
                            let cv = s.cv.subscribe();
                            scope_connection(s, us, cv).await
                        });
                    }
                    Err(e) => {
                        panic!("SCOPE ERROR: accept: {:?}", e);
                    }
                }
            }
        });

        Ok(Server(s0))
    }

    /**
     * Submit a prompt message to all control clients and await a response from
     * one of them.
     */
    pub async fn wait_for(&self, msg: &str) -> String {
        /*
         * Create a broadcast subscriber to listen for a reply to our message.
         */
        let mut rxin = self.0.txin.subscribe();

        let id = {
            let mut q = self.0.queue.lock().unwrap();
            let id = q.next_id;
            q.out.push_back(messages::PromptOut {
                id,
                msg: format!("{} <press enter>", msg),
            });
            q.next_id += 1;
            id
        };
        self.0.cv.signal();

        /*
         * Take the first reply that matches the ID of the message we sent:
         */
        let pi = loop {
            let pi = rxin.recv().await.unwrap();
            if pi.id == id {
                break pi;
            }
        };

        /*
         * Prune our message from the queue.
         */
        {
            let mut q = self.0.queue.lock().unwrap();
            while let Some(pos) = q.out.iter().position(|po| po.id == id) {
                q.out.remove(pos);
            }
        }
        self.0.cv.signal();

        pi.reply
    }
}
