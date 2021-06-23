use std::io::{BufRead, Write};
use std::sync::Mutex;

struct InteractPrompt {
    id: u64,
    msg: String,
}

struct InteractInner {
    next_id: u64,
    tx: std::sync::mpsc::Sender<InteractPrompt>,
}

pub struct Interact {
    inner: Mutex<InteractInner>,
    brx: tokio::sync::watch::Receiver<u64>,
}

impl Interact {
    pub fn new() -> Interact {
        /*
         * Print messages to the console:
         */
        let (tx, rx) = std::sync::mpsc::channel::<InteractPrompt>();

        /*
         * Receive replies from the console I/O thread:
         */
        let (btx, brx) = tokio::sync::watch::channel(0);

        /*
         * Create a thread to manage console I/O:
         */
        std::thread::spawn(move || {
            let stdin = std::io::stdin();
            let bufin = std::io::BufReader::new(stdin.lock());
            let mut lines = bufin.lines();

            loop {
                let ip = rx.recv().unwrap();

                {
                    let stdout = std::io::stdout();
                    let mut stdout = stdout.lock();
                    stdout.write_all(ip.msg.as_bytes()).unwrap();
                    stdout.flush().unwrap();
                }

                /*
                 * Wait for a newline.
                 */
                lines.next().unwrap();
                btx.send(ip.id).unwrap();
            }
        });

        Interact {
            inner: Mutex::new(InteractInner { next_id: 1, tx }),
            brx,
        }
    }

    pub async fn wait_for(&self, msg: &str) {
        /*
         * Clone the watch receiver first so that we are guaranteed not to miss
         * the wakeup.
         */
        let mut brx = self.brx.clone();

        let id = {
            let mut inner = self.inner.lock().unwrap();
            let id = inner.next_id;
            inner.next_id += 1;

            /*
             * Pass the message we wish to display on to the stdio thread.
             */
            let ip = InteractPrompt {
                id,
                msg: format!("\x1b[7m#### {} #### <press enter>\x1b[0m\n", msg),
            };
            inner.tx.send(ip).unwrap();

            id
        };

        loop {
            brx.changed().await;
            if *brx.borrow() >= id {
                /*
                 * Our message has been acknowledged.
                 */
                return;
            }
        }
    }
}
