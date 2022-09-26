// Copyright 2022 Oxide Computer Company
use super::*;

/**
 * Couple a BlockOp with a notifier for calling code. This uses a single-use
 * channel to send the result of a particular operation, and is meant to be
 * paired with a BlockReqWaiter.
 */
#[must_use]
#[derive(Debug)]
pub(crate) struct BlockReq {
    pub op: BlockOp,
    sender: mpsc::Sender<Result<(), CrucibleError>>,
}

impl BlockReq {
    pub fn new(
        op: BlockOp,
        sender: mpsc::Sender<Result<(), CrucibleError>>,
    ) -> BlockReq {
        Self { op, sender }
    }

    /// Return a copy of the block op
    pub fn op(&self) -> BlockOp {
        self.op.clone()
    }

    /// Consume this BlockReq and send Ok to the receiver
    pub async fn send_ok(self) {
        self.send_result(Ok(())).await;
    }

    /// Consume this BlockReq and send an Err to the receiver
    pub async fn send_err(self, e: CrucibleError) {
        self.send_result(Err(e)).await;
    }

    /// Consume this BlockReq and send a Result to the receiver
    pub async fn send_result(self, r: Result<(), CrucibleError>) {
        // XXX this eats the result!
        let _ = self.sender.send(r).await;
    }
}

/**
 * When BlockOps are sent to a guest, the calling function receives a waiter
 * that it can block on. This uses a single-use channel to receive the
 * result of a particular operation, and is meant to be paired with a
 * BlockReq.
 */
#[must_use]
pub(crate) struct BlockReqWaiter {
    recv: mpsc::Receiver<Result<(), CrucibleError>>,
}

impl BlockReqWaiter {
    pub fn new(
        recv: mpsc::Receiver<Result<(), CrucibleError>>,
    ) -> BlockReqWaiter {
        Self { recv }
    }

    /// Consume this BlockReqWaiter and wait on the message
    pub async fn wait(mut self) -> Result<(), CrucibleError> {
        match self.recv.recv().await {
            Some(v) => v,
            None => crucible_bail!(RecvDisconnected),
        }
    }

    #[allow(dead_code)]
    pub async fn try_wait(&mut self) -> Option<Result<(), CrucibleError>> {
        match self.recv.try_recv() {
            Ok(v) => Some(v),
            Err(e) => match e {
                mpsc::error::TryRecvError::Empty => None,
                mpsc::error::TryRecvError::Disconnected => {
                    Some(Err(CrucibleError::RecvDisconnected))
                }
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_blockreqwaiter_send() {
        let (send, recv) = mpsc::channel(1);
        let brw = BlockReqWaiter::new(recv);

        send.send(Ok(())).await.unwrap();

        brw.wait().await.unwrap();
    }

    #[tokio::test]
    async fn test_blockreq_and_blockreqwaiter() {
        let (send, recv) = mpsc::channel(1);

        let op = BlockOp::Flush {
            snapshot_details: None,
        };
        let br = BlockReq::new(op, send);
        let brw = BlockReqWaiter::new(recv);

        br.send_ok().await;

        brw.wait().await.unwrap();
    }

    #[tokio::test]
    async fn test_blockreq_and_blockreqwaiter_err() {
        let (send, recv) = mpsc::channel(1);

        let op = BlockOp::Flush {
            snapshot_details: None,
        };
        let br = BlockReq::new(op, send);
        let brw = BlockReqWaiter::new(recv);

        br.send_err(CrucibleError::UpstairsInactive).await;

        assert!(brw.wait().await.is_err());
    }
}
