// Copyright 2022 Oxide Computer Company
use super::*;
use crate::backpressure::{BackpressureCounters, BackpressureGuard};
use tokio::sync::oneshot;

/**
 * Couple a BlockOp with a notifier for calling code. This uses a single-use
 * channel to send the result of a particular operation, and is meant to be
 * paired with a BlockReqWaiter.
 */
#[must_use]
#[derive(Debug)]
pub(crate) struct BlockReq {
    op: BlockOp,
    res: BlockRes,
}

impl BlockReq {
    /// Builds a new `BlockReq`, binding the backpressure handle to `res`
    pub fn new(
        op: BlockOp,
        mut res: BlockRes,
        bp: Arc<BackpressureCounters>,
    ) -> Self {
        res.bind(&op, bp);
        Self { op, res }
    }

    /// Builds a new BlockReq without any backpressure
    ///
    /// This is only allowed during unit tests!
    #[cfg(test)]
    pub fn new_without_backpressure(op: BlockOp, res: BlockRes) -> Self {
        Self { op, res }
    }

    /// Decomposes the `BlockReq` into its component pieces
    pub fn split(self) -> (BlockOp, BlockRes) {
        (self.op, self.res)
    }

    /// Returns a reference to the inner `BlockOp`
    pub fn op(&self) -> &BlockOp {
        &self.op
    }
}

#[must_use]
#[derive(Debug, PartialEq)]
pub(crate) struct BlockReqReply {
    /// We return the buffer (if passed in) so the host can reuse it
    pub buffer: Option<Buffer>,

    /// Actual result of the Crucible operation
    pub result: Result<(), CrucibleError>,
}

/// Handle to send a reply back to the guest
///
/// The handle includes both a reply oneshot and a backpressure guard, which
/// will update our backpressure values when the `BlockRes` is dropped.  If the
/// `BlockRes` reply is sent before the IO job is complete, then the
/// backpressure guard should be extracted with `take_backpressure_guard` and
/// bound to a longer-lived object (e.g. a `DownstairsIO`)
#[must_use]
#[derive(Debug)]
pub(crate) struct BlockRes {
    /// Reply handle to the upstairs
    tx: Option<oneshot::Sender<BlockReqReply>>,

    /// Handle to track IO and jobs in flight for upstairs backpressure
    backpressure_guard: Option<BackpressureGuard>,
}

impl BlockRes {
    /// Consume this BlockRes and send Ok to the receiver
    pub fn send_ok(self) {
        self.send_result(None, Ok(()))
    }

    /// Consume this BlockRes and send Ok (with data) to the receiver
    pub fn send_ok_with_buffer(self, buffer: Buffer) {
        self.send_result(Some(buffer), Ok(()))
    }

    /// Consume this BlockRes and send an Err to the receiver
    pub fn send_err(self, e: CrucibleError) {
        self.send_result(None, Err(e))
    }

    /// Consume this BlockRes and send Err (with data) to the receiver
    pub fn send_err_with_buffer(self, buffer: Buffer, e: CrucibleError) {
        self.send_result(Some(buffer), Err(e))
    }

    /// Consume this BlockRes and send a Result to the receiver
    fn send_result(
        mut self,
        buffer: Option<Buffer>,
        result: Result<(), CrucibleError>,
    ) {
        // XXX this eats the result!
        let _ = self
            .tx
            .take()
            .expect("sender was populated")
            .send(BlockReqReply { buffer, result });
    }

    /// Add this job to the given backpressure counters
    ///
    /// The backpressure counters are stored locally and decremented when this
    /// `BlockRes` is dropped, so we automatically keep track of outstanding
    /// work without human intervention.
    fn bind(&mut self, op: &BlockOp, bp: Arc<BackpressureCounters>) {
        // Count this job as 1 job and some number of bytes
        //
        // (we only count write bytes because only writes return immediately)
        assert!(self.backpressure_guard.is_none());
        let bytes = match op {
            BlockOp::Write { data, .. } => data.len() as u64,
            _ => 0,
        };
        self.backpressure_guard = Some(BackpressureGuard::new(bp, bytes));
    }

    /// Steals the backpressure guard
    ///
    /// This effectively extends how long this `BlockReq` will exert
    /// backpressure.  It is used when we reply to the `BlockReq` immediately
    /// (e.g. for a write), but the IO operation lives and should be counted for
    /// longer.
    pub fn take_backpressure_guard(&mut self) -> Option<BackpressureGuard> {
        self.backpressure_guard.take()
    }
}

impl Drop for BlockRes {
    fn drop(&mut self) {
        if self.tx.is_some() {
            // During normal operation, we expect to reply to every BlockReq, so
            // we'll fire a DTrace probe here.
            cdt::up__block__req__dropped!();
        }
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
    recv: oneshot::Receiver<BlockReqReply>,
}

impl BlockReqWaiter {
    /// Create associated `BlockReqWaiter`/`BlockRes` pair
    pub fn pair() -> (BlockReqWaiter, BlockRes) {
        let (send, recv) = oneshot::channel();
        (
            Self { recv },
            BlockRes {
                tx: Some(send),
                backpressure_guard: None,
            },
        )
    }

    /// Consume this BlockReqWaiter and wait on the message
    ///
    /// If the other side of the oneshot drops without a reply, log an error
    pub async fn wait(self, log: &Logger) -> BlockReqReply {
        match self.recv.await {
            Ok(reply) => reply,
            Err(_) => {
                warn!(
                    log,
                    "BlockReqWaiter disconnected; \
                     this should only happen at exit"
                );

                // The Sender dropped without sending anything, the Buffer is
                // gone in this case.
                BlockReqReply {
                    buffer: None,
                    result: Err(CrucibleError::RecvDisconnected),
                }
            }
        }
    }

    #[cfg(test)]
    pub fn try_wait(&mut self) -> Option<BlockReqReply> {
        match self.recv.try_recv() {
            Ok(reply) => Some(reply),
            Err(e) => match e {
                oneshot::error::TryRecvError::Empty => None,
                oneshot::error::TryRecvError::Closed => Some(BlockReqReply {
                    buffer: None,
                    result: Err(CrucibleError::RecvDisconnected),
                }),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_blockreq_and_blockreqwaiter() {
        let (brw, res) = BlockReqWaiter::pair();

        res.send_ok();

        let reply = brw.wait(&crucible_common::build_logger()).await;
        assert!(reply.buffer.is_none());
        reply.result.unwrap();
    }

    #[tokio::test]
    async fn test_blockreq_and_blockreqwaiter_with_buffer() {
        let (brw, res) = BlockReqWaiter::pair();

        res.send_ok_with_buffer(Buffer::with_capacity(0, 512));

        let reply = brw.wait(&crucible_common::build_logger()).await;
        assert!(reply.buffer.is_some());
        reply.result.unwrap();
    }

    #[tokio::test]
    async fn test_blockreq_and_blockreqwaiter_err() {
        let (brw, res) = BlockReqWaiter::pair();

        res.send_err(CrucibleError::UpstairsInactive);

        let reply = brw.wait(&crucible_common::build_logger()).await;
        assert!(reply.buffer.is_none());
        assert!(reply.result.is_err());
    }

    #[tokio::test]
    async fn test_blockreq_and_blockreqwaiter_err_with_buffer() {
        let (brw, res) = BlockReqWaiter::pair();

        res.send_err_with_buffer(
            Buffer::with_capacity(0, 512),
            CrucibleError::UpstairsInactive,
        );

        let reply = brw.wait(&crucible_common::build_logger()).await;
        assert!(reply.buffer.is_some());
        assert!(reply.result.is_err());
    }
}
