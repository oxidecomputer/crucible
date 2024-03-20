// Copyright 2022 Oxide Computer Company
use super::*;
use tokio::sync::oneshot;

/// Wrapper to contain a `BlockOp`
///
/// The `BlockReq` used to have more fields, but now it's down to just this one;
/// we can remove it at a later point.
#[derive(Debug)]
pub(crate) struct BlockReq {
    pub op: BlockOp,
}

#[must_use]
#[derive(Debug)]
pub(crate) struct BlockRes<T, E = CrucibleError>(
    Option<oneshot::Sender<Result<T, E>>>,
);

impl<T, E> BlockRes<T, E> {
    /// Consume this BlockRes and send Ok to the receiver
    pub fn send_ok(self, t: T) {
        self.send_result(Ok(t))
    }

    /// Consume this BlockRes and send an Err to the receiver
    pub fn send_err(self, e: E) {
        self.send_result(Err(e))
    }

    /// Consume this BlockRes and send a Result to the receiver
    pub(crate) fn send_result(mut self, result: Result<T, E>) {
        // XXX this eats the result!
        let _ = self.0.take().expect("sender was populated").send(result);
    }
}

impl<T, E> Drop for BlockRes<T, E> {
    fn drop(&mut self) {
        if self.0.is_some() {
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
 * sender stored in the `BlockOp`.
 */
#[must_use]
pub(crate) struct BlockReqWaiter<T, E = CrucibleError> {
    recv: oneshot::Receiver<Result<T, E>>,
}

impl<T, E> BlockReqWaiter<T, E> {
    /// Create associated `BlockReqWaiter`/`BlockRes` pair
    pub fn pair() -> (Self, BlockRes<T, E>) {
        let (send, recv) = oneshot::channel();
        (Self { recv }, BlockRes(Some(send)))
    }

    /// Consume this BlockReqWaiter and wait on the message
    ///
    /// If the other side of the oneshot drops without a reply, log an error
    /// and return `None`.
    pub async fn wait_raw(self, log: &Logger) -> Option<Result<T, E>> {
        match self.recv.await {
            Ok(reply) => Some(reply),
            Err(_) => {
                warn!(
                    log,
                    "BlockReqWaiter disconnected; \
                     this should only happen at exit"
                );
                None
            }
        }
    }
}

impl<T> BlockReqWaiter<T, CrucibleError> {
    /// Wait, translating disconnection into `RecvDisconnected`
    pub async fn wait(self, log: &Logger) -> Result<T, CrucibleError> {
        self.wait_raw(log)
            .await
            .unwrap_or(Err(CrucibleError::RecvDisconnected))
    }
}

#[cfg(test)]
impl<T> BlockReqWaiter<T, CrucibleError> {
    pub fn try_wait(&mut self) -> Option<Result<T, CrucibleError>> {
        match self.recv.try_recv() {
            Ok(reply) => Some(reply),
            Err(e) => match e {
                oneshot::error::TryRecvError::Empty => None,
                oneshot::error::TryRecvError::Closed => {
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
    async fn test_blockreq_and_blockreqwaiter() {
        let (brw, res) = BlockReqWaiter::pair();

        res.send_ok(());

        let reply = brw.wait(&crucible_common::build_logger()).await;
        assert!(reply.is_ok());
    }

    #[tokio::test]
    async fn test_blockreq_and_blockreqwaiter_err() {
        let (brw, res) = BlockReqWaiter::<()>::pair();

        res.send_err(CrucibleError::UpstairsInactive);

        let reply = brw.wait(&crucible_common::build_logger()).await;
        assert!(reply.is_err());
    }
}
