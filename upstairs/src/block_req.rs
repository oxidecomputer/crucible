// Copyright 2022 Oxide Computer Company
use super::*;
use tokio::sync::oneshot;

#[must_use]
#[derive(Debug)]
pub(crate) struct BlockRes<T = (), E = CrucibleError>(
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
    pub fn send_result(mut self, result: Result<T, E>) {
        // XXX this eats the result!
        let _ = self.0.take().expect("sender was populated").send(result);
    }

    /// Builds an empty `BlockRes`, for use in unit testing
    #[cfg(test)]
    pub fn dummy() -> Self {
        let (tx, _) = oneshot::channel();
        Self(Some(tx))
    }
}

impl<T, E> Drop for BlockRes<T, E> {
    fn drop(&mut self) {
        if self.0.is_some() {
            // During normal operation, we expect to reply to every BlockOp, so
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
pub(crate) struct BlockOpWaiter<T, E = CrucibleError> {
    recv: oneshot::Receiver<Result<T, E>>,
}

impl<T, E> BlockOpWaiter<T, E> {
    /// Create associated `BlockOpWaiter`/`BlockRes` pair
    pub fn pair() -> (Self, BlockRes<T, E>) {
        let (send, recv) = oneshot::channel();
        (Self { recv }, BlockRes(Some(send)))
    }

    /// Consume this BlockOpWaiter and wait on the message
    ///
    /// Returns `None` if the other side drops without a reply
    pub async fn wait_raw(self) -> Option<Result<T, E>> {
        match self.recv.await {
            Ok(reply) => Some(reply),
            Err(_) => None,
        }
    }
}

impl<T> BlockOpWaiter<T, CrucibleError> {
    /// Wait, translating disconnection into `RecvDisconnected`
    pub async fn wait(self) -> Result<T, CrucibleError> {
        self.wait_raw()
            .await
            .unwrap_or(Err(CrucibleError::RecvDisconnected))
    }
}

#[cfg(test)]
impl<T> BlockOpWaiter<T, CrucibleError> {
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
        let (brw, res) = BlockOpWaiter::pair();

        res.send_ok(());

        let reply = brw.wait().await;
        assert!(reply.is_ok());
    }

    #[tokio::test]
    async fn test_blockreq_and_blockreqwaiter_err() {
        let (brw, res) = BlockOpWaiter::<()>::pair();

        res.send_err(CrucibleError::UpstairsInactive);

        let reply = brw.wait().await;
        assert!(reply.is_err());
    }
}
