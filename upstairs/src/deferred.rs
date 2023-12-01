// Copyright 2023 Oxide Computer Company
//! Tools to defer encryption work to a separate thread pool
use std::sync::Arc;

use crate::{
    upstairs::UpstairsConfig, BlockContext, BlockReq, BlockRes, ImpactedBlocks,
};
use bytes::Bytes;
use crucible_common::{integrity_hash, CrucibleError, RegionDefinition};
use futures::{
    future::{Either, Ready},
    stream::FuturesOrdered,
    StreamExt,
};
use tokio::sync::oneshot;

/// Future stored in a [`DeferredQueue`]
///
/// This is either an immediately-ready `T` or a oneshot channel which returns a
/// `T` when an off-task job finishes.
type DeferredQueueFuture<T> =
    Either<Ready<Result<T, oneshot::error::RecvError>>, oneshot::Receiver<T>>;

/// A `DeferredQueue` stores pending work (optionally executed off-task)
pub(crate) struct DeferredQueue<T> {
    /// Ordered stream of deferred futures
    stream: FuturesOrdered<DeferredQueueFuture<T>>,

    /// Stores whether it is known that there are no futures in `self.stream`
    ///
    /// This is tracked separately because `FuturesOrdered::next` will
    /// immediately return `None` if the queue is empty; we don't want that when
    /// it's one of many options in a `tokio::select!`.
    empty: bool,
}

impl<T> DeferredQueue<T> {
    /// Build a new empty `FuturesOrdered`
    pub fn new() -> Self {
        Self {
            stream: FuturesOrdered::new(),
            empty: true,
        }
    }

    /// Stores a new future in the queue, marking it as non-empty
    pub fn push_back(&mut self, f: DeferredQueueFuture<T>) {
        self.stream.push_back(f);
        self.empty = false;
    }

    /// Returns the next future from the queue
    ///
    /// If the future is `None`, then the queue is marked as empty
    ///
    /// This function is cancel safe: if a result is taken from the internal
    /// `FuturesOrdered`, then it guaranteed to be returned.
    pub async fn next(&mut self) -> Option<T> {
        // Early exit if we know the stream is empty
        if self.empty {
            return None;
        }

        // Cancel-safety: there can't be any yield points after this!
        let t = self.stream.next().await;
        self.empty |= t.is_none();

        // The oneshot is managed by a worker thread, which should never be
        // dropped, so we don't expect the oneshot
        t.map(|t| t.expect("oneshot failed"))
    }

    /// Check whether the queue is known to be empty
    ///
    /// It is possible for this to return `false` if the queue is actually
    /// empty; in that case, a subsequent call to `next()` will return `None`
    /// and *later* calls to `is_empty()` will return `true`.
    pub fn is_empty(&self) -> bool {
        self.empty
    }
}

////////////////////////////////////////////////////////////////////////////////

/// All of the information needed to encrypt a write operation
///
/// The `DeferredWrite` is standalone so that it can either be executed locally
/// or in a separate worker thread.
pub(crate) struct DeferredWrite {
    pub ddef: RegionDefinition,
    pub impacted_blocks: ImpactedBlocks,
    pub data: Bytes,
    pub res: Option<BlockRes>,
    pub is_write_unwritten: bool,
    pub cfg: Arc<UpstairsConfig>,
}

/// Result of a deferred `BlockReq`
///
/// In most cases, this is simply the original `BlockReq` (stored in
/// `DeferredBlockReq::Other`).  The exception is `BlockReq::Write` and
/// `BlockReq::WriteUnwritten`, which require encryption; in these cases,
/// encryption is done off-thread and the result is a `DeferredBlockReq::Write`.
#[derive(Debug)]
pub(crate) enum DeferredBlockReq {
    Write(EncryptedWrite),
    Other(BlockReq),
}

#[derive(Debug)]
pub(crate) struct EncryptedWrite {
    pub writes: Vec<crucible_protocol::Write>,
    pub impacted_blocks: ImpactedBlocks,
    pub res: Option<BlockRes>,
    pub is_write_unwritten: bool,
}

impl DeferredWrite {
    pub fn run(self) -> Option<EncryptedWrite> {
        // Build up all of the Write operations, encrypting data here
        let mut writes: Vec<crucible_protocol::Write> =
            Vec::with_capacity(self.impacted_blocks.len(&self.ddef));

        let mut cur_offset: usize = 0;
        let byte_len: usize = self.ddef.block_size() as usize;
        for (eid, offset) in self.impacted_blocks.blocks(&self.ddef) {
            let (sub_data, encryption_context, hash) = if let Some(context) =
                &self.cfg.encryption_context
            {
                // Encrypt here
                let mut mut_data = self
                    .data
                    .slice(cur_offset..(cur_offset + byte_len))
                    .to_vec();

                let (nonce, tag, hash) =
                    match context.encrypt_in_place(&mut mut_data[..]) {
                        Err(e) => {
                            if let Some(res) = self.res {
                                res.send_err(CrucibleError::EncryptionError(
                                    e.to_string(),
                                ));
                            }
                            return None;
                        }

                        Ok(v) => v,
                    };

                (
                    Bytes::copy_from_slice(&mut_data),
                    Some(crucible_protocol::EncryptionContext {
                        nonce: nonce.into(),
                        tag: tag.into(),
                    }),
                    hash,
                )
            } else {
                // Unencrypted
                let sub_data =
                    self.data.slice(cur_offset..(cur_offset + byte_len));
                let hash = integrity_hash(&[&sub_data[..]]);

                (sub_data, None, hash)
            };

            writes.push(crucible_protocol::Write {
                eid,
                offset,
                data: sub_data,
                block_context: BlockContext {
                    hash,
                    encryption_context,
                },
            });

            cur_offset += byte_len;
        }
        Some(EncryptedWrite {
            writes,
            impacted_blocks: self.impacted_blocks,
            res: self.res,
            is_write_unwritten: self.is_write_unwritten,
        })
    }
}
