// Copyright 2024 Oxide Computer Company
//! Tools to defer encryption work to a separate thread pool
use std::sync::Arc;

use crate::{
    client::ConnectionId, upstairs::UpstairsConfig, BlockContext, BlockReq,
    BlockRes, ClientId, ImpactedBlocks, Message,
};
use bytes::BytesMut;
use crucible_common::{integrity_hash, CrucibleError, RegionDefinition};
use crucible_protocol::{RawWrite, WriteBlockMetadata};
use futures::{
    future::{ready, Either, Ready},
    stream::FuturesOrdered,
    StreamExt,
};
use slog::{error, Logger};
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

    /// Stores a new future in the queue, marking it as non-empty
    pub fn push_immediate(&mut self, t: T) {
        self.push_back(Either::Left(ready(Ok(t))));
    }

    /// Stores a new pending oneshot in the queue, returning the sender
    pub fn push_oneshot(&mut self) -> oneshot::Sender<T> {
        let (rx, tx) = oneshot::channel();
        self.push_back(Either::Right(tx));
        rx
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
    pub data: BytesMut,
    pub res: Option<BlockRes<()>>,
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
    /// An `RawWrite` containing our encrypted data
    pub data: RawWrite,
    pub impacted_blocks: ImpactedBlocks,
    pub res: Option<BlockRes<()>>,
    pub is_write_unwritten: bool,
}

impl DeferredWrite {
    pub fn run(mut self) -> Option<EncryptedWrite> {
        let num_blocks = self.impacted_blocks.blocks(&self.ddef).len();
        let mut blocks = Vec::with_capacity(num_blocks);
        let block_size = self.ddef.block_size() as usize;

        // In-place encryption into `self.data`
        let mut pos: usize = 0;
        for (eid, offset) in self.impacted_blocks.blocks(&self.ddef) {
            let (encryption_context, hash) = if let Some(ctx) =
                &self.cfg.encryption_context
            {
                // Encrypt here
                let mut_data = &mut self.data[pos..][..block_size];
                let (nonce, tag, hash) = match ctx.encrypt_in_place(mut_data) {
                    Ok(v) => v,
                    Err(e) => {
                        if let Some(res) = self.res {
                            res.send_err(CrucibleError::EncryptionError(
                                e.to_string(),
                            ));
                        }
                        return None;
                    }
                };

                (
                    Some(crucible_protocol::EncryptionContext {
                        nonce: nonce.into(),
                        tag: tag.into(),
                    }),
                    hash,
                )
            } else {
                // Unencrypted
                let hash = integrity_hash(&[&self.data[pos..][..block_size]]);

                (None, hash)
            };
            blocks.push(WriteBlockMetadata {
                eid,
                offset,
                block_context: BlockContext {
                    hash,
                    encryption_context,
                },
            });
            pos += block_size;
        }

        let data = RawWrite {
            blocks,
            data: self.data,
        };

        Some(EncryptedWrite {
            data,
            impacted_blocks: self.impacted_blocks,
            res: self.res,
            is_write_unwritten: self.is_write_unwritten,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct DeferredMessage {
    pub message: Message,

    /// If this was a `ReadResponse`, then the hashes are stored here
    pub hashes: Vec<Option<u64>>,

    pub client_id: ClientId,

    /// See `DeferredRead::connection_id`
    pub connection_id: ConnectionId,
}

/// Standalone data structure which can perform decryption
pub(crate) struct DeferredRead {
    /// Message, which must be a `ReadResponse`
    pub message: Message,

    /// Unique ID for this particular connection to the downstairs
    ///
    /// This is needed because -- if read decryption is deferred -- it may
    /// complete after we have disconnected from the client, which would make
    /// handling the decrypted value incorrect (because it may have been skipped
    /// or re-sent).
    pub connection_id: ConnectionId,

    pub client_id: ClientId,
    pub cfg: Arc<UpstairsConfig>,
    pub log: Logger,
}

impl DeferredRead {
    /// Consume the `DeferredRead` and perform decryption
    ///
    /// If decryption fails, then the resulting `Message` has an error in the
    /// `responses` field, and `hashes` is empty.
    pub fn run(mut self) -> DeferredMessage {
        use crate::client::{
            validate_encrypted_read_response,
            validate_unencrypted_read_response,
        };
        let Message::ReadResponse { header, data } = &mut self.message else {
            panic!("invalid DeferredRead");
        };
        let mut hashes = vec![];

        if let Ok(rs) = header.blocks.as_mut() {
            assert_eq!(data.len() % rs.len(), 0);
            let block_size = data.len() / rs.len();
            for (i, r) in rs.iter_mut().enumerate() {
                let v = if let Some(ctx) = &self.cfg.encryption_context {
                    validate_encrypted_read_response(
                        &mut r.block_contexts,
                        &mut data[i * block_size..][..block_size],
                        ctx,
                        &self.log,
                    )
                } else {
                    validate_unencrypted_read_response(
                        &mut r.block_contexts,
                        &mut data[i * block_size..][..block_size],
                        &self.log,
                    )
                };
                match v {
                    Ok(hash) => hashes.push(hash),
                    Err(e) => {
                        error!(self.log, "decryption failure: {e:?}");
                        header.blocks = Err(e);
                        hashes.clear();
                        break;
                    }
                }
            }
        }

        DeferredMessage {
            client_id: self.client_id,
            message: self.message,
            connection_id: self.connection_id,
            hashes,
        }
    }
}
