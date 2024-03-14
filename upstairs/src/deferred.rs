// Copyright 2024 Oxide Computer Company
//! Tools to defer encryption work to a separate thread pool
use std::sync::Arc;

use crate::{
    client::ConnectionId, upstairs::UpstairsConfig, BlockContext, BlockReq,
    BlockRes, ClientId, ImpactedBlocks, Message, SerializedWrite,
};
use bytes::{Bytes, BytesMut};
use crucible_common::{integrity_hash, CrucibleError, RegionDefinition};
use slog::{error, Logger};

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
    /// Raw data to be written, along with extra metadata
    ///
    /// This is equivalent to a pre-serialized `Vec<Write>`, but avoids
    /// superfluous memory copies.
    pub data: SerializedWrite,
    pub impacted_blocks: ImpactedBlocks,
    pub res: Option<BlockRes>,
    pub is_write_unwritten: bool,
}

impl DeferredWrite {
    pub fn run(self) -> Option<EncryptedWrite> {
        // Build up all of the Write operations, encrypting data here
        let byte_len: usize = self.ddef.block_size() as usize;
        let mut serialized = {
            let block_count = self.impacted_blocks.blocks(&self.ddef).len();
            // TODO I think is an overestimation?
            let bytes_per_block =
                byte_len + std::mem::size_of::<crucible_protocol::Write>();
            BytesMut::with_capacity(
                block_count * bytes_per_block + std::mem::size_of::<usize>(),
            )
        };

        // First, serialize the length of the Vec<Write>
        let num_blocks = self.impacted_blocks.blocks(&self.ddef).len();
        serialized.extend(bincode::serialize(&num_blocks).unwrap());

        // Metadata to store
        let mut eids = Vec::with_capacity(num_blocks);

        let mut cur_offset: usize = 0;
        for (eid, offset) in self.impacted_blocks.blocks(&self.ddef) {
            if eids.last().map(|e| *e != eid).unwrap_or(true) {
                eids.push(eid);
            }

            // Write the header for this section
            let header = bincode::serialize(&(eid, offset, byte_len)).unwrap();
            serialized.extend(header);

            // Copy over raw data, since we need exclusive ownership to mutate
            // it (doing in-place encryption).
            let pos = serialized.len();
            serialized.extend_from_slice(
                &self.data[cur_offset..(cur_offset + byte_len)],
            );

            let (encryption_context, hash) = if let Some(ctx) =
                &self.cfg.encryption_context
            {
                // Encrypt here
                let mut_data = &mut serialized[pos..];
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
                let hash = integrity_hash(&[&serialized[pos..]]);

                (None, hash)
            };

            // Write the trailing data for this chunk
            let trailer = bincode::serialize(&BlockContext {
                hash,
                encryption_context,
            })
            .unwrap();
            serialized.extend(trailer);

            cur_offset += byte_len;
        }

        let data = SerializedWrite {
            data: serialized.freeze(),
            num_blocks,
            io_size_bytes: self.data.len(),
            eids,
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
        let Message::ReadResponse { responses, .. } = &mut self.message else {
            panic!("invalid DeferredRead");
        };
        let mut hashes = vec![];

        if let Ok(rs) = responses {
            for r in rs.iter_mut() {
                let v = if let Some(ctx) = &self.cfg.encryption_context {
                    validate_encrypted_read_response(r, ctx, &self.log)
                } else {
                    validate_unencrypted_read_response(r, &self.log)
                };
                match v {
                    Ok(hash) => hashes.push(hash),
                    Err(e) => {
                        error!(self.log, "decryption failure: {e:?}");
                        *responses = Err(e);
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
