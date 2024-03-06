// Copyright 2023 Oxide Computer Company
use crucible_common::{integrity_hash, CrucibleError};
use crucible_protocol::Message;

/// Result of a deferred `Message`
///
/// In most cases, this is simply the original `Message` (stored in
/// `DeferredMessage::Other`).
pub(crate) enum DeferredMessage {
    Write(Message, PrecomputedWrite),
    Other(Message),
}

/// Data needed to perform a write, which can be computed off-thread
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PrecomputedWrite {
    /// Checks whether incoming hashes are valid
    pub validate_hashes_result: Result<(), CrucibleError>,
}

impl PrecomputedWrite {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        PrecomputedWrite {
            validate_hashes_result: Ok(()),
        }
    }

    /// Precomputes relevant data from a set of writes
    pub(crate) fn from_writes(writes: &[crucible_protocol::Write]) -> Self {
        let validate_hashes_result = Self::validate_hashes(writes);
        PrecomputedWrite {
            validate_hashes_result,
        }
    }

    fn validate_hashes(
        writes: &[crucible_protocol::Write],
    ) -> Result<(), CrucibleError> {
        for write in writes {
            let computed_hash = if let Some(encryption_context) =
                &write.block_context.encryption_context
            {
                integrity_hash(&[
                    &encryption_context.nonce[..],
                    &encryption_context.tag[..],
                    &write.data[..],
                ])
            } else {
                integrity_hash(&[&write.data[..]])
            };

            if computed_hash != write.block_context.hash {
                // TODO: print out the extent and block where this failed!!
                return Err(CrucibleError::HashMismatch);
            }
        }

        Ok(())
    }
}

impl DeferredMessage {
    /// Returns a reference to the original message
    pub(crate) fn into_parts(self) -> (Message, Option<PrecomputedWrite>) {
        match self {
            DeferredMessage::Write(msg, pre) => (msg, Some(pre)),
            DeferredMessage::Other(msg) => (msg, None),
        }
    }
}
