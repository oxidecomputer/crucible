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
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PrecomputedWrite {}

impl PrecomputedWrite {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        PrecomputedWrite {}
    }

    /// Precomputes relevant data from a set of writes
    pub(crate) fn from_writes(_writes: &[crucible_protocol::Write]) -> Self {
        // Right now, we don't precompute anything
        PrecomputedWrite {}
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
