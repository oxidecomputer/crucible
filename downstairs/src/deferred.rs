use crucible_protocol::Message;

/// Result of a deferred `Message`
///
/// In most cases, this is simply the original `Message` (stored in
/// `DeferredMessage::Other`).
pub(crate) enum DeferredMessage {
    Other(Message),
}

/// TODO
pub(crate) struct PrecomputedWrite;

impl DeferredMessage {
    /// Returns a reference to the original message
    pub(crate) fn into_parts(self) -> (Message, Option<PrecomputedWrite>) {
        match self {
            DeferredMessage::Other(msg) => (msg, None),
        }
    }
}
