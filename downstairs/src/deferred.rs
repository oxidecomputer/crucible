use crucible_protocol::Message;

/// Result of a deferred `Message`
///
/// In most cases, this is simply the original `Message` (stored in
/// `DeferredMessage::Other`).
pub(crate) enum DeferredMessage {
    Other(Message),
}
