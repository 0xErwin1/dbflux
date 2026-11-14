//! Messages between the application and the connected MCP agent.
//!
//! The bridge keeps one bounded, ordered log. Both sides append to it and read
//! it by message id, so a reader that remembers the last id it saw never misses
//! or repeats a message while that message is retained.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Messages the bridge retains. Older messages are dropped first.
pub const MAX_RETAINED_MESSAGES: usize = 256;
/// Messages one side may have waiting, unread by the other side, before the
/// bridge refuses new ones from it.
pub const MAX_UNREAD_MESSAGES: usize = 64;
/// Maximum UTF-8 bytes of message text.
pub const MAX_MESSAGE_TEXT_BYTES: usize = 16 * 1024;
/// Maximum serialized bytes of structured message data.
pub const MAX_MESSAGE_DATA_BYTES: usize = 16 * 1024;
/// Maximum length of a message kind.
pub const MAX_MESSAGE_KIND_BYTES: usize = 64;
/// Maximum messages returned by one read.
pub const MAX_MESSAGE_PAGE: usize = 128;

/// Which side wrote a message.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MessageSender {
    /// The application, for example its chat box.
    App,
    /// The connected MCP agent.
    Agent,
}

impl MessageSender {
    /// The other side.
    #[must_use]
    pub const fn peer(self) -> Self {
        match self {
            Self::App => Self::Agent,
            Self::Agent => Self::App,
        }
    }
}

/// A message to append to the log.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct NewMessage {
    /// Message text.
    pub text: String,
    /// Optional lowercase kind, such as `chat`, `event`, or `status`. Defaults to `chat`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Id of the message this one answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<u64>,
    /// Optional structured payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl NewMessage {
    /// A plain chat message.
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            kind: None,
            reply_to: None,
            data: None,
        }
    }

    /// Set the kind.
    #[must_use]
    pub fn with_kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }

    /// Answer the message with this id.
    #[must_use]
    pub fn reply_to(mut self, id: u64) -> Self {
        self.reply_to = Some(id);
        self
    }

    /// Attach structured data.
    #[must_use]
    pub fn with_data(mut self, data: serde_json::Value) -> Self {
        self.data = Some(data);
        self
    }

    /// Check every bound the bridge enforces.
    ///
    /// # Errors
    ///
    /// Returns a static explanation of the first violated bound.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.text.len() > MAX_MESSAGE_TEXT_BYTES {
            return Err("message text exceeds 16 KiB");
        }
        if self.text.trim().is_empty() && self.data.is_none() {
            return Err("a message needs text or data");
        }
        if self.kind.as_ref().is_some_and(|kind| {
            kind.is_empty()
                || kind.len() > MAX_MESSAGE_KIND_BYTES
                || !kind.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'_' | b'-' | b'.')
                })
        }) {
            return Err("message kind must be 1-64 lowercase letters, digits, '.', '_' or '-'");
        }
        if self.reply_to == Some(0) {
            return Err("reply_to must name a message id");
        }
        if let Some(data) = &self.data
            && serde_json::to_vec(data).map_or(true, |bytes| bytes.len() > MAX_MESSAGE_DATA_BYTES)
        {
            return Err("message data exceeds 16 KiB");
        }
        Ok(())
    }
}

/// One message in the log.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Message {
    /// Monotonic id, starting at 1. Ids are never reused within a bridge lifetime.
    pub id: u64,
    /// Which side wrote it.
    pub from: MessageSender,
    /// Kind, `chat` unless the writer chose another.
    pub kind: String,
    /// Message text.
    pub text: String,
    /// Id of the message this one answers.
    pub reply_to: Option<u64>,
    /// Structured payload.
    pub data: Option<serde_json::Value>,
    /// Milliseconds since the Unix epoch when the bridge accepted it.
    pub timestamp_ms: u64,
}

/// A page of messages read from the log.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MessagePage {
    /// Messages after the requested id, oldest first.
    pub messages: Vec<Message>,
    /// Id of the newest message in the log, or 0 when it is empty. Pass the id of
    /// the last message you processed as `after` on the next read.
    pub latest_id: u64,
    /// Whether messages after the requested id were dropped before this read,
    /// because the log only retains the newest 256.
    pub truncated: bool,
    /// Whether more matching messages follow this page.
    pub has_more: bool,
    /// Messages from the app the agent has not read yet.
    pub unread_from_app: u32,
    /// Messages from the agent the app has not read yet.
    pub unread_from_agent: u32,
}

#[cfg(test)]
mod tests {
    use super::{MAX_MESSAGE_TEXT_BYTES, MessageSender, NewMessage};

    #[test]
    fn validation_enforces_bounds() {
        assert_eq!(NewMessage::text("hi").validate(), Ok(()));
        assert!(NewMessage::text(" ").validate().is_err());
        assert!(
            NewMessage::text("x".repeat(MAX_MESSAGE_TEXT_BYTES + 1))
                .validate()
                .is_err()
        );
        assert!(NewMessage::text("hi").with_kind("Chat").validate().is_err());
        assert!(NewMessage::text("hi").reply_to(0).validate().is_err());
        assert_eq!(
            NewMessage::text("")
                .with_data(serde_json::json!({ "selection": ["a"] }))
                .validate(),
            Ok(())
        );
    }

    #[test]
    fn peers_are_symmetric() {
        assert_eq!(MessageSender::App.peer(), MessageSender::Agent);
        assert_eq!(MessageSender::Agent.peer().peer(), MessageSender::Agent);
    }
}
