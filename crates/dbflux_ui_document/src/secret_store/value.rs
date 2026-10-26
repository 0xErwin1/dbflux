//! The retrieved value of one secret, how it is presented, and the audit
//! event every retrieval records.
//!
//! A value only exists here after an explicit Retrieve value action. It is
//! held as `secrecy` types, so it is redacted from `Debug` output and zeroed
//! when dropped, and it is dropped when the selection changes, the listing is
//! refreshed or the tab closes.

use dbflux_core::chrono::{DateTime, Local, Utc};
use dbflux_core::observability::{EventCategory, EventOutcome, EventRecord, EventSeverity};
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{SecretPayload, SecretValue};
use uuid::Uuid;

/// Audit action of a value retrieval that returned a value.
pub const VALUE_RETRIEVED_ACTION: &str = "secret_value_retrieved";

/// Audit action of a value retrieval that failed.
pub const VALUE_RETRIEVE_FAILED_ACTION: &str = "secret_value_retrieve_failed";

/// How a retrieved value is shown.
#[derive(Debug)]
pub enum ValueContent {
    /// A one-level JSON object with scalar values. `text` keeps the original
    /// JSON for the plaintext view and Copy JSON.
    KeyValue {
        pairs: Vec<(String, SecretString)>,
        text: SecretString,
    },
    /// Any other text, including nested JSON and arrays.
    Plaintext(SecretString),
    /// A binary value, shown as standard base64. Read only.
    Binary { base64: SecretString, size: usize },
}

impl ValueContent {
    /// Decides the presentation of a value the store returned.
    pub fn from_value(value: &SecretValue) -> Self {
        match &value.payload {
            SecretPayload::Binary(bytes) => {
                let bytes = bytes.expose_secret();

                Self::Binary {
                    base64: SecretString::from(encode_base64(bytes)),
                    size: bytes.len(),
                }
            }
            SecretPayload::Text(text) => match value.key_value_pairs() {
                Some(pairs) => Self::KeyValue {
                    pairs,
                    text: SecretString::from(text.expose_secret().to_string()),
                },
                None => Self::Plaintext(SecretString::from(text.expose_secret().to_string())),
            },
        }
    }

    pub fn has_key_value_view(&self) -> bool {
        matches!(self, Self::KeyValue { .. })
    }

    pub fn is_binary(&self) -> bool {
        matches!(self, Self::Binary { .. })
    }

    /// The whole value as text: the JSON of a key/value secret, the text of
    /// a plaintext one, the base64 of a binary one.
    pub fn full_text(&self) -> &SecretString {
        match self {
            Self::KeyValue { text, .. } => text,
            Self::Plaintext(text) => text,
            Self::Binary { base64, .. } => base64,
        }
    }
}

/// Which view of a key/value secret is shown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueView {
    KeyValue,
    Plaintext,
}

/// A value fetched by an explicit retrieval.
#[derive(Debug)]
pub struct RetrievedValue {
    pub version: Option<String>,
    pub content: ValueContent,
    pub retrieved_at: DateTime<Local>,
    pub view: ValueView,
    /// Which fields are drawn in plain text: one per key/value pair, or a
    /// single entry for the plaintext and binary views.
    revealed: Vec<bool>,
}

impl RetrievedValue {
    pub fn new(value: &SecretValue) -> Self {
        let content = ValueContent::from_value(value);

        let view = if content.has_key_value_view() {
            ValueView::KeyValue
        } else {
            ValueView::Plaintext
        };

        let mut retrieved = Self {
            version: value.version.clone(),
            content,
            retrieved_at: Local::now(),
            view,
            revealed: Vec::new(),
        };
        retrieved.revealed = vec![false; retrieved.field_count()];
        retrieved
    }

    /// The fields the current view draws, in order. Every field is masked
    /// until revealed.
    pub fn fields(&self) -> Vec<(Option<&str>, &SecretString)> {
        match (&self.content, self.view) {
            (ValueContent::KeyValue { pairs, .. }, ValueView::KeyValue) => pairs
                .iter()
                .map(|(key, value)| (Some(key.as_str()), value))
                .collect(),
            (content, _) => vec![(None, content.full_text())],
        }
    }

    pub fn field_count(&self) -> usize {
        match (&self.content, self.view) {
            (ValueContent::KeyValue { pairs, .. }, ValueView::KeyValue) => pairs.len(),
            _ => 1,
        }
    }

    pub fn is_revealed(&self, index: usize) -> bool {
        self.revealed.get(index).copied().unwrap_or(false)
    }

    pub fn all_revealed(&self) -> bool {
        self.revealed.iter().all(|revealed| *revealed)
    }

    pub fn toggle_reveal(&mut self, index: usize) {
        if let Some(revealed) = self.revealed.get_mut(index) {
            *revealed = !*revealed;
        }
    }

    /// Reveals every field when one is still masked, otherwise masks them
    /// all.
    pub fn toggle_reveal_all(&mut self) {
        let reveal = !self.revealed.iter().all(|revealed| *revealed);
        self.revealed
            .iter_mut()
            .for_each(|revealed| *revealed = reveal);
    }

    pub fn hide_all(&mut self) {
        self.revealed
            .iter_mut()
            .for_each(|revealed| *revealed = false);
    }

    /// Switches between the key/value and plaintext views. A value without
    /// a key/value view stays in plaintext. Every field is masked again.
    pub fn set_view(&mut self, view: ValueView) {
        if view == ValueView::KeyValue && !self.content.has_key_value_view() {
            return;
        }

        self.view = view;
        self.revealed = vec![false; self.field_count()];
    }
}

/// A retrieval the store refused or could not complete.
#[derive(Clone, Debug)]
pub struct ValueFailure {
    pub message: String,
    pub correlation_id: Uuid,
    /// The store refused to decrypt the value (`DbError::PermissionDenied`).
    pub decrypt_denied: bool,
}

/// The value section of the selected secret.
#[derive(Debug, Default)]
pub enum ValueState {
    #[default]
    NotLoaded,
    Loading,
    Loaded(Box<RetrievedValue>),
    Failed(ValueFailure),
}

/// Who and what a retrieval was for, as the audit event records it.
pub struct RetrievalTarget<'a> {
    pub profile_id: Uuid,
    pub driver_id: &'a str,
    pub store_id: &'a str,
    pub store_label: &'a str,
    pub name: &'a str,
}

/// The audit event of one value retrieval.
///
/// It names the connection, the store (`object_type`) and the secret
/// (`object_id`), and the version when the store reported one. It never
/// carries the value or anything derived from it, such as its length.
pub fn value_retrieval_event(
    target: &RetrievalTarget<'_>,
    version: Option<&str>,
    failure: Option<&ValueFailure>,
) -> EventRecord {
    let (severity, outcome, action) = match failure {
        Some(_) => (
            EventSeverity::Error,
            EventOutcome::Failure,
            VALUE_RETRIEVE_FAILED_ACTION,
        ),
        None => (
            EventSeverity::Info,
            EventOutcome::Success,
            VALUE_RETRIEVED_ACTION,
        ),
    };

    let summary = match failure {
        Some(_) => format!(
            "Could not retrieve the value of {} from {}",
            target.name, target.store_label
        ),
        None => format!(
            "Retrieved the value of {} from {}",
            target.name, target.store_label
        ),
    };

    let mut event = EventRecord::new(
        Utc::now().timestamp_millis(),
        severity,
        EventCategory::SecretStore,
        outcome,
    )
    .with_action(action)
    .with_summary(summary)
    .with_actor_id("ui:user")
    .with_object_ref(target.store_id, target.name)
    .with_connection_context(
        target.profile_id.to_string(),
        String::new(),
        target.driver_id,
    );

    if let Some(version) = version {
        event = event.with_details_json(serde_json::json!({ "version": version }).to_string());
    }

    if let Some(failure) = failure {
        event = event
            .with_error("secret_value_retrieve_failed", failure.message.clone())
            .with_correlation_id(failure.correlation_id.to_string());
    }

    event
}

/// Standard base64 with padding (RFC 4648).
pub fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);

    for chunk in bytes.chunks(3) {
        let first = chunk.first().copied().unwrap_or(0);
        let second = chunk.get(1).copied();
        let third = chunk.get(2).copied();

        let triple = (u32::from(first) << 16)
            | (u32::from(second.unwrap_or(0)) << 8)
            | u32::from(third.unwrap_or(0));

        let symbol = |shift: u32| {
            ALPHABET
                .get(((triple >> shift) & 0x3f) as usize)
                .map_or('=', |symbol| char::from(*symbol))
        };

        encoded.push(symbol(18));
        encoded.push(symbol(12));
        encoded.push(if second.is_some() { symbol(6) } else { '=' });
        encoded.push(if third.is_some() { symbol(0) } else { '=' });
    }

    encoded
}
