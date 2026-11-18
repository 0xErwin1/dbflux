//! The retrieved value of one secret, how it is presented, and the audit
//! event every retrieval records.
//!
//! A value only exists here after an explicit Retrieve value action. It is
//! held as `secrecy` types, so it is redacted from `Debug` output and zeroed
//! when dropped, and it is dropped when the selection changes, the listing is
//! refreshed or the tab closes.

use dbflux_core::chrono::{DateTime, Local, Utc};
pub use dbflux_core::encode_base64;
use dbflux_core::observability::{EventCategory, EventOutcome, EventRecord, EventSeverity};
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{SecretPayload, SecretValue};
use uuid::Uuid;

/// The masked text a value field shows until it is revealed.
pub(crate) const MASK: &str = "••••••••••••";

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

/// What a retrieved value is, derived from its content alone. It holds
/// counts, never text, so it is safe to draw next to a masked value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueKind {
    /// Every non-empty, non-comment line is `KEY=VALUE`.
    Dotenv {
        lines: usize,
    },
    JsonObject {
        keys: usize,
    },
    JsonArray {
        items: usize,
    },
    Text {
        lines: usize,
    },
    Binary,
}

impl ValueKind {
    fn of(content: &ValueContent) -> Self {
        let (ValueContent::KeyValue { text, .. } | ValueContent::Plaintext(text)) = content else {
            return Self::Binary;
        };

        let text = text.expose_secret();

        match serde_json::from_str::<serde_json::Value>(text) {
            Ok(serde_json::Value::Object(map)) => return Self::JsonObject { keys: map.len() },
            Ok(serde_json::Value::Array(items)) => {
                return Self::JsonArray { items: items.len() };
            }
            _ => {}
        }

        let lines = text.lines().count();

        if is_dotenv(text) {
            Self::Dotenv { lines }
        } else {
            Self::Text { lines }
        }
    }

    /// "dotenv · 9 lines", "JSON · 6 keys", "text · 1 line".
    pub fn label(self) -> String {
        let (one, many, count) = match self {
            Self::Dotenv { lines } => (
                "document.secret_store.value.kind.dotenv.one",
                "document.secret_store.value.kind.dotenv.many",
                lines,
            ),
            Self::JsonObject { keys } => (
                "document.secret_store.value.kind.json_keys.one",
                "document.secret_store.value.kind.json_keys.many",
                keys,
            ),
            Self::JsonArray { items } => (
                "document.secret_store.value.kind.json_items.one",
                "document.secret_store.value.kind.json_items.many",
                items,
            ),
            Self::Text { lines } => (
                "document.secret_store.value.kind.text.one",
                "document.secret_store.value.kind.text.many",
                lines,
            ),
            Self::Binary => return dbflux_i18n::t!("document.secret_store.value.kind.binary"),
        };

        dbflux_i18n::t!(if count == 1 { one } else { many }, count = count)
    }
}

/// The `=` left over when a base64 token is split at its padding: `YWJj==`
/// reads as the key `YWJj` with the value `=`. A real value may start with
/// `=` (`PASSWORD==secret`); only one made of nothing else is padding.
fn is_base64_padding(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|c| c == '=')
}

/// At least one assignment, and every line that is not blank or a comment
/// is one. A value that is one bare token is not an assignment: its padding
/// looks like one. Among other lines, a `KEY==` line is an assignment whose
/// value is `=`, because the token reading needs a value that is one token.
fn is_dotenv(text: &str) -> bool {
    let mut assignments = 0;
    let bare_token = text.trim().lines().count() == 1;

    for line in text.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let assignment = line.strip_prefix("export ").unwrap_or(line);
        let Some((key, value)) = assignment.split_once('=') else {
            return false;
        };

        let mut chars = key.chars();
        let valid_key = chars
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');

        if !valid_key || (bare_token && is_base64_padding(value)) {
            return false;
        }

        assignments += 1;
    }

    assignments > 0
}

/// Which view of a key/value secret is shown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueView {
    KeyValue,
    Plaintext,
}

/// The text the multi-line viewer draws for a value, decided once when the
/// value is retrieved.
#[derive(Debug)]
struct ViewerText {
    /// What the viewer draws: JSON pretty-printed with two spaces, or the
    /// stored text with the line breaks it was stored with.
    text: SecretString,
}

impl ViewerText {
    /// The viewer a value needs, or `None` for a value that keeps its own
    /// presentation: a key/value secret keeps its pairs, a binary one its
    /// base64, and a value whose drawn text has a single line keeps the
    /// single-line field it always had.
    ///
    /// A value the store keeps as JSON is drawn pretty-printed whenever it
    /// does not open as key/value (nested objects, arrays). The stored text
    /// stays the value's own, so Copy value copies what the store returned.
    fn new(content: &ValueContent) -> Option<Self> {
        let ValueContent::Plaintext(text) = content else {
            return None;
        };

        let stored = text.expose_secret();
        let drawn = match serde_json::from_str::<serde_json::Value>(stored) {
            Ok(json) => serde_json::to_string_pretty(&json).unwrap_or_else(|_| stored.to_string()),
            Err(_) => stored.to_string(),
        };

        (drawn.lines().count() > 1).then(|| Self {
            text: SecretString::from(drawn),
        })
    }
}

/// A value fetched by an explicit retrieval.
#[derive(Debug)]
pub struct RetrievedValue {
    pub version: Option<String>,
    pub content: ValueContent,
    pub retrieved_at: DateTime<Local>,
    pub view: ValueView,
    kind: ValueKind,
    /// What the multi-line viewer draws, `None` when the value keeps the
    /// single-line field.
    viewer: Option<ViewerText>,
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
            viewer: ViewerText::new(&content),
            kind: ValueKind::of(&content),
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

    /// Whether the value is drawn in the multi-line viewer instead of the
    /// single-line field: its text has more than one line, because it holds
    /// line breaks or because JSON that does not open as key/value is drawn
    /// pretty-printed.
    pub fn is_multiline(&self) -> bool {
        self.viewer.is_some()
    }

    /// The lines the viewer draws, in order: the value's own lines once it is
    /// revealed, one mask per line while it is hidden, so the line count
    /// shows and the text never does. Empty when the value keeps the
    /// single-line field.
    pub fn viewer_lines(&self, revealed: bool) -> Vec<String> {
        let Some(viewer) = &self.viewer else {
            return Vec::new();
        };

        let text = viewer.text.expose_secret();

        if revealed {
            text.lines().map(str::to_string).collect()
        } else {
            vec![MASK.to_string(); text.lines().count()]
        }
    }

    pub fn kind(&self) -> ValueKind {
        self.kind
    }

    pub fn kind_label(&self) -> String {
        self.kind.label()
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
