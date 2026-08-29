use serde::{Deserialize, Serialize};

/// Generic key type across key-value databases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum KeyType {
    String,
    Bytes,
    Hash,
    List,
    Set,
    SortedSet,
    Json,
    Stream,
    Unknown,
}

/// UI-oriented representation for a key's value payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ValueRepr {
    Text,
    Json,
    Binary,
    Structured,
    /// Redis stream entries serialized as JSON array of `{id, fields}`.
    Stream,
}

/// Metadata for a key in a key-value store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyEntry {
    pub key: String,
    pub key_type: Option<KeyType>,
    pub ttl_seconds: Option<i64>,
    pub size_bytes: Option<u64>,
}

impl KeyEntry {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            key_type: None,
            ttl_seconds: None,
            size_bytes: None,
        }
    }
}

/// Request for scanning keys with cursor-based pagination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyScanRequest {
    pub cursor: Option<String>,
    pub filter: Option<String>,
    pub limit: u32,
    pub keyspace: Option<u32>,
}

impl KeyScanRequest {
    pub fn new(limit: u32) -> Self {
        Self {
            cursor: None,
            filter: None,
            limit,
            keyspace: None,
        }
    }

    pub fn with_cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    pub fn with_keyspace(mut self, keyspace: u32) -> Self {
        self.keyspace = Some(keyspace);
        self
    }
}

/// A page of keys returned by a scan operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyScanPage {
    pub entries: Vec<KeyEntry>,
    pub next_cursor: Option<String>,
}

/// Request for reading a key value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyGetRequest {
    pub key: String,
    pub keyspace: Option<u32>,
    pub include_type: bool,
    pub include_ttl: bool,
    pub include_size: bool,
    /// Upper bound on the number of value bytes the driver may transfer.
    ///
    /// `None` means unbounded — the driver fetches the value regardless of
    /// its size. A peer that omits this field on the wire (older protocol
    /// version) is treated as `None`.
    #[serde(default)]
    pub max_value_bytes: Option<u64>,
}

impl KeyGetRequest {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            keyspace: None,
            include_type: true,
            include_ttl: true,
            include_size: true,
            max_value_bytes: None,
        }
    }

    pub fn with_keyspace(mut self, keyspace: u32) -> Self {
        self.keyspace = Some(keyspace);
        self
    }

    pub fn with_max_value_bytes(mut self, max_value_bytes: u64) -> Self {
        self.max_value_bytes = Some(max_value_bytes);
        self
    }
}

/// Whether a key's value bytes were fully transferred, and why not when they
/// were not.
///
/// Modeled after `PreviewGate` in
/// `dbflux_ui_document::object_browser::metadata` — a size decision derived
/// from metadata alone, without ever transferring bytes it decided not to
/// transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum KeyLoadState {
    /// The full value was fetched.
    #[default]
    Loaded,
    /// Only part of the value was fetched (e.g. a driver-side item cap on a
    /// collection type). `returned_bytes` describes what was actually
    /// transferred; `total_bytes` is the full size when known.
    Truncated {
        returned_bytes: u64,
        total_bytes: Option<u64>,
    },
    /// The value was not fetched at all because its size exceeds
    /// `max_value_bytes`. `value` is empty in this case.
    TooLarge { size_bytes: u64, limit_bytes: u64 },
}

/// Key value with metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyGetResult {
    pub entry: KeyEntry,
    pub value: Vec<u8>,
    pub repr: ValueRepr,
    /// Whether `value` is the complete payload. Absent on the wire (older
    /// protocol peers) defaults to `Loaded`, matching pre-gate behavior.
    #[serde(default)]
    pub load_state: KeyLoadState,
}

/// Request for writing a key value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeySetRequest {
    pub key: String,
    pub value: Vec<u8>,
    pub repr: ValueRepr,
    pub keyspace: Option<u32>,
    pub ttl_seconds: Option<u64>,
    pub condition: SetCondition,
}

/// Conditional behavior for key writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum SetCondition {
    #[default]
    Always,
    IfNotExists,
    IfExists,
}

impl KeySetRequest {
    pub fn new(key: impl Into<String>, value: Vec<u8>) -> Self {
        Self {
            key: key.into(),
            value,
            repr: ValueRepr::Binary,
            keyspace: None,
            ttl_seconds: None,
            condition: SetCondition::Always,
        }
    }

    pub fn with_repr(mut self, repr: ValueRepr) -> Self {
        self.repr = repr;
        self
    }

    pub fn with_keyspace(mut self, keyspace: u32) -> Self {
        self.keyspace = Some(keyspace);
        self
    }

    pub fn with_ttl(mut self, ttl_seconds: u64) -> Self {
        self.ttl_seconds = Some(ttl_seconds);
        self
    }

    pub fn if_not_exists(mut self) -> Self {
        self.condition = SetCondition::IfNotExists;
        self
    }

    pub fn if_exists(mut self) -> Self {
        self.condition = SetCondition::IfExists;
        self
    }
}

/// Request for deleting a key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyDeleteRequest {
    pub key: String,
    pub keyspace: Option<u32>,
}

impl KeyDeleteRequest {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            keyspace: None,
        }
    }

    pub fn with_keyspace(mut self, keyspace: u32) -> Self {
        self.keyspace = Some(keyspace);
        self
    }
}

/// Request for checking key existence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyExistsRequest {
    pub key: String,
    pub keyspace: Option<u32>,
}

impl KeyExistsRequest {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            keyspace: None,
        }
    }

    pub fn with_keyspace(mut self, keyspace: u32) -> Self {
        self.keyspace = Some(keyspace);
        self
    }
}

/// Request for reading key type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyTypeRequest {
    pub key: String,
    pub keyspace: Option<u32>,
}

impl KeyTypeRequest {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            keyspace: None,
        }
    }

    pub fn with_keyspace(mut self, keyspace: u32) -> Self {
        self.keyspace = Some(keyspace);
        self
    }
}

/// Request for reading key TTL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyTtlRequest {
    pub key: String,
    pub keyspace: Option<u32>,
}

impl KeyTtlRequest {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            keyspace: None,
        }
    }

    pub fn with_keyspace(mut self, keyspace: u32) -> Self {
        self.keyspace = Some(keyspace);
        self
    }
}

/// Request for setting key TTL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyExpireRequest {
    pub key: String,
    pub ttl_seconds: u64,
    pub keyspace: Option<u32>,
}

impl KeyExpireRequest {
    pub fn new(key: impl Into<String>, ttl_seconds: u64) -> Self {
        Self {
            key: key.into(),
            ttl_seconds,
            keyspace: None,
        }
    }

    pub fn with_keyspace(mut self, keyspace: u32) -> Self {
        self.keyspace = Some(keyspace);
        self
    }
}

/// Request for removing key TTL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyPersistRequest {
    pub key: String,
    pub keyspace: Option<u32>,
}

impl KeyPersistRequest {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            keyspace: None,
        }
    }

    pub fn with_keyspace(mut self, keyspace: u32) -> Self {
        self.keyspace = Some(keyspace);
        self
    }
}

/// Request for renaming a key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRenameRequest {
    pub from_key: String,
    pub to_key: String,
    pub keyspace: Option<u32>,
}

impl KeyRenameRequest {
    pub fn new(from_key: impl Into<String>, to_key: impl Into<String>) -> Self {
        Self {
            from_key: from_key.into(),
            to_key: to_key.into(),
            keyspace: None,
        }
    }

    pub fn with_keyspace(mut self, keyspace: u32) -> Self {
        self.keyspace = Some(keyspace);
        self
    }
}

/// Request for fetching multiple keys in one round trip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyBulkGetRequest {
    pub keys: Vec<String>,
    pub keyspace: Option<u32>,
    pub include_type: bool,
    pub include_ttl: bool,
    pub include_size: bool,
}

impl KeyBulkGetRequest {
    pub fn new(keys: Vec<String>) -> Self {
        Self {
            keys,
            keyspace: None,
            include_type: true,
            include_ttl: true,
            include_size: true,
        }
    }

    pub fn with_keyspace(mut self, keyspace: u32) -> Self {
        self.keyspace = Some(keyspace);
        self
    }
}

// ---------------------------------------------------------------------------
// Member-level operations for structured key types (Hash, List, Set, ZSet)
// ---------------------------------------------------------------------------

/// Which end of a list to push to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ListEnd {
    Head,
    Tail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashSetRequest {
    pub key: String,
    pub fields: Vec<(String, String)>,
    pub keyspace: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashDeleteRequest {
    pub key: String,
    pub fields: Vec<String>,
    pub keyspace: Option<u32>,
}

/// Overwrite a list element at a given index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListSetRequest {
    pub key: String,
    pub index: i64,
    pub value: String,
    pub keyspace: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListPushRequest {
    pub key: String,
    pub values: Vec<String>,
    pub end: ListEnd,
    pub keyspace: Option<u32>,
}

/// Remove occurrences of a value from a list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListRemoveRequest {
    pub key: String,
    pub value: String,
    pub count: i64,
    pub keyspace: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetAddRequest {
    pub key: String,
    pub members: Vec<String>,
    pub keyspace: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetRemoveRequest {
    pub key: String,
    pub members: Vec<String>,
    pub keyspace: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ZSetAddRequest {
    pub key: String,
    pub members: Vec<(String, f64)>,
    pub keyspace: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZSetRemoveRequest {
    pub key: String,
    pub members: Vec<String>,
    pub keyspace: Option<u32>,
}

// ---------------------------------------------------------------------------
// Stream operations
// ---------------------------------------------------------------------------

/// How to generate the entry ID for `XADD`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamEntryId {
    /// Server-assigned (`*`).
    #[default]
    Auto,
    /// Caller-supplied explicit ID (e.g. `"1526919030474-55"`).
    Explicit(String),
}

/// Optional max-length trimming strategy for stream writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMaxLen {
    pub count: u64,
    /// If `true`, use approximate trimming (`~`), which is cheaper.
    pub approximate: bool,
}

/// Add an entry to a Stream key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamAddRequest {
    pub key: String,
    pub id: StreamEntryId,
    pub fields: Vec<(String, String)>,
    pub maxlen: Option<StreamMaxLen>,
    pub keyspace: Option<u32>,
}

/// Delete entries from a Stream key by their IDs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDeleteRequest {
    pub key: String,
    pub ids: Vec<String>,
    pub keyspace: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::{KeyEntry, KeyGetRequest, KeyGetResult, KeyLoadState, ValueRepr};

    #[test]
    fn key_get_request_default_is_unbounded() {
        let request = KeyGetRequest::new("some-key");

        assert_eq!(request.max_value_bytes, None);
    }

    #[test]
    fn key_get_request_with_max_value_bytes_sets_budget() {
        let request = KeyGetRequest::new("some-key").with_max_value_bytes(1_024);

        assert_eq!(request.max_value_bytes, Some(1_024));
    }

    #[test]
    fn key_load_state_default_is_loaded() {
        assert_eq!(KeyLoadState::default(), KeyLoadState::Loaded);
    }

    #[test]
    fn key_get_result_missing_load_state_on_wire_deserializes_to_loaded() {
        // Simulates an older peer's response that predates `load_state`.
        let json = serde_json::json!({
            "entry": KeyEntry::new("some-key"),
            "value": [1, 2, 3],
            "repr": "Binary",
        });

        let result: KeyGetResult = serde_json::from_value(json).expect("deserialize");

        assert_eq!(result.load_state, KeyLoadState::Loaded);
        assert_eq!(result.repr, ValueRepr::Binary);
    }

    #[test]
    fn key_load_state_variants_round_trip_through_json() {
        let truncated = KeyLoadState::Truncated {
            returned_bytes: 50,
            total_bytes: Some(200),
        };
        let json = serde_json::to_string(&truncated).expect("serialize");
        let decoded: KeyLoadState = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, truncated);

        let too_large = KeyLoadState::TooLarge {
            size_bytes: 5_000,
            limit_bytes: 1_000,
        };
        let json = serde_json::to_string(&too_large).expect("serialize");
        let decoded: KeyLoadState = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, too_large);
    }
}
