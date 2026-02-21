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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyScanPage {
    pub entries: Vec<KeyEntry>,
    pub next_cursor: Option<String>,
}

/// Request for reading a key value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyGetRequest {
    pub key: String,
    pub keyspace: Option<u32>,
    pub include_type: bool,
    pub include_ttl: bool,
    pub include_size: bool,
}

impl KeyGetRequest {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
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

/// Key value with metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyGetResult {
    pub entry: KeyEntry,
    pub value: Vec<u8>,
    pub repr: ValueRepr,
}

/// Request for writing a key value.
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Set a field in a Hash key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashSetRequest {
    pub key: String,
    pub field: String,
    pub value: String,
    pub keyspace: Option<u32>,
}

/// Delete a field from a Hash key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashDeleteRequest {
    pub key: String,
    pub field: String,
    pub keyspace: Option<u32>,
}

/// Overwrite a list element at a given index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListSetRequest {
    pub key: String,
    pub index: i64,
    pub value: String,
    pub keyspace: Option<u32>,
}

/// Push a value to the head or tail of a list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPushRequest {
    pub key: String,
    pub value: String,
    pub end: ListEnd,
    pub keyspace: Option<u32>,
}

/// Remove occurrences of a value from a list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRemoveRequest {
    pub key: String,
    pub value: String,
    pub count: i64,
    pub keyspace: Option<u32>,
}

/// Add a member to a Set key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetAddRequest {
    pub key: String,
    pub member: String,
    pub keyspace: Option<u32>,
}

/// Remove a member from a Set key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetRemoveRequest {
    pub key: String,
    pub member: String,
    pub keyspace: Option<u32>,
}

/// Add or update a member with a score in a Sorted Set key.
#[derive(Debug, Clone, PartialEq)]
pub struct ZSetAddRequest {
    pub key: String,
    pub member: String,
    pub score: f64,
    pub keyspace: Option<u32>,
}

/// Remove a member from a Sorted Set key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZSetRemoveRequest {
    pub key: String,
    pub member: String,
    pub keyspace: Option<u32>,
}
