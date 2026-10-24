//! Secret store seam for DBFlux.
//!
//! Defines the `SecretStoreSource` trait that drivers implement to browse a secret
//! manager (for example AWS Secrets Manager or Systems Manager Parameter
//! Store). A connection exposes its stores through
//! `Connection::secret_stores()`, and a driver whose connections return any
//! store advertises `DriverCapabilities::SECRET_STORE`.
//!
//! Listing and describing never fetch a value. A value is fetched only by
//! `SecretStoreSource::get_value`, which the UI calls from an explicit user action
//! and audits. Values travel as `secrecy` types, so they are redacted from
//! `Debug` output and zeroed when dropped.
//!
//! Every type here is provider-neutral: no SDK type crosses the seam, so an
//! external RPC driver can implement it.

use bitflags::bitflags;
use chrono::{DateTime, Utc};
use secrecy::{ExposeSecret, SecretSlice, SecretString};

use crate::DbError;

bitflags! {
    /// Behavior that differs between secret stores. The UI gates actions on
    /// these flags and never on a driver id.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct SecretStoreCapabilities: u32 {
        /// The store keeps earlier versions of a value.
        const VERSIONS = 1 << 0;

        /// Versions carry named labels that can move between versions
        /// (for example `AWSCURRENT` and `AWSPREVIOUS`).
        const VERSION_LABELS = 1 << 1;

        /// Values can rotate on a schedule through a rotation function.
        const ROTATION = 1 << 2;

        /// Deleting schedules the deletion with a recovery window, and a
        /// scheduled deletion can be restored.
        const SOFT_DELETE = 1 << 3;

        /// Names are `/`-separated paths that can be browsed one level at a
        /// time through `SecretListRequest::path`.
        const HIERARCHICAL = 1 << 4;

        /// Each entry declares a value type (`SecretSummary::value_type`).
        const TYPED_VALUES = 1 << 5;
    }
}

/// One store a connection exposes, such as "Secrets Manager".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretStoreDescriptor {
    /// Stable identifier, unique within the connection. Used in sidebar node
    /// ids and document dedup keys.
    pub id: String,

    /// Name shown to the user.
    pub label: String,

    pub capabilities: SecretStoreCapabilities,
}

impl SecretStoreDescriptor {
    pub fn new(
        id: impl Into<String>,
        label: impl Into<String>,
        capabilities: SecretStoreCapabilities,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            capabilities,
        }
    }

    pub fn supports(&self, capability: SecretStoreCapabilities) -> bool {
        self.capabilities.contains(capability)
    }
}

/// One page request against `SecretStoreSource::list`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecretListRequest {
    /// Hierarchical stores only: the path whose direct children are listed,
    /// ending in `/` (for example `/app/prod/`). `None` lists the root.
    /// Flat stores ignore it.
    pub path: Option<String>,

    /// Hierarchical stores only: list every entry below `path` instead of
    /// one level, and return no `child_paths`.
    pub recursive: bool,

    /// Name filter the store evaluates on the server side. Its matching rule
    /// (prefix or substring) is the store's own.
    pub name_filter: Option<String>,

    /// Token from the previous page's `SecretListPage::next_page_token`.
    pub page_token: Option<String>,

    /// Upper bound on entries per page. `None` uses the store's default.
    pub page_size: Option<u32>,
}

/// One page of a listing. It holds metadata only, never a value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecretListPage {
    pub entries: Vec<SecretSummary>,

    /// Hierarchical stores only: full paths of the sub-paths found directly
    /// under the requested path, each ending in `/`. A later page can repeat
    /// a path an earlier page returned, so callers merge them.
    pub child_paths: Vec<String>,

    /// `None` when this is the last page.
    pub next_page_token: Option<String>,
}

/// Listing row for one secret.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecretSummary {
    /// Full name, including any path.
    pub name: String,

    pub description: Option<String>,

    /// The store's own name for the value type, for example `SecureString`.
    /// Set by stores that advertise `TYPED_VALUES`.
    pub value_type: Option<String>,

    /// The store's own tier name, for example `Standard`.
    pub tier: Option<String>,

    /// Current version identifier, when the listing reports one.
    pub version: Option<String>,

    pub last_changed: Option<DateTime<Utc>>,

    /// Set by stores that advertise `ROTATION`.
    pub rotation: Option<SecretRotation>,

    /// Service that owns and manages this secret. A secret with an owner is
    /// read only in DBFlux.
    pub owning_service: Option<String>,

    /// When a scheduled deletion takes effect. Set by stores that advertise
    /// `SOFT_DELETE` for a secret whose deletion is pending.
    pub deletion_scheduled_for: Option<DateTime<Utc>>,
}

impl SecretSummary {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    /// Whether another service owns this secret, which makes it read only.
    pub fn is_managed(&self) -> bool {
        self.owning_service.is_some()
    }

    /// Whether a deletion is scheduled, which makes the value unavailable
    /// until the secret is restored.
    pub fn is_pending_deletion(&self) -> bool {
        self.deletion_scheduled_for.is_some()
    }
}

/// Rotation configuration and state of one secret.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecretRotation {
    pub enabled: bool,

    /// Schedule expression as the store accepts it, for example
    /// `rate(30 days)`.
    pub schedule: Option<String>,

    /// Rotation window as the store reports it, for example `4h`.
    pub window: Option<String>,

    /// Name of the function that performs the rotation.
    pub function: Option<String>,

    pub last_rotated: Option<DateTime<Utc>>,

    pub next_rotation: Option<DateTime<Utc>>,
}

/// One `key = value` tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretTag {
    pub key: String,
    pub value: String,
}

impl SecretTag {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Everything `SecretStoreSource::describe` reports about one secret, without its
/// value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecretDetails {
    pub summary: SecretSummary,

    /// Provider-wide identifier, for example an ARN.
    pub resource_id: Option<String>,

    /// Encryption key the value is encrypted with, as the store names it.
    pub encryption_key: Option<String>,

    /// The store's data type of the value, for example `text`.
    pub data_type: Option<String>,

    pub created: Option<DateTime<Utc>>,

    /// Principal that made the last change, when the store reports it.
    pub last_changed_by: Option<String>,

    pub last_accessed: Option<DateTime<Utc>>,

    /// Number of versions the store keeps, when it reports them.
    pub version_count: Option<u32>,

    /// Labels attached to versions, in the store's order. Set by stores that
    /// advertise `VERSION_LABELS`.
    pub version_labels: Vec<String>,

    pub tags: Vec<SecretTag>,

    /// When the deletion was requested. Set together with
    /// `SecretSummary::deletion_scheduled_for`.
    pub deleted_at: Option<DateTime<Utc>>,
}

/// A retrieved value.
#[derive(Debug)]
pub enum SecretPayload {
    Text(SecretString),
    Binary(SecretSlice<u8>),
}

/// The value of one version of a secret, as `SecretStoreSource::get_value`
/// returns it.
#[derive(Debug)]
pub struct SecretValue {
    /// Version the value belongs to, when the store reports it.
    pub version: Option<String>,

    pub payload: SecretPayload,
}

impl SecretValue {
    pub fn text(version: Option<String>, text: impl Into<String>) -> Self {
        Self {
            version,
            payload: SecretPayload::Text(SecretString::from(text.into())),
        }
    }

    pub fn binary(version: Option<String>, bytes: Vec<u8>) -> Self {
        Self {
            version,
            payload: SecretPayload::Binary(SecretSlice::from(bytes)),
        }
    }

    pub fn is_binary(&self) -> bool {
        matches!(self.payload, SecretPayload::Binary(_))
    }

    /// The value as key/value pairs, in the order they appear, when it is a
    /// one-level JSON object whose values are all scalars. Nested objects,
    /// arrays, other JSON and any other text return `None` and are shown as
    /// plaintext. A JSON string value is unquoted, a `null` becomes an empty
    /// string, and numbers and booleans keep their JSON text.
    pub fn key_value_pairs(&self) -> Option<Vec<(String, SecretString)>> {
        let SecretPayload::Text(text) = &self.payload else {
            return None;
        };

        parse_key_value_pairs(text.expose_secret())
    }
}

fn parse_key_value_pairs(text: &str) -> Option<Vec<(String, SecretString)>> {
    let OrderedObject(entries) = serde_json::from_str(text).ok()?;

    if entries.is_empty() {
        return None;
    }

    entries
        .into_iter()
        .map(|(key, value)| {
            let text = match value {
                serde_json::Value::String(text) => text,
                serde_json::Value::Null => String::new(),
                serde_json::Value::Bool(_) | serde_json::Value::Number(_) => value.to_string(),
                serde_json::Value::Array(_) | serde_json::Value::Object(_) => return None,
            };

            Some((key, SecretString::from(text)))
        })
        .collect()
}

/// A JSON object read with its keys in source order. `serde_json::Map`
/// sorts them unless the crate's `preserve_order` feature is on, which would
/// change key order everywhere else in the workspace.
struct OrderedObject(Vec<(String, serde_json::Value)>);

impl<'de> serde::Deserialize<'de> for OrderedObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ObjectVisitor;

        impl<'de> serde::de::Visitor<'de> for ObjectVisitor {
            type Value = OrderedObject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut entries = Vec::with_capacity(map.size_hint().unwrap_or(0));

                while let Some(entry) = map.next_entry::<String, serde_json::Value>()? {
                    entries.push(entry);
                }

                Ok(OrderedObject(entries))
            }
        }

        deserializer.deserialize_map(ObjectVisitor)
    }
}

/// A browsable secret store.
///
/// Methods are synchronous and block on the driver's own runtime, like
/// `DashboardSource`. Callers invoke them from a background executor and
/// never on the UI thread.
pub trait SecretStoreSource: Send + Sync {
    fn descriptor(&self) -> &SecretStoreDescriptor;

    /// Lists one page of secrets. Never fetches a value.
    fn list(&self, request: &SecretListRequest) -> Result<SecretListPage, DbError>;

    /// Reports the metadata of one secret. Never fetches its value.
    fn describe(&self, name: &str) -> Result<SecretDetails, DbError>;

    /// Fetches the value of `name`, at `version` when given and the current
    /// version otherwise. A value the caller may not decrypt or read is an
    /// error (`DbError::PermissionDenied`), never an empty value.
    fn get_value(&self, name: &str, version: Option<&str>) -> Result<SecretValue, DbError>;
}

#[cfg(test)]
mod tests;
