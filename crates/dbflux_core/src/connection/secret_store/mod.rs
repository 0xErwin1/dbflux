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
//! `Debug` output and zeroed when dropped. A value is replaced only by
//! `SecretStoreSource::put_value`, under the same rules, on stores whose
//! descriptor advertises `SecretStoreCapabilities::WRITE`. The history of a
//! secret is `SecretStoreSource::list_versions`, which reports metadata only.
//!
//! Every type here is provider-neutral: no SDK type crosses the seam, so an
//! external RPC driver can implement it.

use bitflags::bitflags;
use chrono::{DateTime, Utc};
use secrecy::{ExposeSecret, SecretSlice, SecretString};

use crate::DbError;

mod operations;
mod scrub;

pub use operations::{
    DEFAULT_RECOVERY_WINDOW_DAYS, MAX_RECOVERY_WINDOW_DAYS, MAX_TAGS, MIN_RECOVERY_WINDOW_DAYS,
    SecretCreateRequest, SecretCreated, SecretDeleteRequest, SecretDeletion,
    SecretDescriptionChange, SecretDescriptionUpdate, SecretRollback, SecretRollbackRequest,
    SecretRotationChange, SecretRotationStart, SecretRotationUpdate, SecretTagsUpdate,
    require_name, validate_rotation_function, validate_rotation_schedule, validate_rotation_window,
    validate_rotation_window_fits, validate_tag,
};
pub use scrub::{encode_base64, scrub_echoed_values, scrub_submitted_payload};

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

        /// Values can be replaced with a new version through
        /// `SecretStoreSource::put_value`. A store that cannot write does not
        /// advertise it, and the UI reads its secrets read only.
        const WRITE = 1 << 6;

        /// Secrets can be deleted through `SecretStoreSource::delete_secret`.
        /// With `SOFT_DELETE` the deletion is scheduled with a recovery
        /// window and can be restored; without it the deletion is
        /// irreversible and needs the secret's exact name as confirmation.
        const DELETE = 1 << 7;

        /// Tags can be added, changed and removed through
        /// `SecretStoreSource::update_tags`.
        const TAGS = 1 << 8;

        /// The description can be changed through
        /// `SecretStoreSource::update_description`.
        const DESCRIPTION = 1 << 9;

        /// New secrets can be created through
        /// `SecretStoreSource::create_secret`.
        const CREATE = 1 << 10;
    }
}

/// The value types and tiers a store accepts when it creates a secret, in the
/// store's own names, so a create dialog renders them without knowing which
/// store it talks to. The store still checks the request: this is what it
/// offers, not what it enforces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretCreateChoices {
    value_types: Vec<String>,
    default_value_type: String,
    encrypted_value_type: Option<String>,
    tiers: Vec<String>,
    default_tier: Option<String>,
}

impl SecretCreateChoices {
    /// The value types in the order they are offered, and the one a new value
    /// starts as. A default the list does not hold falls back to its first
    /// entry.
    pub fn new(
        value_types: impl IntoIterator<Item = impl Into<String>>,
        default_value_type: impl Into<String>,
    ) -> Self {
        let value_types: Vec<String> = value_types.into_iter().map(Into::into).collect();
        let default_value_type =
            Self::default_of(&value_types, default_value_type.into()).unwrap_or_default();

        Self {
            value_types,
            default_value_type,
            encrypted_value_type: None,
            tiers: Vec::new(),
            default_tier: None,
        }
    }

    /// Names the value type that is encrypted, the only one that takes an
    /// encryption key. Ignored when the store does not offer it.
    pub fn with_encrypted_value_type(mut self, value_type: impl Into<String>) -> Self {
        let value_type = value_type.into();

        if self.value_types.contains(&value_type) {
            self.encrypted_value_type = Some(value_type);
        }

        self
    }

    /// The tiers in the order they are offered, and the one a new value
    /// starts in. A default the list does not hold falls back to its first
    /// entry.
    pub fn with_tiers(
        mut self,
        tiers: impl IntoIterator<Item = impl Into<String>>,
        default_tier: impl Into<String>,
    ) -> Self {
        self.tiers = tiers.into_iter().map(Into::into).collect();
        self.default_tier = Self::default_of(&self.tiers, default_tier.into());
        self
    }

    fn default_of(offered: &[String], preferred: String) -> Option<String> {
        if offered.contains(&preferred) {
            Some(preferred)
        } else {
            offered.first().cloned()
        }
    }

    pub fn value_types(&self) -> &[String] {
        &self.value_types
    }

    pub fn default_value_type(&self) -> &str {
        &self.default_value_type
    }

    /// Whether a secret of `value_type` takes an encryption key.
    pub fn takes_encryption_key(&self, value_type: &str) -> bool {
        self.encrypted_value_type.as_deref() == Some(value_type)
    }

    /// The tiers on offer; empty for a store without tiers.
    pub fn tiers(&self) -> &[String] {
        &self.tiers
    }

    pub fn default_tier(&self) -> Option<&str> {
        self.default_tier.as_deref()
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

    /// What a create dialog offers for a store whose values are typed
    /// (`TYPED_VALUES`). `None` for a store whose secrets have neither a
    /// type nor a tier.
    pub create_choices: Option<SecretCreateChoices>,
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
            create_choices: None,
        }
    }

    /// Declares the value types and tiers a create dialog offers.
    pub fn with_create_choices(mut self, choices: SecretCreateChoices) -> Self {
        self.create_choices = Some(choices);
        self
    }

    pub fn supports(&self, capability: SecretStoreCapabilities) -> bool {
        self.capabilities.contains(capability)
    }

    /// Refuses when this store cannot write a value: a store that does not
    /// advertise `WRITE` rejects a put before any call, and
    /// `SecretStoreSource::put_value` implementations begin with this check.
    /// The refusal names the secret the caller asked to write, never its
    /// value.
    pub fn ensure_can_write(&self, name: &str) -> Result<(), DbError> {
        if self.supports(SecretStoreCapabilities::WRITE) {
            return Ok(());
        }

        Err(DbError::NotSupported(format!(
            "Secret store \"{}\" ({}) does not support writing values; refused the write to \"{name}\".",
            self.label, self.id
        )))
    }

    /// Refuses when this store lacks any capability in `required`, naming
    /// the operation the caller asked for and the secret it targets. Every
    /// write operation other than `put_value` begins with this check, with
    /// `WRITE` and the capability of its own, and so does the governed seam
    /// before it calls the store.
    pub fn ensure_can(
        &self,
        required: SecretStoreCapabilities,
        operation: &str,
        name: &str,
    ) -> Result<(), DbError> {
        if self.supports(required) {
            return Ok(());
        }

        Err(DbError::NotSupported(format!(
            "Secret store \"{}\" ({}) does not support this operation; refused to {operation} \"{name}\".",
            self.label, self.id
        )))
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

    /// When the deletion was requested, for a store that reports that moment.
    /// A store that reports only the date the deletion takes effect
    /// (`SecretSummary::deletion_scheduled_for`) leaves it `None`: the moment
    /// and the recovery window are then unknown, and neither is inferred.
    pub deleted_at: Option<DateTime<Utc>>,
}

/// One entry of a secret's version history, as
/// `SecretStoreSource::list_versions` reports it.
///
/// It has no value field and no payload type: a history row cannot hold a
/// secret, so rendering or logging one can never leak it. A value is read
/// only through `SecretStoreSource::get_value` with `id`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecretVersionInfo {
    /// The selector `SecretStoreSource::get_value` accepts for this version:
    /// a version id in Secrets Manager, a version number in Parameter Store.
    pub id: String,

    /// Staging labels (Secrets Manager) or parameter labels (Parameter
    /// Store) attached to this version. Empty for a version with none.
    pub labels: Vec<String>,

    pub created: Option<DateTime<Utc>>,

    /// Principal that wrote this version, when the store reports it.
    pub modified_by: Option<String>,

    /// The version the store serves when no version is requested. At most
    /// one row of a history carries it.
    pub is_current: bool,

    /// Staged by an unfinished change (a rotation that has not finished):
    /// rolling back onto it is refused, because it would promote a value
    /// nobody validated.
    pub is_pending: bool,
}

/// A retrieved or written value.
#[derive(Clone, Debug)]
pub enum SecretPayload {
    Text(SecretString),
    Binary(SecretSlice<u8>),
}

/// The version stage a value is written to.
///
/// What a store does with a stage is its own contract, and neither
/// capability promises a staged write: a store with movable version labels
/// stages it (Secrets Manager attaches `AWSPENDING` and moves no current
/// value), and a store that versions by number has no staging labels and
/// refuses a `Pending` write before any call, keeping the current value
/// (Parameter Store).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SecretVersionStage {
    /// The new version becomes the current one.
    Current,

    /// The new version is written without moving the current one, for
    /// stores that stage versions; a store without a staging shape refuses
    /// it instead of treating it as a `Current` write.
    Pending,
}

/// A value to write, as `SecretStoreSource::put_value` receives it. The
/// payload travels as `secrecy` types, so it is redacted from `Debug` output.
#[derive(Debug, Clone)]
pub struct SecretPutRequest {
    /// Full name of the secret, including any path.
    pub name: String,

    /// The new value, as text or binary.
    pub payload: SecretPayload,

    /// Stage the new version is written to. `None` and `Current` make the new
    /// version the current one.
    pub stage: Option<SecretVersionStage>,
}

impl SecretPutRequest {
    pub fn text(name: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            payload: SecretPayload::Text(SecretString::from(text.into())),
            stage: None,
        }
    }

    pub fn binary(name: impl Into<String>, bytes: Vec<u8>) -> Self {
        Self {
            name: name.into(),
            payload: SecretPayload::Binary(SecretSlice::from(bytes)),
            stage: None,
        }
    }

    pub fn with_stage(mut self, stage: SecretVersionStage) -> Self {
        self.stage = Some(stage);
        self
    }
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

/// The refusal of a write operation a store does not implement, naming the
/// store and the secret and never a value.
fn refuse_unimplemented<T>(
    descriptor: &SecretStoreDescriptor,
    operation: &str,
    name: &str,
) -> Result<T, DbError> {
    Err(DbError::NotSupported(format!(
        "Secret store \"{}\" does not implement {operation}; refused it for \"{name}\".",
        descriptor.id
    )))
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

    /// Writes `request` as a new version of the secret `request.name` names,
    /// in the stage `request.stage` asks for, and returns the value as the
    /// store reports it after the write, with the new version's identifier.
    ///
    /// A store that does not advertise `WRITE` refuses before any call, and
    /// the default refuses too: writing exists only where a store implements
    /// it. Like `get_value`, a write never logs, formats or stores the value
    /// outside the `secrecy` types it travels in, and a failure carries the
    /// store's reason only, never the value.
    fn put_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
        self.descriptor().ensure_can_write(&request.name)?;

        Err(DbError::NotSupported(format!(
            "Secret store \"{}\" does not implement writing values; refused the write to \"{}\".",
            self.descriptor().id,
            request.name
        )))
    }

    /// Lists the versions of `name`, newest first, following every page the
    /// store answers with. Never fetches a value: a row carries metadata
    /// only, and each `id` is read through `get_value`.
    ///
    /// A store's history call may return values to the SDK anyway: Parameter
    /// Store's `GetParameterHistory` carries each version's value even with
    /// decryption off, in plaintext for `String` and `StringList`. An
    /// implementation never reads, stores, logs or formats them; it maps
    /// each entry to metadata and drops the rest.
    ///
    /// Stores advertising `VERSIONS` implement it; the default refuses
    /// instead of answering an empty history.
    fn list_versions(&self, name: &str) -> Result<Vec<SecretVersionInfo>, DbError> {
        Err(DbError::NotSupported(format!(
            "Secret store \"{}\" does not implement listing versions; refused the history of \"{name}\".",
            self.descriptor().id
        )))
    }

    /// Makes `request.version_id` the current version of `request.name`
    /// and the version that was current the previous one, by moving the
    /// staging labels. No value is rewritten and no version is created. It
    /// refuses rolling back to the version that is already current.
    ///
    /// Stores advertising `VERSION_LABELS` and `WRITE` implement it; the
    /// default refuses.
    fn roll_back(&self, request: &SecretRollbackRequest) -> Result<SecretRollback, DbError> {
        refuse_unimplemented(self.descriptor(), "rolling back", &request.name)
    }

    /// Deletes a secret. A `Scheduled` request puts the deletion behind the
    /// recovery window of a `SOFT_DELETE` store, and an `Irreversible` one,
    /// accepted only by a store without that window and only with the exact
    /// name as confirmation, removes it for good. A store refuses the kind
    /// it does not offer before any call.
    ///
    /// Stores advertising `DELETE` and `WRITE` implement it; the default
    /// refuses.
    fn delete_secret(&self, request: &SecretDeleteRequest) -> Result<SecretDeletion, DbError> {
        refuse_unimplemented(self.descriptor(), "deleting secrets", request.name())
    }

    /// Cancels a scheduled deletion, so the secret and its versions are
    /// available again. Stores advertising `SOFT_DELETE` and `WRITE`
    /// implement it; the default refuses.
    fn restore_secret(&self, name: &str) -> Result<(), DbError> {
        refuse_unimplemented(self.descriptor(), "restoring secrets", name)
    }

    /// Starts a rotation of `name` now, with the rotation already
    /// configured on the secret. A secret whose rotation is not enabled is
    /// refused rather than having it switched on as a side effect. Stores
    /// advertising `ROTATION` and `WRITE` implement it; the default refuses.
    fn rotate_now(&self, name: &str) -> Result<SecretRotationStart, DbError> {
        refuse_unimplemented(self.descriptor(), "rotating secrets", name)
    }

    /// Turns automatic rotation on with `update`'s schedule, window and
    /// function, or off, without rotating the secret now. Turning it off
    /// while a rotation is in progress reports the version it left
    /// unfinished in `SecretRotationChange::unfinished_version`. Stores
    /// advertising `ROTATION` and `WRITE` implement it; the default refuses.
    fn update_rotation(
        &self,
        update: &SecretRotationUpdate,
    ) -> Result<SecretRotationChange, DbError> {
        refuse_unimplemented(self.descriptor(), "editing rotation", &update.name)
    }

    /// Creates the secret `request.name` with `request`'s value and
    /// settings, and returns what the store reports about it. It never
    /// overwrites: a name that already exists is refused (a constraint
    /// violation), so creating cannot replace a secret's value. Like
    /// `put_value`, it never logs, formats or stores the value outside the
    /// `secrecy` types it travels in, and a failure carries the store's
    /// reason only. Stores advertising `CREATE` and `WRITE` implement it; the
    /// default refuses.
    fn create_secret(&self, request: &SecretCreateRequest) -> Result<SecretCreated, DbError> {
        refuse_unimplemented(self.descriptor(), "creating secrets", &request.name)
    }

    /// Adds, changes and removes tags without touching the value or any
    /// version. Stores advertising `TAGS` and `WRITE` implement it; the
    /// default refuses.
    fn update_tags(&self, update: &SecretTagsUpdate) -> Result<(), DbError> {
        refuse_unimplemented(self.descriptor(), "editing tags", &update.name)
    }

    /// Changes the description. A store that changes it alone leaves the
    /// versions as they are; a store that can only change it by writing the
    /// value again must keep the value, type and key exactly as they are and
    /// report the version it created, or refuse. Stores advertising
    /// `DESCRIPTION` and `WRITE` implement it; the default refuses.
    fn update_description(
        &self,
        update: &SecretDescriptionUpdate,
    ) -> Result<SecretDescriptionChange, DbError> {
        refuse_unimplemented(self.descriptor(), "editing descriptions", &update.name)
    }
}

#[cfg(test)]
mod tests;
