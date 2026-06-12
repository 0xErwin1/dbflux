pub mod bundle;
pub mod conflict;
pub mod error;
pub mod export;
pub mod import;

#[cfg(feature = "encryption")]
pub mod encryption;

pub use error::PortabilityError;

use std::collections::HashMap;

use dbflux_core::{
    AuthProfile, ConnectionProfile, ExportFieldHint, FieldExportTransform, FormValues,
    ProxyProfile, SshTunnelProfile,
};
use secrecy::SecretString;

// ---------------------------------------------------------------------------
// Caller-supplied seam traits (no I/O in this crate)
// ---------------------------------------------------------------------------

/// Resolves how a specific connection form field should travel in the bundle.
///
/// The app layer implements this by holding the driver registry and delegating
/// to `DbDriver::export_field_hint`. This crate never touches driver ids or
/// driver-specific logic.
pub trait FieldHintResolver {
    fn hint(
        &self,
        profile: &ConnectionProfile,
        field_id: &str,
        values: &FormValues,
    ) -> ExportFieldHint;
}

/// Reads a secret value from the OS keyring by its namespaced ref string.
///
/// Returns `None` when the secret is absent or the keyring is locked.
/// The app layer implements this by calling `SecretManager::get_by_ref`.
pub trait SecretReader {
    fn read(&self, secret_ref: &str) -> Option<SecretString>;
}

/// Resolves the structured export transform for a connection form field.
///
/// The app layer implements this by holding the driver registry and calling
/// `DbDriver::export_field_transform`. This crate never branches on driver ids.
///
/// The transform is consulted BEFORE the hint in the export per-field loop.
/// When the transform returns `SplitSecret`, the skeleton is written to cleartext
/// fields and the secret is staged into `[secrets]`; the hint path is bypassed.
pub trait ExportTransformResolver {
    fn transform(
        &self,
        profile: &ConnectionProfile,
        field_id: &str,
        values: &FormValues,
    ) -> FieldExportTransform;
}

// ---------------------------------------------------------------------------
// Typed inputs for the export pipeline
// ---------------------------------------------------------------------------

/// An AWS-reflected auth profile reference.
///
/// Reflected (read-only) profiles are never stored and never exported with
/// field values. They travel as a `(provider_id, name)` pair that the importer
/// resolves via `reflect_profiles()` on the target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsRef {
    pub provider_id: String,
    pub name: String,
}

/// A connection together with its driver-extracted form values.
///
/// The app layer calls `driver.extract_values(&profile.config)` for each
/// selected connection and pairs the result here. This avoids requiring the
/// portability crate to hold the driver registry.
pub struct ConnectionWithValues<'a> {
    pub profile: &'a ConnectionProfile,
    /// Form field map produced by `DbDriver::extract_values`.
    pub values: FormValues,
}

/// Graph of typed entities to export, assembled by the app layer from `AppState`.
///
/// Reflected AWS auth profiles are supplied separately as `AwsRef` values; the
/// `auth_profiles` list contains only stored (non-reflected) profiles.
pub struct ExportGraph<'a> {
    pub connections: Vec<ConnectionWithValues<'a>>,
    /// Stored (non-reflected) auth profiles referenced by any connection.
    pub auth_profiles: Vec<&'a AuthProfile>,
    /// Reflected (read-only) AWS auth profile references.
    pub aws_references: Vec<AwsRef>,
    pub ssh_tunnels: Vec<&'a SshTunnelProfile>,
    pub proxies: Vec<&'a ProxyProfile>,
}

/// Whether a sensitive field is included in the bundle or excluded (omitted / required-on-import).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IncludeExclude {
    /// Exclude the field: the secret is omitted from the bundle and recorded as a required_ref.
    #[default]
    Exclude,
    /// Include the field: the secret is staged in `[secrets]` as usual.
    Include,
}

/// Per-auth-profile export mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthExportMode {
    /// Export the profile's secret field values into `[secrets]`.
    IncludeValues,
    /// Export as a mappable reference; no secret values in the bundle.
    ///
    /// The importer surfaces this as a conflict / resolution step.
    #[default]
    MappableReference,
    /// Omit values and emit a required_ref so the importer must supply them.
    RequiredOnImport,
    /// Exclude the profile's secret material entirely.
    Exclude,
}

/// Explicit per-secret override: whether to export or force a required_ref regardless of
/// keyring availability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretExportChoice {
    /// Export the secret as usual (honor the hint/transform resolver).
    Export,
    /// Do not export; emit a required_ref even when the secret is available.
    DoNotExport,
}

/// Options controlling what the export pipeline includes.
#[derive(Debug, Clone)]
pub struct ExportOptions {
    /// Include hook definitions and bindings (off by default; spec R-EXP-1).
    pub include_hooks: bool,

    /// Include `settings_overrides` (off by default; spec R-EXP-1).
    pub include_settings_overrides: bool,

    /// Embed SSH private-key bytes into the encrypted secrets section.
    ///
    /// Requires explicit per-export consent plus a warning at the call site.
    /// When `false`, the key path is omitted and recorded as a `required_ref`.
    pub embed_ssh_keys: bool,

    pub encryption: EncryptionChoice,

    /// Whether to include the connection password in the bundle.
    ///
    /// Default: `Exclude` (safe). Only staged when `Include` is chosen explicitly.
    pub connection_password: IncludeExclude,

    /// Whether to include proxy credentials.
    ///
    /// Default: `Exclude` (safe).
    pub proxy_credentials: IncludeExclude,

    /// Whether to include the SSH tunnel password.
    ///
    /// Distinct from `embed_ssh_keys` which controls private-key bytes.
    /// Default: `Exclude` (safe).
    pub ssh_password: IncludeExclude,

    /// Per-auth-profile export mode, keyed by the profile's UUID.
    ///
    /// Absent entries default to `AuthExportMode::MappableReference`.
    pub auth_modes: std::collections::HashMap<uuid::Uuid, AuthExportMode>,

    /// Explicit per-secret override keyed by `(profile_uuid, field_id)`.
    ///
    /// `DoNotExport` forces a required_ref regardless of keyring read success.
    pub per_secret_overrides: std::collections::HashMap<(uuid::Uuid, String), SecretExportChoice>,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            include_hooks: false,
            include_settings_overrides: false,
            embed_ssh_keys: false,
            encryption: EncryptionChoice::Plaintext { forced: false },
            connection_password: IncludeExclude::Exclude,
            proxy_credentials: IncludeExclude::Exclude,
            ssh_password: IncludeExclude::Exclude,
            auth_modes: std::collections::HashMap::new(),
            per_secret_overrides: std::collections::HashMap::new(),
        }
    }
}

/// How the bundle's secrets section should be encrypted.
#[derive(Debug, Clone)]
pub enum EncryptionChoice {
    /// Encrypt with age passphrase mode (default, recommended).
    Passphrase(SecretString),

    /// Write secrets in cleartext.
    ///
    /// `forced` must be `true`; passing `false` causes `export()` to return
    /// `PortabilityError::PlaintextForceMissing`. This two-step requirement
    /// ensures callers consciously acknowledge the security implications of
    /// writing secrets without encryption.
    Plaintext { forced: bool },
}

/// Summary produced by the export pipeline.
#[derive(Debug, Default)]
pub struct ExportReport {
    /// Human-readable warnings (e.g. LocalPath fields that may not be portable).
    pub warnings: Vec<String>,

    /// Number of required references recorded in the bundle.
    pub required_ref_count: usize,
}

// ---------------------------------------------------------------------------
// Import-side types (T2.6)
// ---------------------------------------------------------------------------

/// Snapshot of entities already present at the import destination.
///
/// The app layer assembles this from `AppState` before calling `plan()`.
pub struct DestSnapshot<'a> {
    pub auth_profiles: Vec<&'a AuthProfile>,
    pub ssh_tunnels: Vec<&'a SshTunnelProfile>,
    pub proxies: Vec<&'a ProxyProfile>,
    /// Existing connection profiles, used by connection-conflict detection (ADR-5).
    pub connections: Vec<&'a ConnectionProfile>,
}

/// Parsed bundle with the plaintext metadata extracted.
///
/// When `bundle.encryption = "age-passphrase"`, the `decrypted_secrets` field
/// is `None` until `decrypt()` is called with the correct passphrase.
pub struct ParsedBundle {
    pub bundle: bundle::Bundle,
    /// Decrypted secrets map (key = namespaced ref string, value = secret value).
    /// `None` until `decrypt()` is called.
    pub decrypted_secrets: Option<HashMap<String, String>>,
}

/// Plan produced by `plan()` describing conflicts and required resolutions.
#[derive(Debug, Default)]
pub struct ImportPlan {
    /// Profile conflicts detected at the destination.
    pub conflicts: Vec<ProfileConflict>,

    /// Required resolutions the user must provide before `apply()` can run.
    pub required_resolutions: Vec<RequiredResolution>,
}

/// A conflict between a bundle profile and an existing profile at the destination.
#[derive(Debug)]
pub struct ProfileConflict {
    /// Bundle local_id of the candidate entry.
    pub bundle_local_id: String,

    pub kind: ConflictKind,

    /// Human-readable name of the bundle entry.
    pub bundle_name: String,

    /// UUID of the existing matching profile at the destination.
    pub existing_id: uuid::Uuid,

    /// Human-readable name of the existing profile at the destination.
    pub existing_name: String,
}

/// Entity type involved in a conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictKind {
    AuthProfile,
    SshTunnel,
    Proxy,
    /// A connection profile whose `(name, driver_id)` natural key matches an
    /// existing profile at the destination. See ADR-5.
    Connection,
}

/// A required value the user must supply before import can proceed.
#[derive(Debug)]
pub struct RequiredResolution {
    /// Identifies which bundle entity this resolution belongs to (connection local_id).
    pub owner_local_id: String,

    /// Human-readable display name of the owning connection.
    ///
    /// Populated from the bundle connection's `name` field so the wizard can
    /// show "which connection needs this secret" without looking up the local_id
    /// in the parsed bundle. See R-WIZ-4 (ADR-3).
    pub owner_name: String,

    /// Field ID within the owner entity.
    pub field: String,

    pub kind: RequiredResolutionKind,
}

/// Category of a required resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequiredResolutionKind {
    /// A secret value (password, token) the exporter omitted.
    Secret,
    /// An AWS-reflected auth reference that could not be auto-resolved on the target.
    AwsReference { provider_id: String, name: String },
    /// An auth profile reference that the recipient must create or select.
    AuthProfileRef,
}

/// User-supplied choices for each conflict and required resolution.
#[derive(Debug, Default)]
pub struct ResolutionChoices {
    /// Conflict choices keyed by `bundle_local_id`.
    pub conflict_choices: HashMap<String, ConflictChoice>,

    /// Secret values keyed by `(owner_local_id, field)`.
    pub secret_values: HashMap<(String, String), SecretString>,

    /// Auth profile IDs chosen for required auth-profile resolutions,
    /// keyed by `(owner_local_id, field)`. The UUID is the destination ID.
    pub auth_profile_choices: HashMap<(String, String), uuid::Uuid>,
}

/// How a conflict should be resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictChoice {
    /// Reuse the existing destination profile; do not create a new one.
    Reuse,
    /// Create a new profile with the bundle data; keep the existing one.
    CreateNew,
    /// Map the bundle reference onto a specific existing profile at the destination.
    MapTo(uuid::Uuid),
}

/// Actions produced by `apply()`.
///
/// Pure output: all UUIDs are freshly minted and references are rewired.
/// The app layer persists these through repositories and `SecretManager::set_by_ref`.
pub struct ImportActions {
    pub connections: Vec<ConnectionProfile>,
    pub auth_profiles: Vec<AuthProfile>,
    pub ssh_tunnels: Vec<SshTunnelProfile>,
    pub proxies: Vec<ProxyProfile>,

    /// Secret writes to perform: `(namespaced_ref, secret_value)`.
    ///
    /// Each entry must be written via `SecretManager::set_by_ref`. The returned
    /// `bool` must be checked; a `false` means the keyring write failed and
    /// must be surfaced as a user-facing error.
    pub secret_writes: Vec<(String, SecretString)>,
}
