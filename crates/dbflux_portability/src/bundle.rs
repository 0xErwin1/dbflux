/// Current bundle format version. Bumped on incompatible schema changes.
pub const CURRENT_FORMAT_VERSION: u32 = 1;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Top-level bundle document.
///
/// The entire document is valid TOML. The `[secrets]` section is either a
/// plaintext inline table (`encryption = "none"`) or an age-passphrase ASCII
/// armor blob stored as a TOML string (`encryption = "age-passphrase"`). Every
/// other section is always cleartext and inspectable without a passphrase.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Bundle {
    pub bundle: BundleMeta,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub drivers: Vec<DriverRef>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<ConnectionEntry>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub auth_profiles: Vec<AuthEntry>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ssh_tunnels: Vec<SshEntry>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proxies: Vec<ProxyEntry>,

    /// Encrypted or plaintext secrets section.
    ///
    /// When `bundle.encryption = "age-passphrase"` this field holds the raw age
    /// ASCII armor ciphertext as a single TOML string. When `encryption = "none"`
    /// it holds the secrets as a cleartext TOML inline table. Absent when no
    /// secrets were staged for export.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secrets: Option<SecretsSection>,
}

/// Bundle-level metadata written to the `[bundle]` table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BundleMeta {
    pub format_version: u32,

    /// RFC-3339 timestamp at which the bundle was created.
    pub created_at: String,

    /// DBFlux version that wrote the bundle (informational).
    pub dbflux_version: String,

    /// Encryption mode for the `[secrets]` section.
    pub encryption: EncryptionMode,
}

/// Encryption mode declared in the bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EncryptionMode {
    /// Secrets section is age-passphrase encrypted (scrypt KDF + AEAD).
    AgePassphrase,

    /// Secrets section is written in cleartext. Requires explicit force opt-in
    /// plus a prominent warning at export time.
    None,
}

/// Informational driver identity. Never used to gate import.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DriverRef {
    /// Format: `built-in:<driver>` or `external:<socket_id>`.
    pub reference: String,
}

/// A single connection in the bundle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectionEntry {
    /// Bundle-local identifier; remapped to a fresh UUID on import.
    pub local_id: String,

    pub name: String,

    /// Driver identifier (e.g. `"postgres"`, `"rpc:<socket_id>"`).
    pub driver_id: String,

    /// Include-hinted form field values (cleartext).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub fields: HashMap<String, String>,

    /// LocalPath-hinted field values (cleartext, with portability warning).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub local_path_fields: HashMap<String, String>,

    /// Required references that the recipient must resolve at import time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_refs: Vec<RequiredRef>,

    /// AWS-reflected auth profile reference (kind = aws_reference).
    ///
    /// Present when the connection references a reflected (read-only) auth
    /// profile. The reference carries `provider_id` + `name` only; no secret
    /// material. Resolved on the target by calling `reflect_profiles()`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_ref: Option<AuthRef>,

    /// Local ID of the referenced stored (non-reflected) auth profile, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_profile_local_id: Option<String>,

    /// Access binding: None (direct), ssh, proxy, or managed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<AccessEntry>,

    /// Dynamic value references (SSM/Secrets Manager/env paths).
    /// Included by default; may not resolve on the target (non-blocking).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub value_refs: HashMap<String, toml::Value>,

    /// Whether hooks were included for this connection (opt-in).
    #[serde(default)]
    pub include_hooks: bool,

    /// Whether settings_overrides were included for this connection (opt-in).
    #[serde(default)]
    pub include_settings_overrides: bool,

    /// Serialized hooks payload (present only when `include_hooks = true`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hooks_payload: Option<toml::Value>,

    /// Serialized settings_overrides payload (present only when `include_settings_overrides = true`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settings_overrides_payload: Option<toml::Value>,
}

/// Connection access binding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AccessEntry {
    Ssh {
        /// Local ID of the referenced SSH tunnel profile.
        ssh_local_id: String,
    },
    Proxy {
        /// Local ID of the referenced proxy profile.
        proxy_local_id: String,
    },
    Managed {
        provider: String,
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        params: HashMap<String, String>,
    },
}

/// A stored (non-reflected) auth profile entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuthEntry {
    pub local_id: String,
    pub name: String,
    pub provider_id: String,
    pub enabled: bool,

    /// Non-secret fields.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub fields: HashMap<String, String>,

    /// Secret-kind field names whose values are staged in `[secrets]` under
    /// `auth:<local_id>:<field>` keys.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secret_field_names: Vec<String>,

    /// Required references that the recipient must resolve at import time
    /// (e.g. a secret field the exporter could not read from the keyring).
    /// Provides parity with `SshEntry` and `ProxyEntry` so the importer learns
    /// exactly which auth field was omitted, not just that some secret was missing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_refs: Vec<RequiredRef>,
}

/// An SSH tunnel profile entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SshEntry {
    pub local_id: String,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub user: String,

    /// `"private_key"` or `"password"`. When private_key and `embed_ssh_keys = true`
    /// the key bytes are in `[secrets]` under `ssh_tunnel:<local_id>:private_key`.
    pub auth_method: SshAuthMethodKind,

    /// Whether key bytes are embedded in `[secrets]` (only when auth_method = private_key
    /// and `embed_ssh_keys` was opted in). When false the key path becomes a required_ref.
    #[serde(default)]
    pub key_embedded: bool,

    /// Required references that the recipient must resolve at import time
    /// (e.g. missing key or password that the exporter could not stage).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_refs: Vec<RequiredRef>,
}

/// Serialized SSH auth method kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SshAuthMethodKind {
    PrivateKey,
    Password,
}

/// A proxy profile entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProxyEntry {
    pub local_id: String,
    pub name: String,
    pub kind: String,
    pub host: String,
    pub port: u16,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_proxy: Option<String>,

    /// Whether proxy credentials are staged in `[secrets]`.
    #[serde(default)]
    pub has_secret: bool,

    /// Required references that the recipient must resolve at import time
    /// (e.g. missing credential that the exporter could not stage).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_refs: Vec<RequiredRef>,
}

/// A required reference: a field the recipient must supply or resolve at import.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RequiredRef {
    /// Form field ID.
    pub field: String,

    /// Category of the required value.
    pub kind: RequiredRefKind,
}

/// Category of a required reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequiredRefKind {
    /// A secret value (password, token, key) the exporter omitted.
    Secret,
    /// A reference to an auth profile (e.g. AWS named profile) the recipient must supply.
    AuthProfile,
}

/// An AWS-reflected auth profile reference.
///
/// No secret material. The reference is resolved on the target by calling
/// `reflect_profiles()` for the given provider; if not found, it enters
/// the unified required-resolution step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuthRef {
    pub kind: AuthRefKind,
    pub provider_id: String,
    pub name: String,
}

/// Kind tag for an auth reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthRefKind {
    AwsReference,
}

/// Secrets section of the bundle.
///
/// When `encryption = "age-passphrase"`, the `ciphertext` field holds the
/// raw age ASCII-armor blob. When `encryption = "none"` (plaintext force),
/// the `plaintext` field holds the key-value map directly.
///
/// `Debug` is implemented manually to redact the `Plaintext` map: printing
/// cleartext secrets via `{:?}` would leak them into logs and panic messages.
/// The `Encrypted` arm is safe to print as-is (ciphertext carries no plaintext).
#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum SecretsSection {
    /// Age-encrypted blob (ASCII armor stored as a TOML string).
    Encrypted { ciphertext: String },
    /// Plaintext map (only written with explicit force opt-in + user warning).
    Plaintext { values: HashMap<String, String> },
}

impl std::fmt::Debug for SecretsSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecretsSection::Encrypted { ciphertext } => f
                .debug_struct("Encrypted")
                .field("ciphertext", ciphertext)
                .finish(),
            SecretsSection::Plaintext { values } => f
                .debug_struct("Plaintext")
                .field(
                    "values",
                    &format_args!("<redacted {} entries>", values.len()),
                )
                .finish(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn make_bundle(encryption: EncryptionMode) -> Bundle {
        Bundle {
            bundle: BundleMeta {
                format_version: CURRENT_FORMAT_VERSION,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                dbflux_version: "0.7.0-dev.0".to_string(),
                encryption,
            },
            drivers: vec![DriverRef {
                reference: "built-in:postgres".to_string(),
            }],
            connections: vec![ConnectionEntry {
                local_id: "aaaaaaaa-0000-0000-0000-000000000001".to_string(),
                name: "Prod PG".to_string(),
                driver_id: "postgres".to_string(),
                fields: {
                    let mut m = HashMap::new();
                    m.insert("host".to_string(), "db.internal".to_string());
                    m.insert("port".to_string(), "5432".to_string());
                    m
                },
                local_path_fields: HashMap::new(),
                required_refs: vec![RequiredRef {
                    field: "password".to_string(),
                    kind: RequiredRefKind::Secret,
                }],
                auth_ref: Some(AuthRef {
                    kind: AuthRefKind::AwsReference,
                    provider_id: "aws-sso".to_string(),
                    name: "My AWS SSO".to_string(),
                }),
                auth_profile_local_id: None,
                access: Some(AccessEntry::Ssh {
                    ssh_local_id: "bbbbbbbb-0000-0000-0000-000000000002".to_string(),
                }),
                value_refs: HashMap::new(),
                include_hooks: false,
                include_settings_overrides: false,
                hooks_payload: None,
                settings_overrides_payload: None,
            }],
            auth_profiles: vec![],
            ssh_tunnels: vec![SshEntry {
                local_id: "bbbbbbbb-0000-0000-0000-000000000002".to_string(),
                name: "Bastion".to_string(),
                host: "bastion.example.com".to_string(),
                port: 22,
                user: "ec2-user".to_string(),
                auth_method: SshAuthMethodKind::PrivateKey,
                key_embedded: false,
                required_refs: vec![],
            }],
            proxies: vec![],
            secrets: None,
        }
    }

    #[test]
    fn bundle_round_trips_without_secrets() {
        let original = make_bundle(EncryptionMode::None);

        let serialized = toml::to_string(&original).expect("serialize");
        let deserialized: Bundle = toml::from_str(&serialized).expect("deserialize");

        assert_eq!(original, deserialized);
    }

    #[test]
    fn bundle_round_trips_with_plaintext_secrets() {
        let mut bundle = make_bundle(EncryptionMode::None);
        bundle.secrets = Some(SecretsSection::Plaintext {
            values: {
                let mut m = HashMap::new();
                m.insert(
                    "conn:aaaaaaaa-0000-0000-0000-000000000001:password".to_string(),
                    "s3cr3t".to_string(),
                );
                m
            },
        });

        let serialized = toml::to_string(&bundle).expect("serialize");
        let deserialized: Bundle = toml::from_str(&serialized).expect("deserialize");

        assert_eq!(bundle, deserialized);
    }

    #[test]
    fn bundle_round_trips_with_encrypted_ciphertext() {
        let mut bundle = make_bundle(EncryptionMode::AgePassphrase);
        bundle.secrets = Some(SecretsSection::Encrypted {
            ciphertext: "age-encryption-payload-ascii-armor".to_string(),
        });

        let serialized = toml::to_string(&bundle).expect("serialize");
        let deserialized: Bundle = toml::from_str(&serialized).expect("deserialize");

        assert_eq!(bundle, deserialized);
    }

    #[test]
    fn encryption_mode_serializes_kebab_case() {
        let meta = BundleMeta {
            format_version: CURRENT_FORMAT_VERSION,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            dbflux_version: "0.7.0-dev.0".to_string(),
            encryption: EncryptionMode::AgePassphrase,
        };
        let s = toml::to_string(&meta).expect("serialize");
        assert!(s.contains("age-passphrase"), "got: {s}");

        let none = BundleMeta {
            encryption: EncryptionMode::None,
            ..meta
        };
        let s2 = toml::to_string(&none).expect("serialize");
        assert!(
            s2.contains("\"none\""),
            "EncryptionMode::None must serialize as 'none', got: {s2}"
        );
    }

    #[test]
    fn bundle_meta_format_version_is_current() {
        let meta = BundleMeta {
            format_version: CURRENT_FORMAT_VERSION,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            dbflux_version: "0.7.0-dev.0".to_string(),
            encryption: EncryptionMode::None,
        };
        assert_eq!(meta.format_version, 1);
    }

    #[test]
    fn required_ref_kind_serializes_snake_case() {
        let r = RequiredRef {
            field: "password".to_string(),
            kind: RequiredRefKind::Secret,
        };
        let v = toml::Value::try_from(&r).expect("convert");
        let s = toml::to_string(&v).expect("serialize");
        assert!(s.contains("secret"), "got: {s}");
    }
}
