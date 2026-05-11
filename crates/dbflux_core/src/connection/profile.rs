use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::access::AccessKind;
use crate::config::app::GlobalOverrides;
use crate::connection::hook::{ConnectionHookBindings, ConnectionHooks};
use crate::driver::form::FormValues;
use crate::values::ValueRef;

/// Supported database types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DbKind {
    Postgres,
    SQLite,
    MySQL,
    MariaDB,
    MongoDB,
    Redis,
    DynamoDB,
    CloudWatchLogs,
}

impl DbKind {
    pub fn display_name(&self) -> &'static str {
        match self {
            DbKind::Postgres => "PostgreSQL",
            DbKind::SQLite => "SQLite",
            DbKind::MySQL => "MySQL",
            DbKind::MariaDB => "MariaDB",
            DbKind::MongoDB => "MongoDB",
            DbKind::Redis => "Redis",
            DbKind::DynamoDB => "DynamoDB",
            DbKind::CloudWatchLogs => "CloudWatch Logs",
        }
    }
}

/// Returns `true` when the given SSL mode id string requires a root CA certificate.
///
/// Covers all known cert-requiring ids across every driver (Postgres, MySQL, MongoDB, Redis).
/// The UI uses this to decide whether to reveal the CA certificate path input.
pub fn ssl_mode_id_requires_root_cert(id: &str) -> bool {
    matches!(
        id,
        "verify-ca" | "verify-full" | "VERIFY_CA" | "VERIFY_IDENTITY" | "verify"
    )
}

/// Returns `true` when the given SSL mode id string indicates an active (non-disabled,
/// non-preferred) SSL connection that warrants showing client certificate inputs.
///
/// Used by the UI to decide whether to show client cert / key path inputs.
pub fn ssl_mode_id_is_cert_active(id: &str) -> bool {
    !matches!(
        id,
        "disable" | "DISABLED" | "off" | "prefer" | "PREFERRED" | "allow" | ""
    )
}

/// Deserializes an SSL mode field that may be stored in either the new format
/// (`Option<String>` with a driver-native id like `"prefer"`) or the legacy format
/// (a bare enum variant name like `"Disable"`, `"VerifyCa"`, etc.).
///
/// On load, legacy enum variant names are normalised to the canonical Postgres id strings
/// so that subsequent saves write the new format.
/// Serde helper for the legacy Redis `tls` field. Used as `skip_serializing_if` so new
/// saves never include `tls: false` in the output.
fn is_false(value: &bool) -> bool {
    !*value
}

fn deserialize_ssl_mode_option<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: Option<serde_json::Value> = Option::deserialize(deserializer)?;

    let Some(value) = raw else {
        return Ok(None);
    };

    let s = match &value {
        serde_json::Value::String(s) => s.clone(),
        // Numeric or boolean — shouldn't happen but be defensive.
        other => {
            return Err(serde::de::Error::custom(format!(
                "unexpected ssl_mode value: {other}"
            )));
        }
    };

    // Normalise legacy PascalCase enum variant names → canonical Postgres id strings.
    let normalised = match s.as_str() {
        "Disable" => "disable",
        "Allow" => "allow",
        "Prefer" => "prefer",
        "Require" => "require",
        "VerifyCa" => "verify-ca",
        "VerifyFull" => "verify-full",
        other => other,
    };

    Ok(Some(normalised.to_string()))
}

/// SSL/TLS mode for database connections.
///
/// Deprecated: new code should store SSL mode as `Option<String>` using the driver's native
/// id strings and use `ssl_mode_id_requires_root_cert` for cert-visibility decisions.
/// This enum is retained only for tests that were written against the old API.
#[deprecated(
    since = "0.0.0",
    note = "Use Option<String> ssl_mode with driver-native ids"
)]
#[allow(deprecated)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SslMode {
    /// No SSL (unencrypted connection).
    Disable,

    /// Try SSL but also allow plain connections if the server doesn't support it.
    Allow,

    /// Try SSL, fall back to unencrypted if unavailable.
    #[default]
    Prefer,

    /// Require SSL (fail if server doesn't support it), but do not verify the certificate.
    Require,

    /// Require SSL and verify that the server certificate is signed by a trusted CA.
    VerifyCa,

    /// Require SSL, verify the CA, and also verify that the server hostname matches the certificate.
    VerifyFull,
}

#[allow(deprecated)]
impl SslMode {
    /// Returns the lowercase string id used in driver metadata and the UI segmented control.
    pub fn as_id(&self) -> &'static str {
        match self {
            SslMode::Disable => "disable",
            SslMode::Allow => "allow",
            SslMode::Prefer => "prefer",
            SslMode::Require => "require",
            SslMode::VerifyCa => "verify-ca",
            SslMode::VerifyFull => "verify-full",
        }
    }
}

/// Returns `true` when the given mode requires a root CA certificate to be provided.
///
/// Deprecated: use `ssl_mode_id_requires_root_cert` with string ids instead.
#[deprecated(
    since = "0.0.0",
    note = "Use ssl_mode_id_requires_root_cert(&str) instead"
)]
#[allow(deprecated)]
pub fn ssl_mode_requires_root_cert(mode: SslMode) -> bool {
    matches!(mode, SslMode::VerifyCa | SslMode::VerifyFull)
}

/// Maps a lowercase SSL mode id string to `SslMode`.
///
/// Deprecated: drivers now use `Option<String>` for ssl_mode; this helper is retained only
/// for backwards-compatible callers.
#[deprecated(since = "0.0.0", note = "Drivers now use Option<String> ssl_mode")]
#[allow(deprecated)]
pub fn ssl_mode_from_id(id: &str) -> Option<SslMode> {
    match id {
        "disable" => Some(SslMode::Disable),
        "allow" => Some(SslMode::Allow),
        "prefer" => Some(SslMode::Prefer),
        "require" => Some(SslMode::Require),
        "verify-ca" => Some(SslMode::VerifyCa),
        "verify-full" => Some(SslMode::VerifyFull),
        _ => None,
    }
}

/// SSH authentication method.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SshAuthMethod {
    /// Authenticate using a private key file.
    /// The passphrase (if any) is stored in the keyring.
    PrivateKey {
        /// Path to the private key file. If `None`, uses SSH agent or default keys (~/.ssh/id_rsa).
        key_path: Option<PathBuf>,
    },

    /// Authenticate using a password.
    /// The password is stored in the keyring.
    Password,
}

impl Default for SshAuthMethod {
    fn default() -> Self {
        SshAuthMethod::PrivateKey { key_path: None }
    }
}

/// SSH tunnel configuration for connecting through a bastion host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshTunnelConfig {
    /// SSH server hostname.
    pub host: String,

    /// SSH server port (typically 22).
    pub port: u16,

    /// SSH username.
    pub user: String,

    /// Authentication method (private key or password).
    #[serde(default)]
    pub auth_method: SshAuthMethod,
}

/// Saved SSH tunnel profile for reuse across connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshTunnelProfile {
    pub id: Uuid,
    pub name: String,
    pub config: SshTunnelConfig,
    #[serde(default)]
    pub save_secret: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionMcpPolicyBinding {
    pub actor_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub role_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policy_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionMcpGovernance {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policy_bindings: Vec<ConnectionMcpPolicyBinding>,
}

impl SshTunnelProfile {
    pub fn new(name: impl Into<String>, config: SshTunnelConfig) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            config,
            save_secret: true,
        }
    }

    pub fn secret_ref(&self) -> String {
        crate::storage::secrets::ssh_tunnel_secret_ref(&self.id)
    }
}

/// Database-specific connection parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DbConfig {
    Postgres {
        #[serde(default)]
        use_uri: bool,
        #[serde(default)]
        uri: Option<String>,
        host: String,
        port: u16,
        user: String,
        database: String,
        /// SSL mode using the Postgres native sslmode identifier (e.g. `"prefer"`, `"verify-ca"`).
        ///
        /// Accepts both the new `Option<String>` format and legacy bare enum variant names
        /// (`"Prefer"`, `"VerifyCa"`) written by older versions.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "deserialize_ssl_mode_option"
        )]
        ssl_mode: Option<String>,
        /// Path to the root CA certificate file for `verify-ca` / `verify-full` modes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ssl_root_cert_path: Option<String>,
        /// Path to the client certificate file for mutual TLS.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ssl_client_cert_path: Option<String>,
        /// Path to the client private key file for mutual TLS.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ssl_client_key_path: Option<String>,
        ssh_tunnel: Option<SshTunnelConfig>,
        #[serde(default)]
        ssh_tunnel_profile_id: Option<Uuid>,
    },
    SQLite {
        path: PathBuf,
        /// Optional connection ID for in-memory databases.
        /// Without a connection ID, each connection to `:memory:` creates a new isolated database.
        /// With a connection ID, connections are pooled and shared.
        #[serde(default)]
        connection_id: Option<String>,
    },
    MySQL {
        #[serde(default)]
        use_uri: bool,
        #[serde(default)]
        uri: Option<String>,
        host: String,
        port: u16,
        user: String,
        database: Option<String>,
        /// SSL mode using the MySQL native ssl-mode identifier (e.g. `"PREFERRED"`, `"VERIFY_CA"`).
        ///
        /// Accepts both the new `Option<String>` format and legacy bare enum variant names
        /// (`"Prefer"`, `"VerifyCa"`) written by older versions.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "deserialize_ssl_mode_option"
        )]
        ssl_mode: Option<String>,
        /// Path to the root CA certificate file for `VERIFY_CA` / `VERIFY_IDENTITY` modes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ssl_root_cert_path: Option<String>,
        /// Path to the client certificate file for mutual TLS.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ssl_client_cert_path: Option<String>,
        /// Path to the client private key file for mutual TLS.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ssl_client_key_path: Option<String>,
        ssh_tunnel: Option<SshTunnelConfig>,
        #[serde(default)]
        ssh_tunnel_profile_id: Option<Uuid>,
    },
    MongoDB {
        /// When true, use `uri` field directly. When false, construct URI from host/port.
        #[serde(default)]
        use_uri: bool,
        /// Raw connection URI (used when use_uri=true).
        #[serde(default)]
        uri: Option<String>,
        /// Host (used when use_uri=false).
        host: String,
        /// Port (used when use_uri=false).
        port: u16,
        user: Option<String>,
        database: Option<String>,
        /// Authentication database (defaults to "admin" if user is specified).
        #[serde(default)]
        auth_database: Option<String>,
        /// SSL mode id: `"off"`, `"on"` (TLS without verification), or `"verify"` (TLS with cert verification).
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "deserialize_ssl_mode_option"
        )]
        ssl_mode: Option<String>,
        /// Path to the root CA certificate file for `verify` mode.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ssl_root_cert_path: Option<String>,
        /// Path to the client certificate file for mutual TLS.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ssl_client_cert_path: Option<String>,
        /// Path to the client private key file for mutual TLS.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ssl_client_key_path: Option<String>,
        ssh_tunnel: Option<SshTunnelConfig>,
        #[serde(default)]
        ssh_tunnel_profile_id: Option<Uuid>,
    },
    Redis {
        #[serde(default)]
        use_uri: bool,
        #[serde(default)]
        uri: Option<String>,
        host: String,
        port: u16,
        user: Option<String>,
        /// Redis logical database index.
        #[serde(default)]
        database: Option<u32>,
        /// Legacy TLS toggle from older versions. Read-only migration field: on load,
        /// when `ssl_mode` is `None`, `tls=true` is interpreted as `"on"` and `tls=false`
        /// as `"off"`. Never written by new saves (`skip_serializing_if`).
        #[serde(default, skip_serializing_if = "is_false")]
        tls: bool,
        /// SSL mode id: `"off"`, `"on"` (TLS without verification), or `"verify"` (TLS with cert verification).
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "deserialize_ssl_mode_option"
        )]
        ssl_mode: Option<String>,
        /// Path to the root CA certificate file for `verify` mode.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ssl_root_cert_path: Option<String>,
        /// Path to the client certificate file for mutual TLS.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ssl_client_cert_path: Option<String>,
        /// Path to the client private key file for mutual TLS.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ssl_client_key_path: Option<String>,
        ssh_tunnel: Option<SshTunnelConfig>,
        #[serde(default)]
        ssh_tunnel_profile_id: Option<Uuid>,
    },
    DynamoDB {
        region: String,
        #[serde(default)]
        profile: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        table: Option<String>,
    },
    CloudWatchLogs {
        region: String,
        #[serde(default)]
        profile: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
    },
    /// Generic config for external RPC drivers.
    External {
        kind: DbKind,
        #[serde(default)]
        values: FormValues,
    },
}

impl DbConfig {
    /// Returns the base database kind for this config.
    ///
    /// Note: For MySQL configs, this always returns `DbKind::MySQL`.
    /// Use `ConnectionProfile::kind()` to get the actual kind (MySQL vs MariaDB).
    pub fn kind(&self) -> DbKind {
        match self {
            DbConfig::Postgres { .. } => DbKind::Postgres,
            DbConfig::SQLite { .. } => DbKind::SQLite,
            DbConfig::MySQL { .. } => DbKind::MySQL,
            DbConfig::MongoDB { .. } => DbKind::MongoDB,
            DbConfig::Redis { .. } => DbKind::Redis,
            DbConfig::DynamoDB { .. } => DbKind::DynamoDB,
            DbConfig::CloudWatchLogs { .. } => DbKind::CloudWatchLogs,
            DbConfig::External { kind, .. } => *kind,
        }
    }

    pub fn default_postgres() -> Self {
        DbConfig::Postgres {
            use_uri: false,
            uri: None,
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            database: "postgres".to_string(),
            ssl_mode: Some("prefer".to_string()),
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        }
    }

    pub fn default_sqlite() -> Self {
        DbConfig::SQLite {
            path: PathBuf::new(),
            connection_id: None,
        }
    }

    pub fn default_mysql() -> Self {
        DbConfig::MySQL {
            use_uri: false,
            uri: None,
            host: "localhost".to_string(),
            port: 3306,
            user: "root".to_string(),
            database: None,
            ssl_mode: Some("PREFERRED".to_string()),
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        }
    }

    pub fn default_mongodb() -> Self {
        DbConfig::MongoDB {
            use_uri: false,
            uri: None,
            host: "localhost".to_string(),
            port: 27017,
            user: None,
            database: None,
            auth_database: None,
            ssl_mode: Some("off".to_string()),
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        }
    }

    pub fn default_redis() -> Self {
        DbConfig::Redis {
            use_uri: false,
            uri: None,
            host: "localhost".to_string(),
            port: 6379,
            user: None,
            database: Some(0),
            tls: false,
            ssl_mode: Some("off".to_string()),
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        }
    }

    pub fn default_dynamodb() -> Self {
        DbConfig::DynamoDB {
            region: "us-east-1".to_string(),
            profile: None,
            endpoint: None,
            table: None,
        }
    }

    pub fn default_cloudwatch_logs() -> Self {
        DbConfig::CloudWatchLogs {
            region: "us-east-1".to_string(),
            profile: None,
            endpoint: None,
        }
    }

    pub fn ssh_tunnel(&self) -> Option<&SshTunnelConfig> {
        match self {
            DbConfig::Postgres { ssh_tunnel, .. }
            | DbConfig::MySQL { ssh_tunnel, .. }
            | DbConfig::MongoDB { ssh_tunnel, .. }
            | DbConfig::Redis { ssh_tunnel, .. } => ssh_tunnel.as_ref(),
            DbConfig::SQLite { .. }
            | DbConfig::DynamoDB { .. }
            | DbConfig::CloudWatchLogs { .. }
            | DbConfig::External { .. } => None,
        }
    }

    /// Returns the SSH tunnel profile ID referenced by this config, if any.
    pub fn ssh_tunnel_profile_id(&self) -> Option<Uuid> {
        match self {
            DbConfig::Postgres {
                ssh_tunnel_profile_id,
                ..
            }
            | DbConfig::MySQL {
                ssh_tunnel_profile_id,
                ..
            }
            | DbConfig::MongoDB {
                ssh_tunnel_profile_id,
                ..
            }
            | DbConfig::Redis {
                ssh_tunnel_profile_id,
                ..
            } => *ssh_tunnel_profile_id,
            DbConfig::SQLite { .. }
            | DbConfig::DynamoDB { .. }
            | DbConfig::CloudWatchLogs { .. }
            | DbConfig::External { .. } => None,
        }
    }

    /// Whether this config has any SSH tunnel configured (inline or via profile reference).
    pub fn has_ssh_tunnel(&self) -> bool {
        match self {
            DbConfig::Postgres {
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            }
            | DbConfig::MySQL {
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            }
            | DbConfig::MongoDB {
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            }
            | DbConfig::Redis {
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => ssh_tunnel.is_some() || ssh_tunnel_profile_id.is_some(),
            DbConfig::SQLite { .. }
            | DbConfig::DynamoDB { .. }
            | DbConfig::CloudWatchLogs { .. }
            | DbConfig::External { .. } => false,
        }
    }

    /// Target host and port for tunnel forwarding. `None` for SQLite/external.
    pub fn host_port(&self) -> Option<(&str, u16)> {
        match self {
            DbConfig::Postgres { host, port, .. }
            | DbConfig::MySQL { host, port, .. }
            | DbConfig::MongoDB { host, port, .. }
            | DbConfig::Redis { host, port, .. } => Some((host, *port)),
            DbConfig::SQLite { .. }
            | DbConfig::DynamoDB { .. }
            | DbConfig::CloudWatchLogs { .. }
            | DbConfig::External { .. } => None,
        }
    }

    /// Rewrites host/port to a local tunnel endpoint and disables `use_uri`.
    pub fn redirect_to_tunnel(&mut self, tunnel_port: u16) {
        match self {
            DbConfig::Postgres {
                host,
                port,
                use_uri,
                ..
            }
            | DbConfig::MySQL {
                host,
                port,
                use_uri,
                ..
            }
            | DbConfig::MongoDB {
                host,
                port,
                use_uri,
                ..
            }
            | DbConfig::Redis {
                host,
                port,
                use_uri,
                ..
            } => {
                *host = "127.0.0.1".to_string();
                *port = tunnel_port;
                *use_uri = false;
            }
            DbConfig::SQLite { .. }
            | DbConfig::DynamoDB { .. }
            | DbConfig::CloudWatchLogs { .. }
            | DbConfig::External { .. } => {}
        }
    }

    /// Strips a password embedded in the URI for URI-mode configs.
    ///
    /// Returns the extracted password when one was present and updates the
    /// stored URI in-place to a sanitized form without the password.
    pub fn strip_uri_password(&mut self) -> Option<String> {
        let (use_uri, uri) = match self {
            DbConfig::Postgres { use_uri, uri, .. }
            | DbConfig::MySQL { use_uri, uri, .. }
            | DbConfig::MongoDB { use_uri, uri, .. }
            | DbConfig::Redis { use_uri, uri, .. } => (use_uri, uri),
            DbConfig::SQLite { .. }
            | DbConfig::DynamoDB { .. }
            | DbConfig::CloudWatchLogs { .. }
            | DbConfig::External { .. } => {
                return None;
            }
        };

        if !*use_uri {
            return None;
        }

        let current_uri = uri.as_deref()?;

        let (sanitized_uri, extracted_password) = strip_password_from_uri(current_uri);

        if extracted_password.is_some() {
            *uri = Some(sanitized_uri);
        }

        extracted_password
    }

    /// Returns the database name for configs that support it.
    /// Returns `None` for SQLite, DynamoDB, and External configs.
    pub fn database(&self) -> Option<String> {
        match self {
            DbConfig::Postgres { database, .. } => Some(database.clone()),
            DbConfig::MySQL { database, .. } => database.clone(),
            DbConfig::MongoDB { database, .. } => database.clone(),
            DbConfig::Redis { database, .. } => database.map(|d| d.to_string()),
            DbConfig::SQLite { .. } => Some("main".to_string()),
            DbConfig::DynamoDB { .. } | DbConfig::CloudWatchLogs { .. } => None,
            DbConfig::External { .. } => None,
        }
    }

    /// Returns a new DbConfig with the database field updated.
    /// Returns `Err` if the database type doesn't support changing the database.
    pub fn with_database(self, database: &str) -> Result<Self, String> {
        match self {
            DbConfig::Postgres {
                use_uri,
                uri,
                host,
                port,
                user,
                ssl_mode,
                ssl_root_cert_path,
                ssl_client_cert_path,
                ssl_client_key_path,
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => Ok(DbConfig::Postgres {
                use_uri,
                uri,
                host,
                port,
                user,
                database: database.to_string(),
                ssl_mode,
                ssl_root_cert_path,
                ssl_client_cert_path,
                ssl_client_key_path,
                ssh_tunnel,
                ssh_tunnel_profile_id,
            }),
            DbConfig::MySQL {
                use_uri,
                uri,
                host,
                port,
                user,
                ssl_mode,
                ssl_root_cert_path,
                ssl_client_cert_path,
                ssl_client_key_path,
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => Ok(DbConfig::MySQL {
                use_uri,
                uri,
                host,
                port,
                user,
                database: Some(database.to_string()),
                ssl_mode,
                ssl_root_cert_path,
                ssl_client_cert_path,
                ssl_client_key_path,
                ssh_tunnel,
                ssh_tunnel_profile_id,
            }),
            DbConfig::MongoDB {
                use_uri,
                uri,
                host,
                port,
                user,
                auth_database,
                ssl_mode,
                ssl_root_cert_path,
                ssl_client_cert_path,
                ssl_client_key_path,
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => Ok(DbConfig::MongoDB {
                use_uri,
                uri,
                host,
                port,
                user,
                database: Some(database.to_string()),
                auth_database,
                ssl_mode,
                ssl_root_cert_path,
                ssl_client_cert_path,
                ssl_client_key_path,
                ssh_tunnel,
                ssh_tunnel_profile_id,
            }),
            DbConfig::Redis {
                use_uri,
                uri,
                host,
                port,
                user,
                tls,
                ssl_mode,
                ssl_root_cert_path,
                ssl_client_cert_path,
                ssl_client_key_path,
                ssh_tunnel,
                ssh_tunnel_profile_id,
                ..
            } => {
                let db_index: u32 = database
                    .parse()
                    .map_err(|_| "Invalid database index for Redis (must be a number 0-15)")?;
                Ok(DbConfig::Redis {
                    use_uri,
                    uri,
                    host,
                    port,
                    user,
                    database: Some(db_index),
                    tls,
                    ssl_mode,
                    ssl_root_cert_path,
                    ssl_client_cert_path,
                    ssl_client_key_path,
                    ssh_tunnel,
                    ssh_tunnel_profile_id,
                })
            }
            _ => Err("Changing database is not supported for this database type".to_string()),
        }
    }
}

/// Removes an embedded password from a connection URI.
///
/// Returns `(sanitized_uri, extracted_password)` where `extracted_password` is
/// URL-decoded when possible.
pub fn strip_password_from_uri(uri: &str) -> (String, Option<String>) {
    let Some(scheme_end) = uri.find("://") else {
        return (uri.to_string(), None);
    };

    let prefix = &uri[..scheme_end + 3];
    let rest = &uri[scheme_end + 3..];

    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());

    let authority = &rest[..authority_end];
    let suffix = &rest[authority_end..];

    let Some(at_pos) = authority.rfind('@') else {
        return (uri.to_string(), None);
    };

    let userinfo = &authority[..at_pos];
    let host_port = &authority[at_pos + 1..];

    let Some(colon_pos) = userinfo.find(':') else {
        return (uri.to_string(), None);
    };

    let username = &userinfo[..colon_pos];
    let password = &userinfo[colon_pos + 1..];

    if password.is_empty() {
        return (uri.to_string(), None);
    }

    let decoded_password = urlencoding::decode(password)
        .map(|cow| cow.into_owned())
        .unwrap_or_else(|_| password.to_string());

    let sanitized_authority = if username.is_empty() {
        host_port.to_string()
    } else {
        format!("{}@{}", username, host_port)
    };

    (
        format!("{}{}{}", prefix, sanitized_authority, suffix),
        Some(decoded_password),
    )
}

/// Saved connection profile.
///
/// Persisted to disk as JSON. Passwords are stored separately in the
/// system keyring (if available) and referenced via `secret_ref()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfile {
    /// Unique identifier for this profile.
    pub id: Uuid,

    /// User-defined name shown in the UI.
    pub name: String,

    /// The database type (e.g., MySQL vs MariaDB).
    ///
    /// This is the authoritative source for driver selection.
    /// For legacy profiles without this field, falls back to `config.kind()`.
    #[serde(default)]
    pub kind: Option<DbKind>,

    /// Driver identifier used to resolve the runtime driver implementation.
    ///
    /// For built-in drivers this is the stable ID (e.g., `postgres`, `sqlite`).
    /// For external RPC drivers this is a registry key derived from socket id
    /// (format: `rpc:<socket_id>`).
    ///
    /// Legacy profiles may not have this field. In that case we derive it from
    /// `kind` for backward compatibility.
    #[serde(default)]
    pub driver_id: Option<String>,

    /// Database-specific connection parameters.
    pub config: DbConfig,

    /// Whether to persist the password in the system keyring.
    #[serde(default)]
    pub save_password: bool,

    /// Per-connection overrides for global/driver-level settings
    /// (refresh policy, dangerous query checks, etc.).
    ///
    /// `None` means "use the driver-level (or global) defaults".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settings_overrides: Option<GlobalOverrides>,

    /// Per-connection overrides for driver-owned schema settings
    /// (e.g. scan_batch_size, allow_flush).
    ///
    /// `None` means "use the driver-level defaults".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_settings: Option<FormValues>,

    /// Optional command hooks executed during connect/disconnect flow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hooks: Option<ConnectionHooks>,

    /// Optional references to globally defined hooks from config.json.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hook_bindings: Option<ConnectionHookBindings>,

    /// Optional reference to a saved proxy profile for this connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_profile_id: Option<Uuid>,

    /// Optional reference to a global auth profile for SSO/cloud authentication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_profile_id: Option<Uuid>,

    /// Dynamic value references that override driver config fields at connect time.
    /// Keys are driver field names (e.g., "host", "password"), values are ValueRef.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub value_refs: HashMap<String, ValueRef>,

    /// Unified access method (replaces proxy_profile_id + ssh_tunnel_profile_id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_kind: Option<AccessKind>,

    /// Per-connection MCP governance controls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_governance: Option<ConnectionMcpGovernance>,
}

impl ConnectionProfile {
    pub fn new(name: impl Into<String>, config: DbConfig) -> Self {
        let kind = config.kind();
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            kind: Some(kind),
            driver_id: Some(Self::builtin_driver_id_for_kind(kind).to_string()),
            config,
            save_password: true,
            settings_overrides: None,
            connection_settings: None,
            hooks: None,
            hook_bindings: None,
            proxy_profile_id: None,
            auth_profile_id: None,
            value_refs: HashMap::new(),
            access_kind: None,
            mcp_governance: None,
        }
    }

    /// Creates a new profile with an explicit database kind.
    ///
    /// Use this when the kind differs from what `config.kind()` would return,
    /// such as MariaDB (which uses `DbConfig::MySQL` but `DbKind::MariaDB`).
    pub fn new_with_kind(name: impl Into<String>, kind: DbKind, config: DbConfig) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            kind: Some(kind),
            driver_id: Some(Self::builtin_driver_id_for_kind(kind).to_string()),
            config,
            save_password: false,
            settings_overrides: None,
            connection_settings: None,
            hooks: None,
            hook_bindings: None,
            proxy_profile_id: None,
            auth_profile_id: None,
            value_refs: HashMap::new(),
            access_kind: None,
            mcp_governance: None,
        }
    }

    /// Creates a new profile with explicit database kind and driver id.
    pub fn new_with_driver(
        name: impl Into<String>,
        kind: DbKind,
        driver_id: impl Into<String>,
        config: DbConfig,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            kind: Some(kind),
            driver_id: Some(driver_id.into()),
            config,
            save_password: true,
            settings_overrides: None,
            connection_settings: None,
            hooks: None,
            hook_bindings: None,
            proxy_profile_id: None,
            auth_profile_id: None,
            value_refs: HashMap::new(),
            access_kind: None,
            mcp_governance: None,
        }
    }

    /// Creates a profile preserving id, kind, and driver_id from stored data.
    ///
    /// This is the primary constructor used when loading profiles from a
    /// repository, where the id and driver_id must be preserved rather than
    /// generated fresh.
    #[allow(dead_code)]
    pub(crate) fn with_id_and_driver(
        id: Uuid,
        name: String,
        kind: DbKind,
        driver_id: String,
        config: DbConfig,
    ) -> Self {
        Self {
            id,
            name,
            kind: Some(kind),
            driver_id: Some(driver_id),
            config,
            save_password: true,
            settings_overrides: None,
            connection_settings: None,
            hooks: None,
            hook_bindings: None,
            proxy_profile_id: None,
            auth_profile_id: None,
            value_refs: HashMap::new(),
            access_kind: None,
            mcp_governance: None,
        }
    }

    /// Returns the database kind for this profile.
    ///
    /// This is the authoritative source for driver selection.
    /// Use this instead of `config.kind()` when selecting drivers.
    pub fn kind(&self) -> DbKind {
        self.kind.unwrap_or_else(|| self.config.kind())
    }

    /// Sets the database kind explicitly.
    ///
    /// Use this when changing the kind (e.g., MySQL to MariaDB)
    /// without changing the underlying config.
    pub fn set_kind(&mut self, kind: DbKind) {
        self.kind = Some(kind);

        if self.driver_id.is_none() {
            self.driver_id = Some(Self::builtin_driver_id_for_kind(kind).to_string());
        }
    }

    /// Returns the runtime driver identifier used to resolve the driver.
    pub fn driver_id(&self) -> String {
        if let Some(driver_id) = &self.driver_id {
            return driver_id.clone();
        }

        Self::builtin_driver_id_for_kind(self.kind()).to_string()
    }

    /// Sets the runtime driver identifier explicitly.
    pub fn set_driver_id(&mut self, driver_id: impl Into<String>) {
        self.driver_id = Some(driver_id.into());
    }

    pub fn builtin_driver_id_for_kind(kind: DbKind) -> &'static str {
        match kind {
            DbKind::Postgres => "postgres",
            DbKind::SQLite => "sqlite",
            DbKind::MySQL => "mysql",
            DbKind::MariaDB => "mariadb",
            DbKind::MongoDB => "mongodb",
            DbKind::Redis => "redis",
            DbKind::DynamoDB => "dynamodb",
            DbKind::CloudWatchLogs => "cloudwatch",
        }
    }

    pub fn secret_ref(&self) -> String {
        crate::storage::secrets::connection_secret_ref(&self.id)
    }

    pub fn ssh_secret_ref(&self) -> String {
        crate::storage::secrets::ssh_secret_ref(&self.id)
    }

    /// Returns true if this profile uses the new connect pipeline
    /// (has auth profile, value refs, or unified access method).
    pub fn uses_pipeline(&self) -> bool {
        self.auth_profile_id.is_some() || !self.value_refs.is_empty() || self.access_kind.is_some()
    }

    /// Returns an external auth profile name embedded in the driver config.
    ///
    /// This lets app-layer connection orchestration route profiles through the
    /// generic auth pipeline without matching on concrete driver names.
    pub fn external_auth_profile_name(&self) -> Option<&str> {
        match &self.config {
            DbConfig::CloudWatchLogs {
                profile: Some(profile),
                ..
            }
            | DbConfig::DynamoDB {
                profile: Some(profile),
                ..
            } => Some(profile.as_str()),
            _ => None,
        }
    }

    /// Derives an AccessKind from legacy fields (proxy_profile_id, ssh_tunnel_profile_id)
    /// for backward compatibility.
    pub fn legacy_access_kind(&self) -> AccessKind {
        if let Some(proxy_id) = self.proxy_profile_id {
            return AccessKind::Proxy {
                proxy_profile_id: proxy_id,
            };
        }

        match &self.config {
            DbConfig::Postgres {
                ssh_tunnel_profile_id: Some(id),
                ..
            }
            | DbConfig::MySQL {
                ssh_tunnel_profile_id: Some(id),
                ..
            }
            | DbConfig::MongoDB {
                ssh_tunnel_profile_id: Some(id),
                ..
            }
            | DbConfig::Redis {
                ssh_tunnel_profile_id: Some(id),
                ..
            } => AccessKind::Ssh {
                ssh_tunnel_profile_id: *id,
            },
            _ => AccessKind::Direct,
        }
    }
}

/// SSL info captured during a successful connection test.
#[derive(Debug, Clone, Default)]
pub struct SslInfo {
    /// `true` when the connection is TLS-encrypted.
    pub active: bool,
    /// TLS cipher suite name (e.g. `"TLS_AES_256_GCM_SHA384"`), when known.
    pub cipher: Option<String>,
}

/// Enriched result returned by `DbDriver::test_connection_rich`.
///
/// All fields except `rtt_ms` are optional — drivers populate only what they
/// can observe. The UI collapses `None` fields and omits them from the banner.
#[derive(Debug, Clone, Default)]
pub struct TestConnectionResult {
    /// Engine display string (e.g. `"postgres 16.2"`, `"MySQL 8.0.35"`).
    pub engine: Option<String>,
    /// Round-trip time of the test ping in milliseconds.
    pub rtt_ms: Option<u64>,
    /// Server-side timestamp at the moment of the test (ISO-8601 string).
    pub server_time: Option<String>,
    /// SSL/TLS status of the connection.
    pub ssl: Option<SslInfo>,
}

impl TestConnectionResult {
    /// Format the banner body line that appears below "Connection successful".
    ///
    /// Fields are joined with ` · `. Returns an empty string when no optional
    /// fields are populated.
    pub fn format_body(&self) -> String {
        let mut parts: Vec<String> = Vec::new();

        if let Some(engine) = &self.engine {
            parts.push(engine.clone());
        }

        if let Some(rtt) = self.rtt_ms {
            parts.push(format!("{rtt} ms RTT"));
        }

        if let Some(server_time) = &self.server_time {
            parts.push(format!("server time {server_time}"));
        }

        if let Some(ssl) = &self.ssl
            && ssl.active
        {
            if let Some(cipher) = &ssl.cipher {
                parts.push(format!("SSL \u{00B7} {cipher}"));
            } else {
                parts.push("SSL".to_string());
            }
        }

        parts.join(" \u{00B7} ")
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use crate::RefreshPolicySetting;
    use crate::config::app::GlobalOverrides;
    use crate::driver::form::FormValues;
    use crate::values::ValueRef;

    fn sqlite_profile() -> ConnectionProfile {
        ConnectionProfile::new("test-sqlite", DbConfig::default_sqlite())
    }

    #[test]
    fn legacy_profile_deserializes_without_new_fields() {
        let json = r#"{
            "id": "00000000-0000-0000-0000-000000000001",
            "name": "old-pg",
            "config": {
                "Postgres": {
                    "host": "localhost",
                    "port": 5432,
                    "user": "pg",
                    "database": "mydb",
                    "ssl_mode": "Disable"
                }
            }
        }"#;

        let profile: ConnectionProfile = serde_json::from_str(json).unwrap();

        assert_eq!(profile.name, "old-pg");
        assert!(profile.settings_overrides.is_none());
        assert!(profile.connection_settings.is_none());
        assert!(profile.kind.is_none());
        assert!(profile.driver_id.is_none());
        assert!(profile.hooks.is_none());
        assert!(profile.hook_bindings.is_none());
        assert!(profile.auth_profile_id.is_none());
        assert!(profile.value_refs.is_empty());
        assert!(profile.access_kind.is_none());
        assert!(profile.mcp_governance.is_none());
        assert!(!profile.uses_pipeline());
    }

    #[test]
    fn profile_serde_roundtrip_with_overrides() {
        let mut profile = sqlite_profile();
        profile.settings_overrides = Some(GlobalOverrides {
            refresh_policy: Some(RefreshPolicySetting::Interval),
            refresh_interval_secs: Some(10),
            confirm_dangerous: Some(false),
            ..Default::default()
        });

        let mut settings = FormValues::new();
        settings.insert("scan_batch_size".to_string(), "500".to_string());
        profile.connection_settings = Some(settings);

        let json = serde_json::to_string(&profile).unwrap();
        let restored: ConnectionProfile = serde_json::from_str(&json).unwrap();

        assert_eq!(
            restored.settings_overrides.as_ref().unwrap().refresh_policy,
            Some(RefreshPolicySetting::Interval)
        );
        assert_eq!(
            restored
                .settings_overrides
                .as_ref()
                .unwrap()
                .refresh_interval_secs,
            Some(10)
        );
        assert_eq!(
            restored
                .settings_overrides
                .as_ref()
                .unwrap()
                .confirm_dangerous,
            Some(false)
        );
        assert_eq!(
            restored
                .connection_settings
                .as_ref()
                .unwrap()
                .get("scan_batch_size"),
            Some(&"500".to_string())
        );
        assert!(restored.hooks.is_none());
        assert!(restored.hook_bindings.is_none());
        assert!(restored.mcp_governance.is_none());
    }

    #[test]
    fn kind_falls_back_to_config_kind() {
        let json = r#"{
            "id": "00000000-0000-0000-0000-000000000002",
            "name": "legacy-pg",
            "config": {
                "Postgres": {
                    "host": "localhost",
                    "port": 5432,
                    "user": "pg",
                    "database": "db",
                    "ssl_mode": "Disable"
                }
            }
        }"#;

        let profile: ConnectionProfile = serde_json::from_str(json).unwrap();

        assert!(profile.kind.is_none());
        assert_eq!(profile.kind(), DbKind::Postgres);
    }

    #[test]
    fn driver_id_falls_back_to_builtin() {
        let json = r#"{
            "id": "00000000-0000-0000-0000-000000000003",
            "name": "legacy-redis",
            "config": {
                "Redis": {
                    "host": "localhost",
                    "port": 6379
                }
            }
        }"#;

        let profile: ConnectionProfile = serde_json::from_str(json).unwrap();

        assert!(profile.driver_id.is_none());
        assert_eq!(profile.driver_id(), "redis");
    }

    #[test]
    fn set_kind_populates_driver_id_when_none() {
        let json = r#"{
            "id": "00000000-0000-0000-0000-000000000004",
            "name": "legacy-mysql",
            "config": {
                "MySQL": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "root",
                    "ssl_mode": "Disable"
                }
            }
        }"#;

        let mut profile: ConnectionProfile = serde_json::from_str(json).unwrap();

        assert!(profile.driver_id.is_none());

        profile.set_kind(DbKind::MariaDB);

        assert_eq!(profile.kind(), DbKind::MariaDB);
        assert_eq!(profile.driver_id(), "mariadb");
    }

    #[test]
    fn new_with_driver_sets_explicit_driver_id() {
        let profile = ConnectionProfile::new_with_driver(
            "custom-redis",
            DbKind::Redis,
            "rpc:my_redis",
            DbConfig::default_redis(),
        );

        assert_eq!(profile.kind(), DbKind::Redis);
        assert_eq!(profile.driver_id(), "rpc:my_redis");
        assert!(profile.settings_overrides.is_none());
        assert!(profile.connection_settings.is_none());
        assert!(profile.hooks.is_none());
        assert!(profile.hook_bindings.is_none());
        assert!(profile.mcp_governance.is_none());
    }

    #[test]
    fn dynamodb_config_kind_and_driver_id_fallback_work() {
        let json = r#"{
            "id": "00000000-0000-0000-0000-000000000099",
            "name": "legacy-dynamodb",
            "config": {
                "DynamoDB": {
                    "region": "us-east-1"
                }
            }
        }"#;

        let profile: ConnectionProfile =
            serde_json::from_str(json).expect("dynamodb profile should deserialize");

        assert_eq!(profile.kind(), DbKind::DynamoDB);
        assert_eq!(profile.driver_id(), "dynamodb");
    }

    #[test]
    fn dynamodb_profile_serde_roundtrip_preserves_optional_fields() {
        let profile = ConnectionProfile::new(
            "dynamo",
            DbConfig::DynamoDB {
                region: "us-west-2".to_string(),
                profile: Some("dev".to_string()),
                endpoint: Some("http://localhost:8000".to_string()),
                table: Some("users".to_string()),
            },
        );

        let json = serde_json::to_string(&profile).expect("serialize should succeed");
        let restored: ConnectionProfile =
            serde_json::from_str(&json).expect("deserialize should succeed");

        match restored.config {
            DbConfig::DynamoDB {
                region,
                profile,
                endpoint,
                table,
            } => {
                assert_eq!(region, "us-west-2");
                assert_eq!(profile.as_deref(), Some("dev"));
                assert_eq!(endpoint.as_deref(), Some("http://localhost:8000"));
                assert_eq!(table.as_deref(), Some("users"));
            }
            _ => panic!("expected DynamoDB config variant"),
        }
    }

    #[test]
    fn cloudwatch_config_kind_and_driver_id_fallback_work() {
        let json = r#"{
            "id": "00000000-0000-0000-0000-000000000100",
            "name": "legacy-cloudwatch",
            "config": {
                "CloudWatchLogs": {
                    "region": "us-east-1"
                }
            }
        }"#;

        let profile: ConnectionProfile =
            serde_json::from_str(json).expect("cloudwatch profile should deserialize");

        assert_eq!(profile.kind(), DbKind::CloudWatchLogs);
        assert_eq!(profile.driver_id(), "cloudwatch");
    }

    #[test]
    fn cloudwatch_profile_serde_roundtrip_preserves_optional_fields() {
        let profile = ConnectionProfile::new(
            "cloudwatch",
            DbConfig::CloudWatchLogs {
                region: "us-west-2".to_string(),
                profile: Some("dev".to_string()),
                endpoint: Some("http://localhost:4566".to_string()),
            },
        );

        let json = serde_json::to_string(&profile).expect("serialize should succeed");
        let restored: ConnectionProfile =
            serde_json::from_str(&json).expect("deserialize should succeed");

        match restored.config {
            DbConfig::CloudWatchLogs {
                region,
                profile,
                endpoint,
            } => {
                assert_eq!(region, "us-west-2");
                assert_eq!(profile.as_deref(), Some("dev"));
                assert_eq!(endpoint.as_deref(), Some("http://localhost:4566"));
            }
            _ => panic!("expected CloudWatch Logs config variant"),
        }
    }

    #[test]
    fn ssl_mode_defaults_to_prefer() {
        assert_eq!(SslMode::default(), SslMode::Prefer);
    }

    #[test]
    fn ssl_mode_from_id_maps_all_known_ids() {
        use super::ssl_mode_from_id;

        assert_eq!(ssl_mode_from_id("disable"), Some(SslMode::Disable));
        assert_eq!(ssl_mode_from_id("allow"), Some(SslMode::Allow));
        assert_eq!(ssl_mode_from_id("prefer"), Some(SslMode::Prefer));
        assert_eq!(ssl_mode_from_id("require"), Some(SslMode::Require));
        assert_eq!(ssl_mode_from_id("verify-ca"), Some(SslMode::VerifyCa));
        assert_eq!(ssl_mode_from_id("verify-full"), Some(SslMode::VerifyFull));
    }

    #[test]
    fn ssl_mode_from_id_returns_none_for_unknown() {
        use super::ssl_mode_from_id;

        assert_eq!(ssl_mode_from_id(""), None);
        assert_eq!(ssl_mode_from_id("DISABLE"), None);
        assert_eq!(ssl_mode_from_id("unknown"), None);
    }

    #[test]
    fn ssl_mode_as_id_round_trips_all_variants() {
        assert_eq!(SslMode::Disable.as_id(), "disable");
        assert_eq!(SslMode::Allow.as_id(), "allow");
        assert_eq!(SslMode::Prefer.as_id(), "prefer");
        assert_eq!(SslMode::Require.as_id(), "require");
        assert_eq!(SslMode::VerifyCa.as_id(), "verify-ca");
        assert_eq!(SslMode::VerifyFull.as_id(), "verify-full");
    }

    #[test]
    fn ssl_mode_from_id_round_trips_all_variants() {
        use super::ssl_mode_from_id;

        for mode in [
            SslMode::Disable,
            SslMode::Allow,
            SslMode::Prefer,
            SslMode::Require,
            SslMode::VerifyCa,
            SslMode::VerifyFull,
        ] {
            assert_eq!(
                ssl_mode_from_id(mode.as_id()),
                Some(mode),
                "round-trip failed for {mode:?}"
            );
        }
    }

    #[test]
    fn ssl_mode_requires_root_cert_is_true_for_verify_modes_only() {
        use super::ssl_mode_requires_root_cert;

        assert!(!ssl_mode_requires_root_cert(SslMode::Disable));
        assert!(!ssl_mode_requires_root_cert(SslMode::Allow));
        assert!(!ssl_mode_requires_root_cert(SslMode::Prefer));
        assert!(!ssl_mode_requires_root_cert(SslMode::Require));
        assert!(ssl_mode_requires_root_cert(SslMode::VerifyCa));
        assert!(ssl_mode_requires_root_cert(SslMode::VerifyFull));
    }

    #[test]
    fn strip_password_from_uri_extracts_and_sanitizes_credentials() {
        let (sanitized, password) =
            strip_password_from_uri("postgresql://alice:p%40ss@localhost:5432/app?sslmode=require");

        assert_eq!(
            sanitized,
            "postgresql://alice@localhost:5432/app?sslmode=require"
        );
        assert_eq!(password.as_deref(), Some("p@ss"));
    }

    #[test]
    fn strip_password_from_uri_keeps_uris_without_password() {
        let input = "postgresql://alice@localhost:5432/app";
        let (sanitized, password) = strip_password_from_uri(input);

        assert_eq!(sanitized, input);
        assert!(password.is_none());
    }

    #[test]
    fn strip_uri_password_updates_uri_mode_config_in_place() {
        let mut config = DbConfig::Redis {
            use_uri: true,
            uri: Some("redis://:sekret@localhost:6379/0".to_string()),
            host: String::new(),
            port: 6379,
            user: None,
            database: Some(0),
            tls: false,
            ssl_mode: Some("off".to_string()),
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        };

        let extracted = config.strip_uri_password();

        assert_eq!(extracted.as_deref(), Some("sekret"));
        assert!(matches!(
            config,
            DbConfig::Redis {
                uri: Some(ref uri), ..
            } if uri == "redis://localhost:6379/0"
        ));
    }

    #[test]
    fn uses_pipeline_returns_true_with_auth_profile() {
        let mut profile = sqlite_profile();
        assert!(!profile.uses_pipeline());

        profile.auth_profile_id = Some(Uuid::new_v4());
        assert!(profile.uses_pipeline());
    }

    #[test]
    fn uses_pipeline_returns_true_with_value_refs() {
        let mut profile = sqlite_profile();

        profile
            .value_refs
            .insert("password".to_string(), ValueRef::env("DB_PASS"));
        assert!(profile.uses_pipeline());
    }

    #[test]
    fn profile_roundtrip_with_connection_mcp_governance() {
        let mut profile = sqlite_profile();
        profile.mcp_governance = Some(ConnectionMcpGovernance {
            enabled: true,
            policy_bindings: vec![ConnectionMcpPolicyBinding {
                actor_id: "agent-a".to_string(),
                role_ids: vec!["role-reader".to_string()],
                policy_ids: vec!["policy-read".to_string()],
            }],
        });

        let json = serde_json::to_string(&profile).expect("serialize should succeed");
        let restored: ConnectionProfile =
            serde_json::from_str(&json).expect("deserialize should succeed");

        let governance = restored
            .mcp_governance
            .expect("mcp governance should be present after roundtrip");
        assert!(governance.enabled);
        assert_eq!(governance.policy_bindings.len(), 1);
        assert_eq!(governance.policy_bindings[0].actor_id, "agent-a");
    }

    // --- TestConnectionResult::format_body tests ---

    #[test]
    fn format_body_all_fields_present() {
        let result = super::TestConnectionResult {
            engine: Some("postgres 16.2".to_string()),
            rtt_ms: Some(4),
            server_time: Some("2026-05-09 12:21:34 UTC".to_string()),
            ssl: Some(super::SslInfo {
                active: true,
                cipher: Some("TLS_AES_256_GCM_SHA384".to_string()),
            }),
        };
        let body = result.format_body();
        assert!(body.contains("postgres 16.2"), "engine missing: {body}");
        assert!(body.contains("4 ms RTT"), "rtt missing: {body}");
        assert!(
            body.contains("2026-05-09 12:21:34 UTC"),
            "server_time missing: {body}"
        );
        assert!(
            body.contains("TLS_AES_256_GCM_SHA384"),
            "cipher missing: {body}"
        );
    }

    #[test]
    fn format_body_only_engine() {
        let result = super::TestConnectionResult {
            engine: Some("sqlite 3.45.0".to_string()),
            ..Default::default()
        };
        assert_eq!(result.format_body(), "sqlite 3.45.0");
    }

    #[test]
    fn format_body_only_rtt() {
        let result = super::TestConnectionResult {
            rtt_ms: Some(12),
            ..Default::default()
        };
        assert_eq!(result.format_body(), "12 ms RTT");
    }

    #[test]
    fn format_body_no_optional_fields_returns_empty() {
        let result = super::TestConnectionResult::default();
        assert_eq!(result.format_body(), "");
    }

    #[test]
    fn format_body_ssl_without_cipher() {
        let result = super::TestConnectionResult {
            ssl: Some(super::SslInfo {
                active: true,
                cipher: None,
            }),
            ..Default::default()
        };
        assert_eq!(result.format_body(), "SSL");
    }
}
