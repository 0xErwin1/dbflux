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
        }
    }
}

/// SSL/TLS mode for PostgreSQL connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SslMode {
    /// No SSL (unencrypted connection).
    Disable,

    /// Try SSL, fall back to unencrypted if unavailable.
    #[default]
    Prefer,

    /// Require SSL (fail if server doesn't support it).
    Require,
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
        ssl_mode: SslMode,
        ssh_tunnel: Option<SshTunnelConfig>,
        #[serde(default)]
        ssh_tunnel_profile_id: Option<Uuid>,
    },
    SQLite {
        path: PathBuf,
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
        ssl_mode: SslMode,
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
        #[serde(default)]
        tls: bool,
        ssh_tunnel: Option<SshTunnelConfig>,
        #[serde(default)]
        ssh_tunnel_profile_id: Option<Uuid>,
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
            ssl_mode: SslMode::default(),
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        }
    }

    pub fn default_sqlite() -> Self {
        DbConfig::SQLite {
            path: PathBuf::new(),
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
            ssl_mode: SslMode::default(),
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
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        }
    }

    pub fn ssh_tunnel(&self) -> Option<&SshTunnelConfig> {
        match self {
            DbConfig::Postgres { ssh_tunnel, .. }
            | DbConfig::MySQL { ssh_tunnel, .. }
            | DbConfig::MongoDB { ssh_tunnel, .. }
            | DbConfig::Redis { ssh_tunnel, .. } => ssh_tunnel.as_ref(),
            DbConfig::SQLite { .. } | DbConfig::External { .. } => None,
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
            DbConfig::SQLite { .. } | DbConfig::External { .. } => false,
        }
    }

    /// Target host and port for tunnel forwarding. `None` for SQLite/external.
    pub fn host_port(&self) -> Option<(&str, u16)> {
        match self {
            DbConfig::Postgres { host, port, .. }
            | DbConfig::MySQL { host, port, .. }
            | DbConfig::MongoDB { host, port, .. }
            | DbConfig::Redis { host, port, .. } => Some((host, *port)),
            DbConfig::SQLite { .. } | DbConfig::External { .. } => None,
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
            DbConfig::SQLite { .. } | DbConfig::External { .. } => {}
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
            DbConfig::SQLite { .. } | DbConfig::External { .. } => return None,
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
    kind: Option<DbKind>,

    /// Driver identifier used to resolve the runtime driver implementation.
    ///
    /// For built-in drivers this is the stable ID (e.g., `postgres`, `sqlite`).
    /// For external RPC drivers this is a registry key derived from socket id
    /// (format: `rpc:<socket_id>`).
    ///
    /// Legacy profiles may not have this field. In that case we derive it from
    /// `kind` for backward compatibility.
    #[serde(default)]
    driver_id: Option<String>,

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

#[cfg(test)]
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
    }

    #[test]
    fn ssl_mode_defaults_to_prefer() {
        assert_eq!(SslMode::default(), SslMode::Prefer);
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
}
