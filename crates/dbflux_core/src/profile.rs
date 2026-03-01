use crate::app_config::GlobalOverrides;
use crate::driver_form::FormValues;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

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
    #[default]
    Disable,

    /// Try SSL, fall back to unencrypted if unavailable.
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
        crate::secrets::ssh_tunnel_secret_ref(&self.id)
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
        crate::secrets::connection_secret_ref(&self.id)
    }

    pub fn ssh_secret_ref(&self) -> String {
        crate::secrets::ssh_secret_ref(&self.id)
    }
}
