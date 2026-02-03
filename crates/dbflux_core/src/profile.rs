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
}

impl DbKind {
    pub fn display_name(&self) -> &'static str {
        match self {
            DbKind::Postgres => "PostgreSQL",
            DbKind::SQLite => "SQLite",
            DbKind::MySQL => "MySQL",
            DbKind::MariaDB => "MariaDB",
            DbKind::MongoDB => "MongoDB",
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
            save_secret: false,
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
        host: String,
        port: u16,
        user: String,
        /// Optional database name. If None, user can browse all databases.
        database: Option<String>,
        ssl_mode: SslMode,
        ssh_tunnel: Option<SshTunnelConfig>,
        #[serde(default)]
        ssh_tunnel_profile_id: Option<Uuid>,
    },
    MongoDB {
        /// MongoDB connection URI (e.g., "mongodb://localhost:27017").
        uri: String,
        /// Optional database name. If None, user can browse all databases.
        database: Option<String>,
        /// Application name for MongoDB connection metadata.
        app_name: Option<String>,
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
        }
    }

    pub fn default_postgres() -> Self {
        DbConfig::Postgres {
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
            uri: "mongodb://localhost:27017".to_string(),
            database: None,
            app_name: Some("dbflux".to_string()),
        }
    }

    /// Returns the SSH tunnel configuration if present.
    pub fn ssh_tunnel(&self) -> Option<&SshTunnelConfig> {
        match self {
            DbConfig::Postgres { ssh_tunnel, .. } => ssh_tunnel.as_ref(),
            DbConfig::MySQL { ssh_tunnel, .. } => ssh_tunnel.as_ref(),
            DbConfig::SQLite { .. } | DbConfig::MongoDB { .. } => None,
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

    /// Database-specific connection parameters.
    pub config: DbConfig,

    /// Whether to persist the password in the system keyring.
    #[serde(default)]
    pub save_password: bool,
}

impl ConnectionProfile {
    pub fn new(name: impl Into<String>, config: DbConfig) -> Self {
        let kind = config.kind();
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            kind: Some(kind),
            config,
            save_password: false,
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
            config,
            save_password: false,
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
    }

    pub fn secret_ref(&self) -> String {
        crate::secrets::connection_secret_ref(&self.id)
    }

    pub fn ssh_secret_ref(&self) -> String {
        crate::secrets::ssh_secret_ref(&self.id)
    }
}
