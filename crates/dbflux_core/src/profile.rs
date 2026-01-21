use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

/// Supported database types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DbKind {
    Postgres,
    SQLite,
}

impl DbKind {
    pub fn display_name(&self) -> &'static str {
        match self {
            DbKind::Postgres => "PostgreSQL",
            DbKind::SQLite => "SQLite",
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

/// SSH tunnel configuration for connecting through a bastion host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshTunnelConfig {
    /// SSH server hostname.
    pub host: String,

    /// SSH server port (typically 22).
    pub port: u16,

    /// SSH username.
    pub user: String,

    /// Path to private key file. If `None`, uses SSH agent or default keys.
    pub private_key_path: Option<PathBuf>,
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
    },
    SQLite {
        /// Path to the SQLite database file.
        path: PathBuf,
    },
}

impl DbConfig {
    pub fn kind(&self) -> DbKind {
        match self {
            DbConfig::Postgres { .. } => DbKind::Postgres,
            DbConfig::SQLite { .. } => DbKind::SQLite,
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
        }
    }

    pub fn default_sqlite() -> Self {
        DbConfig::SQLite {
            path: PathBuf::new(),
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

    /// Database-specific connection parameters.
    pub config: DbConfig,

    /// Whether to persist the password in the system keyring.
    #[serde(default)]
    pub save_password: bool,
}

impl ConnectionProfile {
    pub fn new(name: impl Into<String>, config: DbConfig) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            config,
            save_password: false,
        }
    }

    pub fn kind(&self) -> DbKind {
        self.config.kind()
    }

    /// Returns the keyring key for this profile's password.
    pub fn secret_ref(&self) -> String {
        crate::secrets::connection_secret_ref(&self.id)
    }
}
