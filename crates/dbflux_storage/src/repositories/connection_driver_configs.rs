//! Repository for driver-specific connection configs in dbflux.db.
//!
//! This module provides CRUD operations for the cfg_connection_driver_configs table,
//! which stores typed native columns for DbConfig variants instead of JSON.

use log::info;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use dbflux_core::{DbConfig, DbKind, SshAuthMethod, SshTunnelConfig, SslMode};

use crate::bootstrap::OwnedConnection;
use crate::error::StorageError;

/// DTO for connection driver config (native columns for DbConfig).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionDriverConfigDto {
    pub id: String,
    pub profile_id: String,
    pub config_key: String,
    // Relational DB common fields
    pub use_uri: bool,
    pub uri: Option<String>,
    pub host: Option<String>,
    pub port: Option<i32>,
    pub user: Option<String>,
    pub database_name: Option<String>,
    pub ssl_mode: String,
    pub ssl_ca: Option<String>,
    pub ssl_cert: Option<String>,
    pub ssl_key: Option<String>,
    pub password_secret_ref: Option<String>,
    pub connect_timeout_secs: Option<i32>,
    // SSH tunnel inline config
    pub ssh_tunnel_host: Option<String>,
    pub ssh_tunnel_port: Option<i32>,
    pub ssh_tunnel_user: Option<String>,
    pub ssh_tunnel_auth_method: String,
    pub ssh_tunnel_key_path: Option<String>,
    pub ssh_tunnel_passphrase_secret_ref: Option<String>,
    pub ssh_tunnel_password_secret_ref: Option<String>,
    // SQLite-specific
    pub sqlite_path: Option<String>,
    pub sqlite_connection_id: Option<String>,
    // MongoDB-specific
    pub mongo_auth_database: Option<String>,
    // Redis-specific
    pub redis_tls: bool,
    pub redis_database: Option<i32>,
    // DynamoDB-specific
    pub dynamo_region: Option<String>,
    pub dynamo_profile: Option<String>,
    pub dynamo_endpoint: Option<String>,
    pub dynamo_table: Option<String>,
    // External
    pub external_kind: Option<String>,
    pub external_values_json: Option<String>,
}

impl ConnectionDriverConfigDto {
    /// Creates a new empty driver config for a profile.
    pub fn new(profile_id: String, config_key: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            config_key,
            use_uri: false,
            uri: None,
            host: None,
            port: None,
            user: None,
            database_name: None,
            ssl_mode: "prefer".to_string(),
            ssl_ca: None,
            ssl_cert: None,
            ssl_key: None,
            password_secret_ref: None,
            connect_timeout_secs: None,
            ssh_tunnel_host: None,
            ssh_tunnel_port: None,
            ssh_tunnel_user: None,
            ssh_tunnel_auth_method: "private_key".to_string(),
            ssh_tunnel_key_path: None,
            ssh_tunnel_passphrase_secret_ref: None,
            ssh_tunnel_password_secret_ref: None,
            sqlite_path: None,
            sqlite_connection_id: None,
            mongo_auth_database: None,
            redis_tls: false,
            redis_database: None,
            dynamo_region: None,
            dynamo_profile: None,
            dynamo_endpoint: None,
            dynamo_table: None,
            external_kind: None,
            external_values_json: None,
        }
    }

    /// Converts a DbConfig to this DTO.
    pub fn from_db_config(profile_id: String, config: &DbConfig) -> Self {
        let mut dto = Self::new(profile_id, db_kind_to_str(config.kind()));

        match config {
            DbConfig::Postgres {
                use_uri,
                uri,
                host,
                port,
                user,
                database,
                ssl_mode,
                ssh_tunnel,
                ..
            } => {
                dto.use_uri = *use_uri;
                dto.uri = uri.clone();
                dto.host = Some(host.clone());
                dto.port = Some(*port as i32);
                dto.user = Some(user.clone());
                dto.database_name = Some(database.clone());
                dto.ssl_mode = ssl_mode_to_str(ssl_mode);
                if let Some(tunnel) = ssh_tunnel {
                    fill_ssh_tunnel_fields(&mut dto, tunnel);
                }
            }
            DbConfig::MySQL {
                use_uri,
                uri,
                host,
                port,
                user,
                database,
                ssl_mode,
                ssh_tunnel,
                ..
            } => {
                dto.use_uri = *use_uri;
                dto.uri = uri.clone();
                dto.host = Some(host.clone());
                dto.port = Some(*port as i32);
                dto.user = Some(user.clone());
                dto.database_name = database.clone();
                dto.ssl_mode = ssl_mode_to_str(ssl_mode);
                if let Some(tunnel) = ssh_tunnel {
                    fill_ssh_tunnel_fields(&mut dto, tunnel);
                }
            }
            DbConfig::MongoDB {
                use_uri,
                uri,
                host,
                port,
                user,
                database,
                auth_database,
                ssh_tunnel,
                ..
            } => {
                dto.use_uri = *use_uri;
                dto.uri = uri.clone();
                dto.host = Some(host.clone());
                dto.port = Some(*port as i32);
                dto.user = user.clone();
                dto.database_name = database.clone();
                dto.mongo_auth_database = auth_database.clone();
                if let Some(tunnel) = ssh_tunnel {
                    fill_ssh_tunnel_fields(&mut dto, tunnel);
                }
            }
            DbConfig::Redis {
                use_uri,
                uri,
                host,
                port,
                user,
                database,
                tls,
                ssh_tunnel,
                ..
            } => {
                dto.use_uri = *use_uri;
                dto.uri = uri.clone();
                dto.host = Some(host.clone());
                dto.port = Some(*port as i32);
                dto.user = user.clone();
                dto.database_name = database.map(|d| d.to_string());
                dto.redis_tls = *tls;
                dto.redis_database = database.map(|d| d as i32);
                if let Some(tunnel) = ssh_tunnel {
                    fill_ssh_tunnel_fields(&mut dto, tunnel);
                }
            }
            DbConfig::SQLite {
                path,
                connection_id,
            } => {
                dto.sqlite_path = Some(path.to_string_lossy().to_string());
                dto.sqlite_connection_id = connection_id.clone();
            }
            DbConfig::DynamoDB {
                region,
                profile,
                endpoint,
                table,
            } => {
                dto.dynamo_region = Some(region.clone());
                dto.dynamo_profile = profile.clone();
                dto.dynamo_endpoint = endpoint.clone();
                dto.dynamo_table = table.clone();
            }
            DbConfig::External { kind, values } => {
                dto.external_kind = Some(db_kind_to_str(*kind));
                dto.external_values_json = Some(serde_json::to_string(values).unwrap_or_default());
            }
        }

        dto
    }

    /// Converts this DTO back to a DbConfig.
    pub fn to_db_config(&self) -> Option<DbConfig> {
        let kind = str_to_db_kind(&self.config_key)?;

        match kind {
            DbKind::Postgres => {
                let ssh_tunnel = build_ssh_tunnel(self);

                Some(DbConfig::Postgres {
                    use_uri: self.use_uri,
                    uri: self.uri.clone(),
                    host: self.host.clone().unwrap_or_default(),
                    port: self.port.unwrap_or(5432) as u16,
                    user: self.user.clone().unwrap_or_default(),
                    database: self.database_name.clone().unwrap_or_default(),
                    ssl_mode: str_to_ssl_mode(&self.ssl_mode),
                    ssh_tunnel,
                    ssh_tunnel_profile_id: None,
                })
            }
            DbKind::MySQL | DbKind::MariaDB => {
                let ssh_tunnel = build_ssh_tunnel(self);

                Some(DbConfig::MySQL {
                    use_uri: self.use_uri,
                    uri: self.uri.clone(),
                    host: self.host.clone().unwrap_or_default(),
                    port: self.port.unwrap_or(3306) as u16,
                    user: self.user.clone().unwrap_or_default(),
                    database: self
                        .database_name
                        .clone()
                        .filter(|database| !database.is_empty()),
                    ssl_mode: str_to_ssl_mode(&self.ssl_mode),
                    ssh_tunnel,
                    ssh_tunnel_profile_id: None,
                })
            }
            DbKind::MongoDB => {
                let ssh_tunnel = build_ssh_tunnel(self);

                Some(DbConfig::MongoDB {
                    use_uri: self.use_uri,
                    uri: self.uri.clone(),
                    host: self.host.clone().unwrap_or_default(),
                    port: self.port.unwrap_or(27017) as u16,
                    user: self.user.clone(),
                    database: self.database_name.clone(),
                    auth_database: self.mongo_auth_database.clone(),
                    ssh_tunnel,
                    ssh_tunnel_profile_id: None,
                })
            }
            DbKind::Redis => {
                let ssh_tunnel = build_ssh_tunnel(self);

                Some(DbConfig::Redis {
                    use_uri: self.use_uri,
                    uri: self.uri.clone(),
                    host: self.host.clone().unwrap_or_default(),
                    port: self.port.unwrap_or(6379) as u16,
                    user: self.user.clone(),
                    database: self.redis_database.map(|d| d as u32),
                    tls: self.redis_tls,
                    ssh_tunnel,
                    ssh_tunnel_profile_id: None,
                })
            }
            DbKind::SQLite => Some(DbConfig::SQLite {
                path: std::path::PathBuf::from(self.sqlite_path.clone().unwrap_or_default()),
                connection_id: self.sqlite_connection_id.clone(),
            }),
            DbKind::DynamoDB => Some(DbConfig::DynamoDB {
                region: self.dynamo_region.clone().unwrap_or_default(),
                profile: self.dynamo_profile.clone(),
                endpoint: self.dynamo_endpoint.clone(),
                table: self.dynamo_table.clone(),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Private helpers for DbConfig <-> DTO conversion
// ---------------------------------------------------------------------------

fn db_kind_to_str(kind: DbKind) -> String {
    match kind {
        DbKind::Postgres => "Postgres",
        DbKind::SQLite => "SQLite",
        DbKind::MySQL => "MySQL",
        DbKind::MariaDB => "MariaDB",
        DbKind::MongoDB => "MongoDB",
        DbKind::Redis => "Redis",
        DbKind::DynamoDB => "DynamoDB",
    }
    .to_string()
}

fn str_to_db_kind(s: &str) -> Option<DbKind> {
    match s {
        "Postgres" => Some(DbKind::Postgres),
        "SQLite" => Some(DbKind::SQLite),
        "MySQL" => Some(DbKind::MySQL),
        "MariaDB" => Some(DbKind::MariaDB),
        "MongoDB" => Some(DbKind::MongoDB),
        "Redis" => Some(DbKind::Redis),
        "DynamoDB" => Some(DbKind::DynamoDB),
        _ => None,
    }
}

fn ssl_mode_to_str(mode: &SslMode) -> String {
    match mode {
        SslMode::Disable => "disable".to_string(),
        SslMode::Prefer => "prefer".to_string(),
        SslMode::Require => "require".to_string(),
    }
}

fn str_to_ssl_mode(s: &str) -> SslMode {
    match s {
        "disable" => SslMode::Disable,
        "require" => SslMode::Require,
        _ => SslMode::Prefer,
    }
}

fn ssh_auth_method_to_str(method: &SshAuthMethod) -> String {
    match method {
        SshAuthMethod::PrivateKey { .. } => "private_key".to_string(),
        SshAuthMethod::Password => "password".to_string(),
    }
}

fn str_to_ssh_auth_method(s: &str) -> SshAuthMethod {
    match s {
        "password" => SshAuthMethod::Password,
        _ => SshAuthMethod::PrivateKey { key_path: None },
    }
}

fn fill_ssh_tunnel_fields(dto: &mut ConnectionDriverConfigDto, tunnel: &SshTunnelConfig) {
    dto.ssh_tunnel_host = Some(tunnel.host.clone());
    dto.ssh_tunnel_port = Some(tunnel.port as i32);
    dto.ssh_tunnel_user = Some(tunnel.user.clone());
    dto.ssh_tunnel_auth_method = ssh_auth_method_to_str(&tunnel.auth_method);
    if let SshAuthMethod::PrivateKey { key_path } = &tunnel.auth_method {
        dto.ssh_tunnel_key_path = key_path.as_ref().map(|p| p.to_string_lossy().to_string());
    }
}

fn build_ssh_tunnel(dto: &ConnectionDriverConfigDto) -> Option<SshTunnelConfig> {
    if dto.ssh_tunnel_host.is_some() {
        Some(SshTunnelConfig {
            host: dto.ssh_tunnel_host.clone()?,
            port: dto.ssh_tunnel_port? as u16,
            user: dto.ssh_tunnel_user.clone()?,
            auth_method: str_to_ssh_auth_method(&dto.ssh_tunnel_auth_method),
        })
    } else {
        None
    }
}

/// Repository for managing connection driver configs with native columns.
pub struct ConnectionDriverConfigsRepository {
    conn: OwnedConnection,
}

impl ConnectionDriverConfigsRepository {
    /// Creates a new repository instance.
    pub fn new(conn: OwnedConnection) -> Self {
        Self { conn }
    }

    /// Borrows the underlying connection.
    fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Gets the driver config for a connection profile.
    pub fn get_for_profile(
        &self,
        profile_id: &str,
    ) -> Result<Option<ConnectionDriverConfigDto>, StorageError> {
        let mut stmt = self
            .conn()
            .prepare(
                r#"
                SELECT
                    id, profile_id, config_key,
                    use_uri, uri, host, port, user, database_name,
                    ssl_mode, ssl_ca, ssl_cert, ssl_key, password_secret_ref, connect_timeout_secs,
                    ssh_tunnel_host, ssh_tunnel_port, ssh_tunnel_user, ssh_tunnel_auth_method,
                    ssh_tunnel_key_path, ssh_tunnel_passphrase_secret_ref, ssh_tunnel_password_secret_ref,
                    sqlite_path, sqlite_connection_id,
                    mongo_auth_database,
                    redis_tls, redis_database,
                    dynamo_region, dynamo_profile, dynamo_endpoint, dynamo_table,
                    external_kind, external_values_json
                FROM cfg_connection_driver_configs
                WHERE profile_id = ?1
                "#,
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        let result = stmt.query_row([profile_id], |row| {
            Ok(ConnectionDriverConfigDto {
                id: row.get(0)?,
                profile_id: row.get(1)?,
                config_key: row.get(2)?,
                use_uri: row.get::<_, i32>(3)? != 0,
                uri: row.get(4)?,
                host: row.get(5)?,
                port: row.get(6)?,
                user: row.get(7)?,
                database_name: row.get(8)?,
                ssl_mode: row.get(9)?,
                ssl_ca: row.get(10)?,
                ssl_cert: row.get(11)?,
                ssl_key: row.get(12)?,
                password_secret_ref: row.get(13)?,
                connect_timeout_secs: row.get(14)?,
                ssh_tunnel_host: row.get(15)?,
                ssh_tunnel_port: row.get(16)?,
                ssh_tunnel_user: row.get(17)?,
                ssh_tunnel_auth_method: row.get(18)?,
                ssh_tunnel_key_path: row.get(19)?,
                ssh_tunnel_passphrase_secret_ref: row.get(20)?,
                ssh_tunnel_password_secret_ref: row.get(21)?,
                sqlite_path: row.get(22)?,
                sqlite_connection_id: row.get(23)?,
                mongo_auth_database: row.get(24)?,
                redis_tls: row.get::<_, i32>(25)? != 0,
                redis_database: row.get(26)?,
                dynamo_region: row.get(27)?,
                dynamo_profile: row.get(28)?,
                dynamo_endpoint: row.get(29)?,
                dynamo_table: row.get(30)?,
                external_kind: row.get(31)?,
                external_values_json: row.get(32)?,
            })
        });

        match result {
            Ok(dto) => Ok(Some(dto)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Sqlite {
                path: "dbflux.db".into(),
                source: e,
            }),
        }
    }

    /// Inserts a new driver config.
    pub fn insert(&self, config: &ConnectionDriverConfigDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO cfg_connection_driver_configs (
                    id, profile_id, config_key,
                    use_uri, uri, host, port, user, database_name,
                    ssl_mode, ssl_ca, ssl_cert, ssl_key, password_secret_ref, connect_timeout_secs,
                    ssh_tunnel_host, ssh_tunnel_port, ssh_tunnel_user, ssh_tunnel_auth_method,
                    ssh_tunnel_key_path, ssh_tunnel_passphrase_secret_ref, ssh_tunnel_password_secret_ref,
                    sqlite_path, sqlite_connection_id,
                    mongo_auth_database,
                    redis_tls, redis_database,
                    dynamo_region, dynamo_profile, dynamo_endpoint, dynamo_table,
                    external_kind, external_values_json
                ) VALUES (
                    ?1, ?2, ?3,
                    ?4, ?5, ?6, ?7, ?8, ?9,
                    ?10, ?11, ?12, ?13, ?14, ?15,
                    ?16, ?17, ?18, ?19,
                    ?20, ?21, ?22,
                    ?23, ?24,
                    ?25,
                    ?26, ?27,
                    ?28, ?29, ?30, ?31,
                    ?32, ?33
                )
                "#,
                params![
                    config.id,
                    config.profile_id,
                    config.config_key,
                    config.use_uri as i32,
                    config.uri,
                    config.host,
                    config.port,
                    config.user,
                    config.database_name,
                    config.ssl_mode,
                    config.ssl_ca,
                    config.ssl_cert,
                    config.ssl_key,
                    config.password_secret_ref,
                    config.connect_timeout_secs,
                    config.ssh_tunnel_host,
                    config.ssh_tunnel_port,
                    config.ssh_tunnel_user,
                    config.ssh_tunnel_auth_method,
                    config.ssh_tunnel_key_path,
                    config.ssh_tunnel_passphrase_secret_ref,
                    config.ssh_tunnel_password_secret_ref,
                    config.sqlite_path,
                    config.sqlite_connection_id,
                    config.mongo_auth_database,
                    config.redis_tls as i32,
                    config.redis_database,
                    config.dynamo_region,
                    config.dynamo_profile,
                    config.dynamo_endpoint,
                    config.dynamo_table,
                    config.external_kind,
                    config.external_values_json,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(())
    }

    /// Upserts a driver config (insert or update by profile_id).
    pub fn upsert(&self, config: &ConnectionDriverConfigDto) -> Result<(), StorageError> {
        self.conn()
            .execute(
                r#"
                INSERT INTO cfg_connection_driver_configs (
                    id, profile_id, config_key,
                    use_uri, uri, host, port, user, database_name,
                    ssl_mode, ssl_ca, ssl_cert, ssl_key, password_secret_ref, connect_timeout_secs,
                    ssh_tunnel_host, ssh_tunnel_port, ssh_tunnel_user, ssh_tunnel_auth_method,
                    ssh_tunnel_key_path, ssh_tunnel_passphrase_secret_ref, ssh_tunnel_password_secret_ref,
                    sqlite_path, sqlite_connection_id,
                    mongo_auth_database,
                    redis_tls, redis_database,
                    dynamo_region, dynamo_profile, dynamo_endpoint, dynamo_table,
                    external_kind, external_values_json
                ) VALUES (
                    ?1, ?2, ?3,
                    ?4, ?5, ?6, ?7, ?8, ?9,
                    ?10, ?11, ?12, ?13, ?14, ?15,
                    ?16, ?17, ?18, ?19,
                    ?20, ?21, ?22,
                    ?23, ?24,
                    ?25,
                    ?26, ?27,
                    ?28, ?29, ?30, ?31,
                    ?32, ?33
                )
                ON CONFLICT(profile_id) DO UPDATE SET
                    config_key = excluded.config_key,
                    use_uri = excluded.use_uri,
                    uri = excluded.uri,
                    host = excluded.host,
                    port = excluded.port,
                    user = excluded.user,
                    database_name = excluded.database_name,
                    ssl_mode = excluded.ssl_mode,
                    ssl_ca = excluded.ssl_ca,
                    ssl_cert = excluded.ssl_cert,
                    ssl_key = excluded.ssl_key,
                    password_secret_ref = excluded.password_secret_ref,
                    connect_timeout_secs = excluded.connect_timeout_secs,
                    ssh_tunnel_host = excluded.ssh_tunnel_host,
                    ssh_tunnel_port = excluded.ssh_tunnel_port,
                    ssh_tunnel_user = excluded.ssh_tunnel_user,
                    ssh_tunnel_auth_method = excluded.ssh_tunnel_auth_method,
                    ssh_tunnel_key_path = excluded.ssh_tunnel_key_path,
                    ssh_tunnel_passphrase_secret_ref = excluded.ssh_tunnel_passphrase_secret_ref,
                    ssh_tunnel_password_secret_ref = excluded.ssh_tunnel_password_secret_ref,
                    sqlite_path = excluded.sqlite_path,
                    sqlite_connection_id = excluded.sqlite_connection_id,
                    mongo_auth_database = excluded.mongo_auth_database,
                    redis_tls = excluded.redis_tls,
                    redis_database = excluded.redis_database,
                    dynamo_region = excluded.dynamo_region,
                    dynamo_profile = excluded.dynamo_profile,
                    dynamo_endpoint = excluded.dynamo_endpoint,
                    dynamo_table = excluded.dynamo_table,
                    external_kind = excluded.external_kind,
                    external_values_json = excluded.external_values_json
                "#,
                params![
                    config.id,
                    config.profile_id,
                    config.config_key,
                    config.use_uri as i32,
                    config.uri,
                    config.host,
                    config.port,
                    config.user,
                    config.database_name,
                    config.ssl_mode,
                    config.ssl_ca,
                    config.ssl_cert,
                    config.ssl_key,
                    config.password_secret_ref,
                    config.connect_timeout_secs,
                    config.ssh_tunnel_host,
                    config.ssh_tunnel_port,
                    config.ssh_tunnel_user,
                    config.ssh_tunnel_auth_method,
                    config.ssh_tunnel_key_path,
                    config.ssh_tunnel_passphrase_secret_ref,
                    config.ssh_tunnel_password_secret_ref,
                    config.sqlite_path,
                    config.sqlite_connection_id,
                    config.mongo_auth_database,
                    config.redis_tls as i32,
                    config.redis_database,
                    config.dynamo_region,
                    config.dynamo_profile,
                    config.dynamo_endpoint,
                    config.dynamo_table,
                    config.external_kind,
                    config.external_values_json,
                ],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        info!(
            "Upserted connection driver config for profile: {}",
            config.profile_id
        );
        Ok(())
    }

    /// Deletes the driver config for a connection profile.
    pub fn delete_for_profile(&self, profile_id: &str) -> Result<(), StorageError> {
        self.conn()
            .execute(
                "DELETE FROM cfg_connection_driver_configs WHERE profile_id = ?1",
                [profile_id],
            )
            .map_err(|source| StorageError::Sqlite {
                path: "dbflux.db".into(),
                source,
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mysql_config() -> DbConfig {
        DbConfig::MySQL {
            use_uri: false,
            uri: None,
            host: "db.example.internal".to_string(),
            port: 3307,
            user: "app_user".to_string(),
            database: Some("app_db".to_string()),
            ssl_mode: SslMode::Require,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        }
    }

    #[test]
    fn mysql_driver_config_round_trips_as_mysql() {
        let dto =
            ConnectionDriverConfigDto::from_db_config("profile-id".to_string(), &mysql_config());
        let config = dto.to_db_config().expect("config should round-trip");

        match config {
            DbConfig::MySQL {
                host,
                port,
                user,
                database,
                ssl_mode,
                ..
            } => {
                assert_eq!(host, "db.example.internal");
                assert_eq!(port, 3307);
                assert_eq!(user, "app_user");
                assert_eq!(database.as_deref(), Some("app_db"));
                assert_eq!(ssl_mode, SslMode::Require);
            }
            other => panic!("expected MySQL config, got {other:?}"),
        }
    }

    #[test]
    fn mariadb_driver_config_key_round_trips_as_mysql_config() {
        let mut dto =
            ConnectionDriverConfigDto::from_db_config("profile-id".to_string(), &mysql_config());
        dto.config_key = "MariaDB".to_string();

        let config = dto.to_db_config().expect("config should round-trip");

        assert!(matches!(config, DbConfig::MySQL { .. }));
    }
}
