/// TablePlus `Connections.plist` importer.
///
/// TablePlus keeps its saved connections as an XML property list: a top-level
/// array with one dictionary per connection. On Windows the file lives at
/// `%LocalAppData%\com.tinyapp.TablePlus\Data\Connections.plist`; on macOS at
/// `~/Library/Application Support/com.tinyapp.TablePlus/Data/Connections.plist`,
/// or under `com.tinyapp.TablePlus-setapp` for the Setapp build.
///
/// Only the Windows layout has been checked against a real file. The macOS
/// field names (such as `isUseSocket`) follow the ones Raycast's TablePlus
/// extension reads.
///
/// macOS can write a plist in binary form, which starts with `bplist00`; that
/// form is rejected with a clear error rather than misreported as malformed
/// XML.
///
/// Ports are stored as strings and flags as integers. A SQLite connection
/// stores the full file path in `DatabaseHost`, while `DatabaseName` holds
/// only the file name. A socket connection is flagged `UseSock` on Windows and
/// `isUseSocket` on macOS; the DBFlux configs this importer builds have no
/// socket mode, so those entries are skipped.
///
/// Passwords are not in the plist. TablePlus keeps them in its own encrypted
/// store on Windows and in the Keychain on macOS, so this importer cannot
/// recover them.
/// Every connection that authenticates still imports; only the secret is
/// skipped, with a reason telling the user to re-enter it.
mod plist;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::PathBuf;

use dbflux_core::{DbConfig, DbKind, SshAuthMethod, SshTunnelConfig};

use self::plist::PlistValue;
use super::{
    ConnectionImporter, ExternalImportCandidate, ExternalImportError, ExternalImportOutcome,
    ExternalImportSkip, ImportInput,
};

const BINARY_PLIST_MAGIC: &[u8] = b"bplist00";

const PASSWORD_SKIP_REASON: &str =
    "TablePlus does not store passwords in its connection file; re-enter it after import";

pub struct TablePlusImporter;

impl ConnectionImporter for TablePlusImporter {
    fn id(&self) -> &'static str {
        "tableplus"
    }

    fn display_name(&self) -> &'static str {
        "TablePlus"
    }

    fn parse(&self, input: &ImportInput<'_>) -> Result<ExternalImportOutcome, ExternalImportError> {
        let bytes = input.primary.as_bytes()?;
        if bytes.starts_with(BINARY_PLIST_MAGIC) {
            return Err(ExternalImportError::BinaryPlist);
        }

        let text = std::str::from_utf8(bytes)
            .map_err(|e| ExternalImportError::InvalidUtf8(e.to_string()))?;

        let PlistValue::Array(entries) =
            plist::parse(text).map_err(ExternalImportError::InvalidPlist)?
        else {
            return Err(ExternalImportError::Structure(
                "expected a top-level array of connections".to_string(),
            ));
        };

        let mut outcome = ExternalImportOutcome::default();

        for value in &entries {
            let PlistValue::Dictionary(fields) = value else {
                outcome.skips.push(ExternalImportSkip {
                    name: String::new(),
                    reason: "entry is not a dictionary".to_string(),
                });
                continue;
            };

            let entry = Entry(fields);
            match map_connection(&entry) {
                Ok(candidate) => outcome.candidates.push(candidate),
                Err(reason) => outcome.skips.push(ExternalImportSkip {
                    name: entry.trimmed("ConnectionName").unwrap_or_default(),
                    reason,
                }),
            }
        }

        Ok(outcome)
    }
}

struct Entry<'a>(&'a HashMap<String, PlistValue>);

impl Entry<'_> {
    /// A string field exactly as stored, or `None` when it is absent or blank.
    fn text(&self, key: &str) -> Option<String> {
        match self.0.get(key)? {
            PlistValue::String(value) if !value.trim().is_empty() => Some(value.clone()),
            _ => None,
        }
    }

    /// A string field with surrounding whitespace removed, for values such as
    /// names and hostnames where that whitespace never means anything.
    fn trimmed(&self, key: &str) -> Option<String> {
        self.text(key).map(|value| value.trim().to_string())
    }

    fn flag(&self, key: &str) -> bool {
        match self.0.get(key) {
            Some(PlistValue::Integer(value)) => *value != 0,
            Some(PlistValue::Boolean(value)) => *value,
            _ => false,
        }
    }

    fn port(&self, key: &str) -> Option<u16> {
        match self.0.get(key)? {
            PlistValue::String(value) => value.trim().parse::<u16>().ok(),
            PlistValue::Integer(value) => u16::try_from(*value).ok(),
            _ => None,
        }
    }

    fn required_host(&self) -> Result<String, String> {
        self.trimmed("DatabaseHost")
            .ok_or_else(|| "missing required field 'host'".to_string())
    }
}

/// Maps one TablePlus connection dictionary to a candidate, or a skip reason.
fn map_connection(entry: &Entry<'_>) -> Result<ExternalImportCandidate, String> {
    let name = entry
        .trimmed("ConnectionName")
        .or_else(|| entry.trimmed("ID"))
        .ok_or_else(|| "connection has no name".to_string())?;

    let driver = entry
        .trimmed("Driver")
        .ok_or_else(|| "connection has no driver".to_string())?;

    if entry.flag("UseSock") || entry.flag("isUseSocket") {
        return Err("socket connections are not supported".to_string());
    }

    let (config, kind) = match driver.as_str() {
        "PostgreSQL" => (postgres_config(entry)?, DbKind::Postgres),
        "MySQL" => (mysql_config(entry)?, DbKind::MySQL),
        "MariaDB" => (mysql_config(entry)?, DbKind::MariaDB),
        "MicrosoftSQLServer" => (sql_server_config(entry)?, DbKind::SqlServer),
        "Redis" => (redis_config(entry)?, DbKind::Redis),
        "SQLite" => (sqlite_config(entry)?, DbKind::SQLite),
        other => return Err(format!("unsupported driver '{other}'")),
    };

    let secret_skip_reason = (kind != DbKind::SQLite).then(|| PASSWORD_SKIP_REASON.to_string());

    Ok(ExternalImportCandidate {
        name,
        config,
        kind,
        secret: None,
        secret_skip_reason,
    })
}

fn postgres_config(entry: &Entry<'_>) -> Result<DbConfig, String> {
    Ok(DbConfig::Postgres {
        use_uri: false,
        uri: None,
        host: entry.required_host()?,
        port: entry.port("DatabasePort").unwrap_or(5432),
        user: entry.text("DatabaseUser").unwrap_or_default(),
        database: entry.text("DatabaseName").unwrap_or_default(),
        ssl_mode: None,
        ssl_root_cert_path: None,
        ssl_client_cert_path: None,
        ssl_client_key_path: None,
        ssh_tunnel: ssh_tunnel(entry)?,
        ssh_tunnel_profile_id: None,
    })
}

fn mysql_config(entry: &Entry<'_>) -> Result<DbConfig, String> {
    Ok(DbConfig::MySQL {
        use_uri: false,
        uri: None,
        host: entry.required_host()?,
        port: entry.port("DatabasePort").unwrap_or(3306),
        user: entry.text("DatabaseUser").unwrap_or_default(),
        database: entry.text("DatabaseName"),
        ssl_mode: None,
        ssl_root_cert_path: None,
        ssl_client_cert_path: None,
        ssl_client_key_path: None,
        ssh_tunnel: ssh_tunnel(entry)?,
        ssh_tunnel_profile_id: None,
    })
}

fn sql_server_config(entry: &Entry<'_>) -> Result<DbConfig, String> {
    Ok(DbConfig::SqlServer {
        use_uri: false,
        uri: None,
        host: entry.required_host()?,
        port: entry.port("DatabasePort").unwrap_or(1433),
        user: entry.text("DatabaseUser").unwrap_or_default(),
        database: entry.text("DatabaseName"),
        instance: None,
        ssl_mode: None,
        trust_server_certificate: false,
        ssl_root_cert_path: None,
        ssh_tunnel: ssh_tunnel(entry)?,
        ssh_tunnel_profile_id: None,
    })
}

fn redis_config(entry: &Entry<'_>) -> Result<DbConfig, String> {
    Ok(DbConfig::Redis {
        use_uri: false,
        uri: None,
        host: entry.required_host()?,
        port: entry.port("DatabasePort").unwrap_or(6379),
        user: entry.text("DatabaseUser"),
        database: entry
            .text("DatabaseName")
            .and_then(|index| index.trim().parse::<u32>().ok()),
        tls: false,
        ssl_mode: None,
        ssl_root_cert_path: None,
        ssl_client_cert_path: None,
        ssl_client_key_path: None,
        ssh_tunnel: ssh_tunnel(entry)?,
        ssh_tunnel_profile_id: None,
        topology: None,
        sentinel_master_name: None,
        additional_nodes: None,
    })
}

/// TablePlus stores a SQLite file's full path in `DatabaseHost`.
fn sqlite_config(entry: &Entry<'_>) -> Result<DbConfig, String> {
    let path = entry
        .text("DatabaseHost")
        .ok_or_else(|| "sqlite connection has no database file path".to_string())?;

    Ok(DbConfig::SQLite {
        path: PathBuf::from(path),
        connection_id: None,
    })
}

/// Builds the SSH tunnel for a connection marked `isOverSSH`.
///
/// TablePlus records a private key by file name only, not by path, so the
/// tunnel falls back to the SSH agent and default keys rather than guessing
/// where the file lives.
fn ssh_tunnel(entry: &Entry<'_>) -> Result<Option<SshTunnelConfig>, String> {
    if !entry.flag("isOverSSH") {
        return Ok(None);
    }

    let host = entry
        .trimmed("ServerAddress")
        .ok_or_else(|| "SSH is enabled but no SSH server address is set".to_string())?;

    let auth_method = if entry.flag("isUsePrivateKey") {
        SshAuthMethod::PrivateKey { key_path: None }
    } else {
        SshAuthMethod::Password
    };

    Ok(Some(SshTunnelConfig {
        host,
        port: entry.port("ServerPort").unwrap_or(22),
        user: entry.text("ServerUser").unwrap_or_default(),
        auth_method,
        ssh_config_host: None,
    }))
}
