/// Beekeeper Studio `app.db` importer.
///
/// Beekeeper Studio persists saved connections as a TypeORM entity in its own
/// SQLite database. The table is expected to be named `saved_connection`, but
/// column and table naming has drifted across Beekeeper releases, so the
/// table is located by its column signature rather than assumed by name.
///
/// Passwords are encrypted with a key generated per Beekeeper install and
/// never leave that install's keychain/config, so this importer cannot
/// recover them. A connection with a saved password still imports; only the
/// secret is skipped, with a reason telling the user to re-enter it.
use dbflux_core::{DbConfig, DbKind};
use rusqlite::Connection;

use super::{
    ConnectionImporter, ExternalImportCandidate, ExternalImportError, ExternalImportOutcome,
    ExternalImportSkip, ImportInput,
};

/// Column names this importer looks for, tried in order per logical field to
/// tolerate naming drift between Beekeeper Studio versions.
struct ColumnSet {
    name: String,
    connection_type: String,
    host: Option<String>,
    port: Option<String>,
    username: Option<String>,
    default_database: Option<String>,
    socket_path: Option<String>,
    password: Option<String>,
}

pub struct BeekeeperImporter;

impl ConnectionImporter for BeekeeperImporter {
    fn id(&self) -> &'static str {
        "beekeeper"
    }

    fn display_name(&self) -> &'static str {
        "Beekeeper Studio"
    }

    fn parse(&self, input: &ImportInput<'_>) -> Result<ExternalImportOutcome, ExternalImportError> {
        let path = input.primary.as_path()?;

        let conn =
            Connection::open(path).map_err(|e| ExternalImportError::Sqlite(e.to_string()))?;

        let table = find_connection_table(&conn)?;
        let columns = resolve_columns(&conn, &table)?;

        let query = format!("SELECT * FROM \"{table}\"");
        let mut stmt = conn
            .prepare(&query)
            .map_err(|e| ExternalImportError::Sqlite(e.to_string()))?;

        let mut outcome = ExternalImportOutcome::default();

        let mut rows = stmt
            .query([])
            .map_err(|e| ExternalImportError::Sqlite(e.to_string()))?;

        while let Some(row) = rows
            .next()
            .map_err(|e| ExternalImportError::Sqlite(e.to_string()))?
        {
            let name: String = get_text(row, &columns.name).unwrap_or_default();
            let connection_type = get_text(row, &columns.connection_type).unwrap_or_default();

            match map_row(row, &columns, &connection_type) {
                Ok((config, kind, has_secret)) => {
                    let secret_skip_reason = has_secret.then(|| {
                        "password is encrypted with a Beekeeper-local key; re-enter it after import"
                            .to_string()
                    });

                    outcome.candidates.push(ExternalImportCandidate {
                        name,
                        config,
                        kind,
                        secret: None,
                        secret_skip_reason,
                    });
                }
                Err(reason) => outcome.skips.push(ExternalImportSkip { name, reason }),
            }
        }

        Ok(outcome)
    }
}

/// Locates the saved-connections table by column signature: a table with at
/// least `name`, a connection-type-like column, and `host` is treated as the
/// saved-connections table regardless of its exact name.
fn find_connection_table(conn: &Connection) -> Result<String, ExternalImportError> {
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .map_err(|e| ExternalImportError::Sqlite(e.to_string()))?;

    let table_names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| ExternalImportError::Sqlite(e.to_string()))?
        .filter_map(Result::ok)
        .collect();

    for table_name in &table_names {
        let columns = table_columns(conn, table_name)?;
        let has_name = find_column(&columns, &["name"]).is_some();
        let has_type = find_column(&columns, &["connectionType", "connection_type"]).is_some();
        let has_host = find_column(&columns, &["host"]).is_some();

        if has_name && has_type && has_host {
            return Ok(table_name.clone());
        }
    }

    Err(ExternalImportError::NotBeekeeperDatabase(
        "no table with name/connectionType/host columns was found".to_string(),
    ))
}

fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>, ExternalImportError> {
    let query = format!("PRAGMA table_info(\"{table}\")");
    let mut stmt = conn
        .prepare(&query)
        .map_err(|e| ExternalImportError::Sqlite(e.to_string()))?;

    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|e| ExternalImportError::Sqlite(e.to_string()))?
        .filter_map(Result::ok)
        .collect();

    Ok(columns)
}

/// Returns the first candidate name present in `columns`, case-insensitively.
fn find_column(columns: &[String], candidates: &[&str]) -> Option<String> {
    candidates.iter().find_map(|candidate| {
        columns
            .iter()
            .find(|c| c.eq_ignore_ascii_case(candidate))
            .cloned()
    })
}

fn resolve_columns(conn: &Connection, table: &str) -> Result<ColumnSet, ExternalImportError> {
    let columns = table_columns(conn, table)?;

    let name = find_column(&columns, &["name"])
        .ok_or_else(|| ExternalImportError::NotBeekeeperDatabase("missing 'name' column".into()))?;
    let connection_type = find_column(&columns, &["connectionType", "connection_type"])
        .ok_or_else(|| {
            ExternalImportError::NotBeekeeperDatabase("missing 'connectionType' column".into())
        })?;

    Ok(ColumnSet {
        name,
        connection_type,
        host: find_column(&columns, &["host"]),
        port: find_column(&columns, &["port"]),
        username: find_column(&columns, &["username"]),
        default_database: find_column(&columns, &["defaultDatabase", "default_database"]),
        socket_path: find_column(&columns, &["socketPath", "socket_path"]),
        password: find_column(&columns, &["password"]),
    })
}

fn get_text(row: &rusqlite::Row<'_>, column: &str) -> Option<String> {
    row.get::<_, Option<String>>(column).ok().flatten()
}

fn get_port(row: &rusqlite::Row<'_>, column: &Option<String>) -> Option<u16> {
    let column = column.as_ref()?;

    if let Ok(Some(text)) = row.get::<_, Option<String>>(column.as_str())
        && let Ok(port) = text.trim().parse::<u16>()
    {
        return Some(port);
    }

    row.get::<_, Option<i64>>(column.as_str())
        .ok()
        .flatten()
        .and_then(|v| u16::try_from(v).ok())
}

fn get_opt_text(row: &rusqlite::Row<'_>, column: &Option<String>) -> Option<String> {
    let column = column.as_ref()?;
    get_text(row, column).filter(|v| !v.is_empty())
}

/// Maps one saved-connection row into a `(DbConfig, DbKind, has_secret)` triple.
fn map_row(
    row: &rusqlite::Row<'_>,
    columns: &ColumnSet,
    connection_type: &str,
) -> Result<(DbConfig, DbKind, bool), String> {
    let has_secret = columns
        .password
        .as_ref()
        .and_then(|c| get_text(row, c))
        .is_some_and(|v| !v.is_empty());

    let host = get_opt_text(row, &columns.host);
    let port = get_port(row, &columns.port);
    let username = get_opt_text(row, &columns.username);
    let database = get_opt_text(row, &columns.default_database);
    let socket_path = get_opt_text(row, &columns.socket_path);

    let (config, kind) = match connection_type {
        "postgresql" => (
            DbConfig::Postgres {
                use_uri: false,
                uri: None,
                host: host.ok_or_else(|| "missing required field 'host'".to_string())?,
                port: port.unwrap_or(5432),
                user: username.unwrap_or_default(),
                database: database.unwrap_or_default(),
                ssl_mode: None,
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
            DbKind::Postgres,
        ),

        "mysql" | "mariadb" => {
            let kind = if connection_type == "mariadb" {
                DbKind::MariaDB
            } else {
                DbKind::MySQL
            };
            (
                DbConfig::MySQL {
                    use_uri: false,
                    uri: None,
                    host: host.ok_or_else(|| "missing required field 'host'".to_string())?,
                    port: port.unwrap_or(3306),
                    user: username.unwrap_or_default(),
                    database,
                    ssl_mode: None,
                    ssl_root_cert_path: None,
                    ssl_client_cert_path: None,
                    ssl_client_key_path: None,
                    ssh_tunnel: None,
                    ssh_tunnel_profile_id: None,
                },
                kind,
            )
        }

        "sqlite" => {
            let path = database
                .or(socket_path)
                .ok_or_else(|| "sqlite connection has no database file path".to_string())?;
            (
                DbConfig::SQLite {
                    path: path.into(),
                    connection_id: None,
                },
                DbKind::SQLite,
            )
        }

        "sqlserver" => (
            DbConfig::SqlServer {
                use_uri: false,
                uri: None,
                host: host.ok_or_else(|| "missing required field 'host'".to_string())?,
                port: port.unwrap_or(1433),
                user: username.unwrap_or_default(),
                database,
                instance: None,
                ssl_mode: None,
                trust_server_certificate: false,
                ssl_root_cert_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
            DbKind::SqlServer,
        ),

        "mongodb" => (
            DbConfig::MongoDB {
                use_uri: false,
                uri: None,
                host: host.ok_or_else(|| "missing required field 'host'".to_string())?,
                port: port.unwrap_or(27017),
                user: username,
                database,
                auth_database: None,
                ssl_mode: None,
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
            DbKind::MongoDB,
        ),

        "redis" => (
            DbConfig::Redis {
                use_uri: false,
                uri: None,
                host: host.ok_or_else(|| "missing required field 'host'".to_string())?,
                port: port.unwrap_or(6379),
                user: username,
                database: None,
                tls: false,
                ssl_mode: None,
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
                topology: None,
                sentinel_master_name: None,
                additional_nodes: None,
            },
            DbKind::Redis,
        ),

        other => return Err(format!("unsupported connectionType '{other}'")),
    };

    Ok((config, kind, has_secret))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::external::{ImportSource, importers};
    use tempfile::NamedTempFile;

    fn seed_db(rows: &[(&str, &str, &str, i64, &str, &str, &str)]) -> NamedTempFile {
        let file = NamedTempFile::new().expect("temp file");
        let conn = Connection::open(file.path()).expect("open");

        conn.execute(
            "CREATE TABLE saved_connection (
                id INTEGER PRIMARY KEY,
                name TEXT,
                connectionType TEXT,
                host TEXT,
                port INTEGER,
                username TEXT,
                defaultDatabase TEXT,
                socketPath TEXT,
                password TEXT,
                ssl INTEGER,
                readOnlyMode INTEGER
            )",
            [],
        )
        .expect("create table");

        for (name, connection_type, host, port, username, default_database, password) in rows {
            conn.execute(
                "INSERT INTO saved_connection
                    (name, connectionType, host, port, username, defaultDatabase, password)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    name,
                    connection_type,
                    host,
                    port,
                    username,
                    default_database,
                    password
                ],
            )
            .expect("insert row");
        }

        file
    }

    #[test]
    fn registry_includes_beekeeper() {
        assert!(importers().iter().any(|i| i.id() == "beekeeper"));
    }

    #[test]
    fn happy_path_postgres_connection() {
        let file = seed_db(&[(
            "Demo Postgres",
            "postgresql",
            "db.example.invalid",
            5432,
            "demo",
            "app",
            "",
        )]);

        let source = ImportSource::Path(file.path().to_path_buf());
        let outcome = BeekeeperImporter
            .parse(&ImportInput::new(&source))
            .expect("parse");

        assert!(outcome.skips.is_empty());
        assert_eq!(outcome.candidates.len(), 1);
        let candidate = outcome.candidates.first().expect("candidate");
        assert_eq!(candidate.name, "Demo Postgres");
        assert_eq!(candidate.kind, DbKind::Postgres);
        assert!(candidate.secret_skip_reason.is_none());
        match &candidate.config {
            DbConfig::Postgres {
                host,
                port,
                user,
                database,
                ..
            } => {
                assert_eq!(host, "db.example.invalid");
                assert_eq!(*port, 5432);
                assert_eq!(user, "demo");
                assert_eq!(database, "app");
            }
            other => panic!("expected Postgres config, got {other:?}"),
        }
    }

    #[test]
    fn encrypted_password_still_imports_connection_with_secret_skip() {
        let file = seed_db(&[(
            "Demo MySQL",
            "mysql",
            "db.example.invalid",
            3306,
            "demo",
            "app",
            "{\"iv\":\"...\",\"ciphertext\":\"...\"}",
        )]);

        let source = ImportSource::Path(file.path().to_path_buf());
        let outcome = BeekeeperImporter
            .parse(&ImportInput::new(&source))
            .expect("parse");

        assert_eq!(outcome.candidates.len(), 1);
        let candidate = outcome.candidates.first().expect("candidate");
        assert!(candidate.secret.is_none());
        assert!(candidate.secret_skip_reason.is_some());
    }

    #[test]
    fn unsupported_connection_type_is_skipped_with_reason() {
        let file = seed_db(&[(
            "Legacy Cassandra",
            "cassandra",
            "db.example.invalid",
            9042,
            "demo",
            "",
            "",
        )]);

        let source = ImportSource::Path(file.path().to_path_buf());
        let outcome = BeekeeperImporter
            .parse(&ImportInput::new(&source))
            .expect("parse");

        assert!(outcome.candidates.is_empty());
        assert_eq!(outcome.skips.len(), 1);
        assert!(
            outcome
                .skips
                .first()
                .expect("skip")
                .reason
                .contains("cassandra")
        );
    }

    #[test]
    fn non_beekeeper_sqlite_returns_typed_error() {
        let file = NamedTempFile::new().expect("temp file");
        let conn = Connection::open(file.path()).expect("open");
        conn.execute("CREATE TABLE unrelated_table (id INTEGER PRIMARY KEY)", [])
            .expect("create table");

        let source = ImportSource::Path(file.path().to_path_buf());
        let result = BeekeeperImporter.parse(&ImportInput::new(&source));

        assert!(matches!(
            result,
            Err(ExternalImportError::NotBeekeeperDatabase(_))
        ));
    }

    #[test]
    fn one_bad_entry_does_not_abort_the_batch() {
        let file = seed_db(&[
            (
                "Good Postgres",
                "postgresql",
                "db.example.invalid",
                5432,
                "demo",
                "app",
                "",
            ),
            (
                "Bad Cassandra",
                "cassandra",
                "db.example.invalid",
                9042,
                "demo",
                "",
                "",
            ),
        ]);

        let source = ImportSource::Path(file.path().to_path_buf());
        let outcome = BeekeeperImporter
            .parse(&ImportInput::new(&source))
            .expect("parse");

        assert_eq!(outcome.candidates.len(), 1);
        assert_eq!(outcome.skips.len(), 1);
    }
}
