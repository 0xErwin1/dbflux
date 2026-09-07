/// DBeaver `data-sources.json` / `credentials-config.json` importer.
///
/// `data-sources.json` lists connections under a top-level `connections` map,
/// keyed by DBeaver's internal connection id. `credentials-config.json` is an
/// optional companion file holding the saved username/password for each
/// connection, encrypted so it cannot be read by simply copying the file.
///
/// The credentials cipher is AES-128-CBC with a fixed key and a zero IV. The
/// key is not a secret DBeaver tries to protect: it ships in the DBeaver
/// source tree and is documented on the project's own wiki (see
/// https://github.com/dbeaver/dbeaver/wiki/Project-security, "Credentials
/// encryption"). Decrypting this file recovers exactly what a local DBeaver
/// install already recovers on every startup; the barrier is obscurity within
/// the file format, not confidentiality. A failure to decrypt (corrupted
/// file, unexpected key) is therefore a graceful per-batch skip on the
/// secrets only, never a hard error — the connections themselves still
/// import from `data-sources.json` alone.
use std::collections::HashMap;
use std::path::PathBuf;

use dbflux_core::{DbConfig, DbKind};
use secrecy::SecretString;
use serde::Deserialize;
use serde_json::Value;

use super::{
    ConnectionImporter, ExternalImportCandidate, ExternalImportError, ExternalImportOutcome,
    ExternalImportSkip, ImportInput, port_from_json,
};

/// Fixed AES-128-CBC key DBeaver uses for `credentials-config.json`, documented
/// publicly on the DBeaver wiki. Not a per-user secret.
const CREDENTIALS_KEY: [u8; 16] = [
    0xba, 0xbb, 0x4a, 0x9f, 0x77, 0x4a, 0xb8, 0x53, 0xc9, 0x6c, 0x2d, 0x65, 0x3d, 0xfe, 0x54, 0x4a,
];

const CREDENTIALS_IV: [u8; 16] = [0u8; 16];

/// The exporter discards this many leading bytes of the decrypted plaintext;
/// DBeaver prefixes the JSON payload with a fixed-size random salt block that
/// carries no data of its own.
const CREDENTIALS_PREFIX_LEN: usize = 16;

pub struct DBeaverImporter;

impl ConnectionImporter for DBeaverImporter {
    fn id(&self) -> &'static str {
        "dbeaver"
    }

    fn display_name(&self) -> &'static str {
        "DBeaver"
    }

    fn parse(&self, input: &ImportInput<'_>) -> Result<ExternalImportOutcome, ExternalImportError> {
        let text = std::str::from_utf8(input.primary.as_bytes()?)
            .map_err(|e| ExternalImportError::InvalidUtf8(e.to_string()))?;

        let file: DataSourcesFile = serde_json::from_str(text)?;

        let credentials = match input.secondary {
            Some(source) => decrypt_credentials(source.as_bytes()?),
            None => None,
        };

        let mut outcome = ExternalImportOutcome::default();

        for (connection_id, entry) in file.connections {
            let name = entry.name.clone().unwrap_or_default();

            match map_connection(&entry, connection_id.as_str(), credentials.as_ref()) {
                Ok(candidate) => outcome.candidates.push(candidate),
                Err(reason) => outcome.skips.push(ExternalImportSkip { name, reason }),
            }
        }

        Ok(outcome)
    }
}

#[derive(Deserialize)]
struct DataSourcesFile {
    #[serde(default)]
    connections: HashMap<String, DBeaverConnection>,
}

#[derive(Deserialize)]
struct DBeaverConnection {
    provider: Option<String>,
    name: Option<String>,
    configuration: Option<DBeaverConfiguration>,
}

#[derive(Deserialize)]
struct DBeaverConfiguration {
    host: Option<String>,
    port: Option<Value>,
    database: Option<String>,
    url: Option<String>,
    #[serde(rename = "configurationType")]
    configuration_type: Option<String>,
    #[serde(rename = "auth-properties", default)]
    auth_properties: HashMap<String, String>,
}

/// Maps one DBeaver connection entry to a candidate, or a skip reason.
fn map_connection(
    entry: &DBeaverConnection,
    connection_id: &str,
    credentials: Option<&HashMap<String, Value>>,
) -> Result<ExternalImportCandidate, String> {
    let name = entry
        .name
        .clone()
        .unwrap_or_else(|| connection_id.to_string());

    let provider = entry
        .provider
        .as_deref()
        .ok_or_else(|| "connection has no provider".to_string())?;

    let configuration = entry
        .configuration
        .as_ref()
        .ok_or_else(|| "connection has no configuration".to_string())?;

    let is_url_mode = configuration.configuration_type.as_deref() == Some("URL");
    let user = configuration.auth_properties.get("user").cloned();

    let (config, kind) = match provider {
        "postgresql" => {
            let (use_uri, uri, host, port, database) =
                connection_shape(configuration, is_url_mode, 5432)?;
            (
                DbConfig::Postgres {
                    use_uri,
                    uri,
                    host,
                    port,
                    user: user.unwrap_or_default(),
                    database,
                    ssl_mode: None,
                    ssl_root_cert_path: None,
                    ssl_client_cert_path: None,
                    ssl_client_key_path: None,
                    ssh_tunnel: None,
                    ssh_tunnel_profile_id: None,
                },
                DbKind::Postgres,
            )
        }

        "mysql" | "mariadb" => {
            let (use_uri, uri, host, port, database) =
                connection_shape(configuration, is_url_mode, 3306)?;
            let kind = if provider == "mariadb" {
                DbKind::MariaDB
            } else {
                DbKind::MySQL
            };
            (
                DbConfig::MySQL {
                    use_uri,
                    uri,
                    host,
                    port,
                    user: user.unwrap_or_default(),
                    database: Some(database).filter(|d| !d.is_empty()),
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
            // DbConfig::SQLite has no URI mode, unlike the other drivers here;
            // a URL-configured sqlite connection cannot be represented and is
            // skipped rather than guessed at by string-stripping the JDBC url.
            if is_url_mode {
                return Err("URL-mode configuration is not supported for sqlite".to_string());
            }

            let path = configuration
                .database
                .clone()
                .filter(|p| !p.is_empty())
                .ok_or_else(|| "sqlite connection has no database file path".to_string())?;

            (
                DbConfig::SQLite {
                    path: PathBuf::from(path),
                    connection_id: None,
                },
                DbKind::SQLite,
            )
        }

        "mongodb" => {
            let (use_uri, uri, host, port, database) =
                connection_shape(configuration, is_url_mode, 27017)?;
            (
                DbConfig::MongoDB {
                    use_uri,
                    uri,
                    host,
                    port,
                    user: user.clone().filter(|u| !u.is_empty()),
                    database: Some(database).filter(|d| !d.is_empty()),
                    auth_database: None,
                    ssl_mode: None,
                    ssl_root_cert_path: None,
                    ssl_client_cert_path: None,
                    ssl_client_key_path: None,
                    ssh_tunnel: None,
                    ssh_tunnel_profile_id: None,
                },
                DbKind::MongoDB,
            )
        }

        "redis" => {
            let (use_uri, uri, host, port, _database) =
                connection_shape(configuration, is_url_mode, 6379)?;
            (
                DbConfig::Redis {
                    use_uri,
                    uri,
                    host,
                    port,
                    user: user.clone().filter(|u| !u.is_empty()),
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
            )
        }

        "mssqlserver" | "sqlserver" => {
            let (use_uri, uri, host, port, database) =
                connection_shape(configuration, is_url_mode, 1433)?;
            (
                DbConfig::SqlServer {
                    use_uri,
                    uri,
                    host,
                    port,
                    user: user.unwrap_or_default(),
                    database: Some(database).filter(|d| !d.is_empty()),
                    instance: None,
                    ssl_mode: None,
                    trust_server_certificate: false,
                    ssl_root_cert_path: None,
                    ssh_tunnel: None,
                    ssh_tunnel_profile_id: None,
                },
                DbKind::SqlServer,
            )
        }

        other => return Err(format!("unsupported provider '{other}'")),
    };

    let secret = credentials.and_then(|map| {
        let entry = map.get(connection_id)?;
        find_credential_field(entry, "password")
    });

    Ok(ExternalImportCandidate {
        name,
        config,
        kind,
        secret: secret.map(SecretString::from),
        secret_skip_reason: None,
    })
}

/// Resolves the `(use_uri, uri, host, port, database)` tuple shared by every
/// URI-capable driver's `connection_shape`.
///
/// In URL mode the connection's raw `url` field is carried as-is into the
/// driver's `uri` field; the driver's own URI parser is responsible for
/// making sense of it. In manual mode, `host` is required — its absence is a
/// missing-fields skip rather than a silent empty-string default.
fn connection_shape(
    configuration: &DBeaverConfiguration,
    is_url_mode: bool,
    default_port: u16,
) -> Result<(bool, Option<String>, String, u16, String), String> {
    if is_url_mode {
        let url = configuration
            .url
            .clone()
            .ok_or_else(|| "URL-mode connection has no url".to_string())?;
        // DBeaver stores URL-mode connections with a `jdbc:` prefix
        // (e.g. `jdbc:postgresql://host/db`); DBflux's own URI fields expect
        // the driver's native scheme without it.
        let url = url.strip_prefix("jdbc:").map(str::to_string).unwrap_or(url);
        return Ok((true, Some(url), String::new(), default_port, String::new()));
    }

    let host = configuration
        .host
        .clone()
        .filter(|h| !h.is_empty())
        .ok_or_else(|| "missing required field 'host'".to_string())?;

    let port = port_from_json(configuration.port.as_ref()).unwrap_or(default_port);
    let database = configuration.database.clone().unwrap_or_default();

    Ok((false, None, host, port, database))
}

/// Decrypts `credentials-config.json` bytes into a map keyed by DBeaver
/// connection id. Returns `None` (rather than an error) on any failure —
/// wrong/rotated key material, truncated file, bad padding — so a corrupted
/// credentials file degrades to "no secrets available" instead of aborting
/// the whole batch.
fn decrypt_credentials(ciphertext: &[u8]) -> Option<HashMap<String, Value>> {
    use aes::Aes128;
    use cbc::Decryptor;
    use cbc::cipher::{BlockDecryptMut, KeyIvInit, block_padding::Pkcs7};

    if ciphertext.is_empty() || !ciphertext.len().is_multiple_of(16) {
        return None;
    }

    let mut buffer = ciphertext.to_vec();
    let decryptor = Decryptor::<Aes128>::new(&CREDENTIALS_KEY.into(), &CREDENTIALS_IV.into());
    let plaintext = decryptor.decrypt_padded_mut::<Pkcs7>(&mut buffer).ok()?;

    let payload = plaintext.get(CREDENTIALS_PREFIX_LEN..)?;
    serde_json::from_slice(payload).ok()
}

/// Searches a decrypted credentials entry for a named field, tolerating both
/// a flat `{ "user": ..., "password": ... }` shape and DBeaver's nested
/// `{ "#connection": { "user": ..., "password": ... } }` shape.
fn find_credential_field(entry: &Value, field: &str) -> Option<String> {
    if let Some(value) = entry.get(field).and_then(Value::as_str) {
        return Some(value.to_string());
    }

    entry
        .get("#connection")
        .and_then(|nested| nested.get(field))
        .and_then(Value::as_str)
        .map(str::to_string)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::external::{ImportSource, importers};
    use secrecy::ExposeSecret;
    use serde_json::json;

    fn parse_bytes(json: &str) -> ExternalImportOutcome {
        let source = ImportSource::Bytes(json.as_bytes().to_vec());
        DBeaverImporter
            .parse(&ImportInput::new(&source))
            .expect("parse")
    }

    #[test]
    fn registry_includes_dbeaver() {
        assert!(importers().iter().any(|i| i.id() == "dbeaver"));
    }

    #[test]
    fn happy_path_postgres_manual_connection() {
        let json = r#"{
            "connections": {
                "conn-1": {
                    "provider": "postgresql",
                    "driver": "postgres-jdbc",
                    "name": "Demo Postgres",
                    "save-password": true,
                    "configuration": {
                        "host": "db.example.invalid",
                        "port": 5432,
                        "database": "app",
                        "configurationType": "MANUAL",
                        "auth-properties": { "user": "demo" }
                    }
                }
            }
        }"#;

        let outcome = parse_bytes(json);

        assert!(
            outcome.skips.is_empty(),
            "unexpected skips: {:?}",
            outcome.skips.iter().map(|s| &s.reason).collect::<Vec<_>>()
        );
        assert_eq!(outcome.candidates.len(), 1);
        let candidate = outcome.candidates.first().expect("candidate");
        assert_eq!(candidate.name, "Demo Postgres");
        assert_eq!(candidate.kind, DbKind::Postgres);
        match &candidate.config {
            DbConfig::Postgres {
                host,
                port,
                user,
                database,
                use_uri,
                ..
            } => {
                assert_eq!(host, "db.example.invalid");
                assert_eq!(*port, 5432);
                assert_eq!(user, "demo");
                assert_eq!(database, "app");
                assert!(!use_uri);
            }
            other => panic!("expected Postgres config, got {other:?}"),
        }
    }

    #[test]
    fn tolerant_string_port() {
        let json = r#"{
            "connections": {
                "conn-1": {
                    "provider": "mysql",
                    "name": "Demo MySQL",
                    "configuration": {
                        "host": "db.example.invalid",
                        "port": "3307",
                        "database": "app",
                        "configurationType": "MANUAL",
                        "auth-properties": { "user": "demo" }
                    }
                }
            }
        }"#;

        let outcome = parse_bytes(json);
        assert_eq!(outcome.candidates.len(), 1);
        match &outcome.candidates.first().expect("candidate").config {
            DbConfig::MySQL { port, .. } => assert_eq!(*port, 3307),
            other => panic!("expected MySQL config, got {other:?}"),
        }
    }

    #[test]
    fn unknown_provider_is_skipped_with_reason() {
        let json = r#"{
            "connections": {
                "conn-1": {
                    "provider": "oracle",
                    "name": "Legacy Oracle",
                    "configuration": {
                        "host": "db.example.invalid",
                        "configurationType": "MANUAL"
                    }
                }
            }
        }"#;

        let outcome = parse_bytes(json);
        assert!(outcome.candidates.is_empty());
        assert_eq!(outcome.skips.len(), 1);
        assert!(
            outcome
                .skips
                .first()
                .expect("skip")
                .reason
                .contains("oracle")
        );
    }

    #[test]
    fn url_mode_maps_to_driver_uri_field() {
        let json = r#"{
            "connections": {
                "conn-1": {
                    "provider": "postgresql",
                    "name": "URL Postgres",
                    "configuration": {
                        "url": "jdbc:postgresql://db.example.invalid:5432/app",
                        "configurationType": "URL"
                    }
                }
            }
        }"#;

        let outcome = parse_bytes(json);
        assert_eq!(outcome.candidates.len(), 1);
        match &outcome.candidates.first().expect("candidate").config {
            DbConfig::Postgres { use_uri, uri, .. } => {
                assert!(use_uri);
                assert_eq!(
                    uri.as_deref(),
                    Some("postgresql://db.example.invalid:5432/app"),
                    "the leading `jdbc:` prefix must be stripped before storing the URI"
                );
            }
            other => panic!("expected Postgres config, got {other:?}"),
        }
    }

    #[test]
    fn url_mode_without_jdbc_prefix_is_stored_unchanged() {
        let json = r#"{
            "connections": {
                "conn-1": {
                    "provider": "postgresql",
                    "name": "URL Postgres",
                    "configuration": {
                        "url": "postgresql://db.example.invalid:5432/app",
                        "configurationType": "URL"
                    }
                }
            }
        }"#;

        let outcome = parse_bytes(json);
        assert_eq!(outcome.candidates.len(), 1);
        match &outcome.candidates.first().expect("candidate").config {
            DbConfig::Postgres { use_uri, uri, .. } => {
                assert!(use_uri);
                assert_eq!(
                    uri.as_deref(),
                    Some("postgresql://db.example.invalid:5432/app")
                );
            }
            other => panic!("expected Postgres config, got {other:?}"),
        }
    }

    #[test]
    fn sqlite_url_mode_is_skipped() {
        let json = r#"{
            "connections": {
                "conn-1": {
                    "provider": "sqlite",
                    "name": "URL Sqlite",
                    "configuration": {
                        "url": "jdbc:sqlite::memory:",
                        "configurationType": "URL"
                    }
                }
            }
        }"#;

        let outcome = parse_bytes(json);
        assert_eq!(outcome.candidates.len(), 0);
        assert_eq!(outcome.skips.len(), 1);
    }

    #[test]
    fn one_bad_entry_does_not_abort_the_batch() {
        let json = r#"{
            "connections": {
                "conn-1": {
                    "provider": "postgresql",
                    "name": "Good Postgres",
                    "configuration": {
                        "host": "db.example.invalid",
                        "port": 5432,
                        "database": "app",
                        "configurationType": "MANUAL"
                    }
                },
                "conn-2": {
                    "provider": "oracle",
                    "name": "Bad Oracle",
                    "configuration": { "configurationType": "MANUAL" }
                }
            }
        }"#;

        let outcome = parse_bytes(json);
        assert_eq!(outcome.candidates.len(), 1);
        assert_eq!(outcome.skips.len(), 1);
    }

    /// Encrypts a fixture credentials payload the same way DBeaver does, so
    /// the round trip is verified against our own decryptor rather than a
    /// hand-crafted byte string.
    fn encrypt_fixture_credentials(payload: &[u8]) -> Vec<u8> {
        use aes::Aes128;
        use cbc::Encryptor;
        use cbc::cipher::{BlockEncryptMut, KeyIvInit, block_padding::Pkcs7};

        let mut prefixed = vec![0u8; CREDENTIALS_PREFIX_LEN];
        prefixed.extend_from_slice(payload);

        let encryptor = Encryptor::<Aes128>::new(&CREDENTIALS_KEY.into(), &CREDENTIALS_IV.into());
        encryptor.encrypt_padded_vec_mut::<Pkcs7>(&prefixed)
    }

    #[test]
    fn dbeaver_credentials_round_trip_recovers_user_and_password() {
        let inner = json!({
            "conn-1": {
                "#connection": {
                    "user": "demo",
                    "password": "s3cret-fixture"
                }
            }
        });
        let ciphertext = encrypt_fixture_credentials(inner.to_string().as_bytes());

        let data_sources = r#"{
            "connections": {
                "conn-1": {
                    "provider": "postgresql",
                    "name": "Demo Postgres",
                    "configuration": {
                        "host": "db.example.invalid",
                        "port": 5432,
                        "database": "app",
                        "configurationType": "MANUAL",
                        "auth-properties": { "user": "demo" }
                    }
                }
            }
        }"#;

        let primary = ImportSource::Bytes(data_sources.as_bytes().to_vec());
        let secondary = ImportSource::Path(std::path::PathBuf::new());
        // Ensure the path variant is rejected up front for the secondary slot
        // when bytes are expected, then exercise the real bytes path below.
        assert!(secondary.as_bytes().is_err());

        let secondary = ImportSource::Bytes(ciphertext);
        let input = ImportInput::with_secondary(&primary, &secondary);

        let outcome = DBeaverImporter.parse(&input).expect("parse");
        assert_eq!(outcome.candidates.len(), 1);
        let secret = outcome
            .candidates
            .first()
            .expect("candidate")
            .secret
            .as_ref()
            .expect("password recovered");
        assert_eq!(secret.expose_secret(), "s3cret-fixture");
    }

    #[test]
    fn corrupted_credentials_file_is_a_graceful_skip_on_secret_only() {
        let data_sources = r#"{
            "connections": {
                "conn-1": {
                    "provider": "postgresql",
                    "name": "Demo Postgres",
                    "configuration": {
                        "host": "db.example.invalid",
                        "port": 5432,
                        "database": "app",
                        "configurationType": "MANUAL"
                    }
                }
            }
        }"#;

        let primary = ImportSource::Bytes(data_sources.as_bytes().to_vec());
        // Not a multiple of the AES block size: must not panic or abort parsing.
        let secondary = ImportSource::Bytes(vec![1, 2, 3]);
        let input = ImportInput::with_secondary(&primary, &secondary);

        let outcome = DBeaverImporter.parse(&input).expect("parse");
        assert_eq!(outcome.candidates.len(), 1);
        assert!(
            outcome
                .candidates
                .first()
                .expect("candidate")
                .secret
                .is_none()
        );
    }
}
