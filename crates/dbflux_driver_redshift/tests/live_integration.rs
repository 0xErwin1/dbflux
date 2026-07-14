#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err,
    clippy::unwrap_in_result
)]

//! Live smoke tests against a real Amazon Redshift cluster.
//!
//! No local or Docker-based Redshift emulator exists (LocalStack only
//! emulates the management/Data API, not the wire protocol), so these tests
//! read connection details from environment variables instead of spinning up
//! a testcontainer, and are `#[ignore]`d by default. Run explicitly with:
//!
//! ```text
//! DBFLUX_TEST_REDSHIFT_HOST=cluster.abc123.us-east-1.redshift.amazonaws.com \
//! DBFLUX_TEST_REDSHIFT_USER=awsuser \
//! DBFLUX_TEST_REDSHIFT_PASSWORD=... \
//! DBFLUX_TEST_REDSHIFT_DATABASE=dev \
//! cargo nextest run -p dbflux_driver_redshift --run-ignored all
//! ```
//!
//! The metadata-introspection tests below need additional cluster fixtures
//! and read them from further optional/required env vars (see each test's
//! `#[ignore]` reason and body):
//!
//! - `DBFLUX_TEST_REDSHIFT_SCHEMA` (defaults to `public`)
//! - `DBFLUX_TEST_REDSHIFT_EMPTY_SCHEMA` — a schema with zero tables/views
//! - `DBFLUX_TEST_REDSHIFT_TABLE` — a table with a declared DISTKEY and SORTKEY
//! - `DBFLUX_TEST_REDSHIFT_PK_TABLE` — a table with a declared (informational) PRIMARY KEY
//! - `DBFLUX_TEST_REDSHIFT_SUPER_TABLE` / `DBFLUX_TEST_REDSHIFT_SUPER_COLUMN` — a table/column of type SUPER or VARBYTE

use dbflux_core::secrecy::SecretString;
use dbflux_core::{ConnectionProfile, DbConfig, DbDriver, DbError, QueryRequest};
use dbflux_driver_redshift::RedshiftDriver;

struct LiveRedshiftEnv {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

impl LiveRedshiftEnv {
    fn from_env() -> Self {
        let host = std::env::var("DBFLUX_TEST_REDSHIFT_HOST").expect(
            "DBFLUX_TEST_REDSHIFT_HOST must be set to run Redshift live tests (see module docs)",
        );
        let port = std::env::var("DBFLUX_TEST_REDSHIFT_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(5439);
        let user =
            std::env::var("DBFLUX_TEST_REDSHIFT_USER").unwrap_or_else(|_| "awsuser".to_string());
        let password = std::env::var("DBFLUX_TEST_REDSHIFT_PASSWORD")
            .expect("DBFLUX_TEST_REDSHIFT_PASSWORD must be set to run Redshift live tests");
        let database =
            std::env::var("DBFLUX_TEST_REDSHIFT_DATABASE").unwrap_or_else(|_| "dev".to_string());

        Self {
            host,
            port,
            user,
            password,
            database,
        }
    }

    fn profile_with_ssl_mode(&self, ssl_mode: &str) -> ConnectionProfile {
        ConnectionProfile::new(
            "live-redshift",
            DbConfig::Redshift {
                use_uri: false,
                uri: None,
                host: self.host.clone(),
                port: self.port,
                user: self.user.clone(),
                database: self.database.clone(),
                ssl_mode: Some(ssl_mode.to_string()),
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        )
    }
}

#[test]
#[ignore = "requires a real Amazon Redshift cluster; see module docs for required env vars"]
fn redshift_live_connect_and_select_1_with_sslmode_require() -> Result<(), DbError> {
    let env = LiveRedshiftEnv::from_env();
    let driver = RedshiftDriver::new();
    let profile = env.profile_with_ssl_mode("require");

    let connection = driver.connect_with_secrets(
        &profile,
        Some(&SecretString::from(env.password.clone())),
        None,
    )?;

    let result = connection.execute(&QueryRequest::new("SELECT 1"))?;
    assert_eq!(result.rows.len(), 1);

    Ok(())
}

#[test]
#[ignore = "requires a TLS-only Amazon Redshift cluster; see module docs for required env vars"]
fn redshift_live_sslmode_disable_on_tls_only_cluster_returns_clear_error() {
    let env = LiveRedshiftEnv::from_env();
    let driver = RedshiftDriver::new();
    let profile = env.profile_with_ssl_mode("disable");

    let result = driver.connect_with_secrets(
        &profile,
        Some(&SecretString::from(env.password.clone())),
        None,
    );

    match result {
        Err(DbError::ConnectionFailed(formatted)) => {
            assert!(!formatted.message.is_empty());
        }
        Err(other) => panic!("expected DbError::ConnectionFailed, got {other:?}"),
        Ok(_) => panic!("expected sslmode=disable to fail against a TLS-only cluster"),
    }
}

#[test]
#[ignore = "performs a real network connection attempt to an unreachable host"]
fn redshift_live_invalid_host_returns_clear_error_not_panic() {
    let driver = RedshiftDriver::new();
    let profile = ConnectionProfile::new(
        "live-redshift-invalid-host",
        DbConfig::Redshift {
            use_uri: false,
            uri: None,
            host: "redshift-does-not-exist.invalid".to_string(),
            port: 5439,
            user: "awsuser".to_string(),
            database: "dev".to_string(),
            ssl_mode: Some("require".to_string()),
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );

    let result = driver.connect_with_secrets(&profile, None, None);

    match result {
        Err(DbError::ConnectionFailed(formatted)) => assert!(!formatted.message.is_empty()),
        Err(other) => panic!("expected DbError::ConnectionFailed, got {other:?}"),
        Ok(_) => panic!("expected an unreachable host to fail to connect"),
    }
}

#[test]
#[ignore = "requires a real Amazon Redshift cluster with at least one non-system schema/table/view; see module docs"]
fn redshift_live_schema_lists_schemas_tables_and_views() -> Result<(), DbError> {
    let env = LiveRedshiftEnv::from_env();
    let driver = RedshiftDriver::new();
    let profile = env.profile_with_ssl_mode("require");

    let connection = driver.connect_with_secrets(
        &profile,
        Some(&SecretString::from(env.password.clone())),
        None,
    )?;

    let snapshot = connection.schema()?;
    let schemas = snapshot.schemas();
    assert!(
        !schemas.is_empty(),
        "expected at least one non-system schema"
    );

    let total_tables: usize = schemas.iter().map(|schema| schema.tables.len()).sum();
    assert!(
        total_tables > 0,
        "expected at least one table across all schemas"
    );

    Ok(())
}

#[test]
#[ignore = "requires a real Amazon Redshift cluster with an empty schema; see DBFLUX_TEST_REDSHIFT_EMPTY_SCHEMA"]
fn redshift_live_empty_schema_lists_zero_tables_without_error() -> Result<(), DbError> {
    let env = LiveRedshiftEnv::from_env();
    let empty_schema = std::env::var("DBFLUX_TEST_REDSHIFT_EMPTY_SCHEMA")
        .expect("DBFLUX_TEST_REDSHIFT_EMPTY_SCHEMA must name a schema with zero tables/views");

    let driver = RedshiftDriver::new();
    let profile = env.profile_with_ssl_mode("require");
    let connection = driver.connect_with_secrets(
        &profile,
        Some(&SecretString::from(env.password.clone())),
        None,
    )?;

    let snapshot = connection.schema()?;
    let schema = snapshot
        .schemas()
        .iter()
        .find(|schema| schema.name == empty_schema)
        .unwrap_or_else(|| panic!("schema '{empty_schema}' not found in schema snapshot"));

    assert!(schema.tables.is_empty(), "expected zero tables");
    assert!(schema.views.is_empty(), "expected zero views");

    Ok(())
}

#[test]
#[ignore = "requires a real Amazon Redshift table with a declared DISTKEY and SORTKEY; see DBFLUX_TEST_REDSHIFT_TABLE"]
fn redshift_live_table_details_surface_distkey_and_sortkey() -> Result<(), DbError> {
    let env = LiveRedshiftEnv::from_env();
    let schema =
        std::env::var("DBFLUX_TEST_REDSHIFT_SCHEMA").unwrap_or_else(|_| "public".to_string());
    let table = std::env::var("DBFLUX_TEST_REDSHIFT_TABLE")
        .expect("DBFLUX_TEST_REDSHIFT_TABLE must name a table with a DISTKEY and SORTKEY");

    let driver = RedshiftDriver::new();
    let profile = env.profile_with_ssl_mode("require");
    let connection = driver.connect_with_secrets(
        &profile,
        Some(&SecretString::from(env.password.clone())),
        None,
    )?;

    let details = connection.table_details(&env.database, Some(&schema), &table)?;
    let hints = details.storage_hints.unwrap_or_default();

    assert!(
        hints.iter().any(|hint| hint.label == "Distribution Key"),
        "expected a Distribution Key storage hint"
    );
    assert!(
        hints.iter().any(|hint| hint.label == "Sort Key"),
        "expected a Sort Key storage hint"
    );

    Ok(())
}

#[test]
#[ignore = "requires a real Amazon Redshift table with a declared (informational) primary key; see DBFLUX_TEST_REDSHIFT_PK_TABLE"]
fn redshift_live_declared_primary_key_is_advisory_with_no_fabricated_index() -> Result<(), DbError>
{
    let env = LiveRedshiftEnv::from_env();
    let schema =
        std::env::var("DBFLUX_TEST_REDSHIFT_SCHEMA").unwrap_or_else(|_| "public".to_string());
    let table = std::env::var("DBFLUX_TEST_REDSHIFT_PK_TABLE")
        .expect("DBFLUX_TEST_REDSHIFT_PK_TABLE must name a table with a declared PRIMARY KEY");

    let driver = RedshiftDriver::new();
    let profile = env.profile_with_ssl_mode("require");
    let connection = driver.connect_with_secrets(
        &profile,
        Some(&SecretString::from(env.password.clone())),
        None,
    )?;

    let details = connection.table_details(&env.database, Some(&schema), &table)?;

    let columns = details.columns.expect("columns must be loaded");
    assert!(
        columns.iter().any(|column| column.is_primary_key),
        "expected at least one column marked is_primary_key"
    );

    assert!(
        details.indexes.is_none(),
        "Redshift has no true indexes; table_details must not fabricate an IndexData list"
    );

    let hints = details.storage_hints.unwrap_or_default();
    assert!(
        hints
            .iter()
            .any(|hint| hint.label == "Constraints advisory"),
        "expected a Constraints advisory storage hint marking the PK as non-enforced"
    );

    Ok(())
}

#[test]
#[ignore = "requires a real Amazon Redshift table with a SUPER or VARBYTE column; see DBFLUX_TEST_REDSHIFT_SUPER_TABLE/DBFLUX_TEST_REDSHIFT_SUPER_COLUMN"]
fn redshift_live_super_or_varbyte_column_renders_as_text_without_panic() -> Result<(), DbError> {
    let env = LiveRedshiftEnv::from_env();
    let schema =
        std::env::var("DBFLUX_TEST_REDSHIFT_SCHEMA").unwrap_or_else(|_| "public".to_string());
    let table = std::env::var("DBFLUX_TEST_REDSHIFT_SUPER_TABLE")
        .expect("DBFLUX_TEST_REDSHIFT_SUPER_TABLE must name a table with a SUPER/VARBYTE column");
    let column = std::env::var("DBFLUX_TEST_REDSHIFT_SUPER_COLUMN")
        .expect("DBFLUX_TEST_REDSHIFT_SUPER_COLUMN must name that SUPER/VARBYTE column");

    let driver = RedshiftDriver::new();
    let profile = env.profile_with_ssl_mode("require");
    let connection = driver.connect_with_secrets(
        &profile,
        Some(&SecretString::from(env.password.clone())),
        None,
    )?;

    let sql = format!("SELECT \"{column}\" FROM \"{schema}\".\"{table}\" LIMIT 1");
    let result = connection.execute(&QueryRequest::new(&sql))?;

    assert_eq!(result.columns.len(), 1);
    // A real SUPER/VARBYTE OID must classify as `ColumnKind::Text` here, not
    // `Unknown` — an `Unknown` result would mean the hardcoded extended-type
    // OID literals in `redshift_oid_to_kind` do not match this cluster.
    assert_eq!(result.columns[0].kind, dbflux_core::ColumnKind::Text);

    if let Some(row) = result.rows.first() {
        match &row[0] {
            dbflux_core::Value::Text(_) | dbflux_core::Value::Null => {}
            other => {
                panic!("expected SUPER/VARBYTE column to decode as text or null, got {other:?}")
            }
        }
    }

    Ok(())
}

#[test]
#[ignore = "requires a real Amazon Redshift cluster; see module docs for required env vars"]
fn redshift_live_select_into_is_rejected_at_the_wire_not_executed() {
    let env = LiveRedshiftEnv::from_env();
    let driver = RedshiftDriver::new();
    let profile = env.profile_with_ssl_mode("require");

    let connection = driver
        .connect_with_secrets(
            &profile,
            Some(&SecretString::from(env.password.clone())),
            None,
        )
        .expect("connect should succeed");

    // `classify_query_for_language` treats `SELECT ... INTO ...` as a read
    // statement (it starts with SELECT), so `ensure_read_only` lets it
    // through. This test documents that the wire layer still rejects it:
    // PostgreSQL (and Redshift, which shares its wire protocol) refuses
    // `SELECT INTO` inside the extended query protocol's `Parse` step
    // (`client.prepare`), so no table is ever created even though the
    // shared classifier alone would not have blocked it.
    let table_name = format!("dbflux_select_into_probe_{}", uuid::Uuid::new_v4().simple());
    let sql = format!("SELECT 1 AS x INTO {table_name}");

    let result = connection.execute(&QueryRequest::new(&sql));

    assert!(
        result.is_err(),
        "SELECT INTO must be rejected at the wire layer, not silently create a table"
    );
}

/// Documents the TLS root-CA + client-certificate (mutual TLS) path against a
/// real cluster. No local or Docker-based Redshift emulator exists, so this is
/// `#[ignore]`d like the other live tests. Run it against a cluster fronted by
/// a private CA (and, for mTLS, requiring a client certificate) with:
///
/// ```text
/// DBFLUX_TEST_REDSHIFT_HOST=... \
/// DBFLUX_TEST_REDSHIFT_PASSWORD=... \
/// DBFLUX_TEST_REDSHIFT_SSL_ROOT_CERT=/path/to/private-ca.pem \
/// DBFLUX_TEST_REDSHIFT_SSL_CLIENT_CERT=/path/to/client.pem \
/// DBFLUX_TEST_REDSHIFT_SSL_CLIENT_KEY=/path/to/client-key.pem \
/// cargo nextest run -p dbflux_driver_redshift --run-ignored all \
///   redshift_live_verify_full_with_private_ca_and_client_cert
/// ```
///
/// A `verify-full` connection here proves the pinned private CA is honored
/// (the handshake would otherwise fail against a cert the system trust store
/// does not chain to) and, when the client cert/key are set, that mutual TLS
/// is negotiated rather than being inert.
#[test]
#[ignore = "requires a real Amazon Redshift cluster fronted by a private CA / client cert; see test docs for env vars"]
fn redshift_live_verify_full_with_private_ca_and_client_cert() {
    let env = LiveRedshiftEnv::from_env();

    let ssl_root_cert_path = std::env::var("DBFLUX_TEST_REDSHIFT_SSL_ROOT_CERT").ok();
    let ssl_client_cert_path = std::env::var("DBFLUX_TEST_REDSHIFT_SSL_CLIENT_CERT").ok();
    let ssl_client_key_path = std::env::var("DBFLUX_TEST_REDSHIFT_SSL_CLIENT_KEY").ok();

    let profile = ConnectionProfile::new(
        "live-redshift-mtls",
        DbConfig::Redshift {
            use_uri: false,
            uri: None,
            host: env.host.clone(),
            port: env.port,
            user: env.user.clone(),
            database: env.database.clone(),
            ssl_mode: Some("verify-full".to_string()),
            ssl_root_cert_path,
            ssl_client_cert_path,
            ssl_client_key_path,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );

    let driver = RedshiftDriver::new();
    let connection = driver
        .connect_with_secrets(
            &profile,
            Some(&SecretString::from(env.password.clone())),
            None,
        )
        .expect("verify-full connection with the pinned private CA / client cert should succeed");

    connection
        .ping()
        .expect("ping over the mutually-authenticated TLS connection should succeed");
}
