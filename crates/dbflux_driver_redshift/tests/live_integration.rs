#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
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
