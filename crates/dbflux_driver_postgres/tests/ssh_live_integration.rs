#![allow(clippy::result_large_err)]

use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbError, QueryRequest, SshAuthMethod, SshTunnelConfig,
    Value,
};
use dbflux_driver_postgres::PostgresDriver;
use std::env;
use std::path::PathBuf;

fn required_env(name: &str) -> Result<String, DbError> {
    env::var(name)
        .map_err(|_| DbError::InvalidProfile(format!("{name} must be set for the SSH live test")))
}

#[test]
#[ignore = "requires SSH and PostgreSQL test services"]
fn postgres_live_connects_through_ssh_with_private_key() -> Result<(), DbError> {
    let database_port = env::var("DBFLUX_TEST_DB_PORT")
        .unwrap_or_else(|_| "5432".to_string())
        .parse::<u16>()
        .map_err(|error| {
            DbError::InvalidProfile(format!("DBFLUX_TEST_DB_PORT must be a valid port: {error}"))
        })?;
    let ssh_port = required_env("DBFLUX_TEST_SSH_PORT")?
        .parse::<u16>()
        .map_err(|error| {
            DbError::InvalidProfile(format!(
                "DBFLUX_TEST_SSH_PORT must be a valid port: {error}"
            ))
        })?;

    let profile = ConnectionProfile::new(
        "live-postgres-ssh",
        DbConfig::Postgres {
            use_uri: false,
            uri: None,
            host: required_env("DBFLUX_TEST_DB_HOST")?,
            port: database_port,
            user: required_env("DBFLUX_TEST_DB_USER")?,
            database: required_env("DBFLUX_TEST_DB_NAME")?,
            ssl_mode: Some("disable".to_string()),
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: Some(SshTunnelConfig {
                host: env::var("DBFLUX_TEST_SSH_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
                port: ssh_port,
                user: required_env("DBFLUX_TEST_SSH_USER")?,
                auth_method: SshAuthMethod::PrivateKey {
                    key_path: Some(PathBuf::from(required_env("DBFLUX_TEST_SSH_KEY_PATH")?)),
                },
            }),
            ssh_tunnel_profile_id: None,
        },
    );

    let database_password = SecretString::from(required_env("DBFLUX_TEST_DB_PASSWORD")?);
    let ssh_passphrase = env::var("DBFLUX_TEST_SSH_PASSPHRASE")
        .ok()
        .map(SecretString::from);

    let connection = PostgresDriver::new().connect_with_secrets(
        &profile,
        Some(&database_password),
        ssh_passphrase.as_ref(),
    )?;

    connection.ping()?;
    let result = connection.execute(&QueryRequest::new("SELECT 42::bigint AS answer"))?;
    assert_eq!(result.rows, vec![vec![Value::Int(42)]]);

    Ok(())
}
