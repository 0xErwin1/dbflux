use crate::PostgresDriver;
use dbflux_core::{ConnectionProfile, DbConfig, DbDriver, DbError};

/// A self-signed EC certificate (P-256) that `native_tls` accepts as a root
/// CA and, with [`TEST_CLIENT_KEY_PEM`], as a client identity. It never
/// completes a handshake in these tests.
const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIBhzCCAS2gAwIBAgIUfDqUB953evSPLrRZKIAd1X3MCjYwCgYIKoZIzj0EAwIw\nGTEXMBUGA1UEAwwOREJGbHV4IFRlc3QgQ0EwHhcNMjYwNzE0MDA1MjIyWhcNMzYw\nNzExMDA1MjIyWjAZMRcwFQYDVQQDDA5EQkZsdXggVGVzdCBDQTBZMBMGByqGSM49\nAgEGCCqGSM49AwEHA0IABOFT5ZRd/rfnrUqTlNwrr9re7uketQQVH75KLhtCI+20\nw/mw4zpxD6qN0stYTUGGgBIlEHLqiCst1FRYEW/SCcGjUzBRMB0GA1UdDgQWBBRE\nfkCdgdaJ5XNuMH8foiUchEXQSzAfBgNVHSMEGDAWgBREfkCdgdaJ5XNuMH8foiUc\nhEXQSzAPBgNVHRMBAf8EBTADAQH/MAoGCCqGSM49BAMCA0gAMEUCIQD7tjjn6DCZ\nns9mSa35tCapkZPLQOL3lwhXI/9yZDr7WQIgejmcfurHGyGi88nkFtU3kvppcPU6\niDUm/dkd5PZo3ck=\n-----END CERTIFICATE-----\n";

const TEST_CLIENT_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgZmGGhghb5mjXoMRq\nSIgHgMKHiGoK1t5+k0wO/C8gYC6hRANCAAThU+WUXf63561Kk5TcK6/a3u7pHrUE\nFR++Si4bQiPttMP5sOM6cQ+qjdLLWE1BhoASJRBy6ogrLdRUWBFv0gnB\n-----END PRIVATE KEY-----\n";

/// A temp directory removed on drop, so a failing assertion still cleans up.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(label: &str) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let path = std::env::temp_dir().join(format!(
            "dbflux_pg_tls_{label}_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("create scratch dir");
        Self(path)
    }

    fn write(&self, name: &str, contents: &str) -> String {
        let path = self.0.join(name);
        std::fs::write(&path, contents).expect("write scratch file");
        path.to_string_lossy().into_owned()
    }

    fn missing(&self, name: &str) -> String {
        self.0.join(name).to_string_lossy().into_owned()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!("failed to remove {}: {error}", self.0.display());
        }
    }
}

fn closed_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener.local_addr().expect("bound address").port()
}

fn form_profile(
    ssl_mode: &str,
    root: Option<String>,
    cert: Option<String>,
    key: Option<String>,
) -> ConnectionProfile {
    ConnectionProfile::new(
        "tls",
        DbConfig::Postgres {
            use_uri: false,
            uri: None,
            host: "127.0.0.1".to_string(),
            port: closed_port(),
            user: "testuser".to_string(),
            database: "testdb".to_string(),
            ssl_mode: Some(ssl_mode.to_string()),
            ssl_root_cert_path: root,
            ssl_client_cert_path: cert,
            ssl_client_key_path: key,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    )
}

fn uri_profile(uri: String) -> ConnectionProfile {
    ConnectionProfile::new(
        "tls-uri",
        DbConfig::Postgres {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 5432,
            user: String::new(),
            database: String::new(),
            ssl_mode: None,
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    )
}

fn connect_error(profile: &ConnectionProfile) -> String {
    match PostgresDriver::new().connect(profile) {
        Ok(_) => panic!("nothing listens on the port, connect must fail"),
        Err(DbError::ConnectionFailed(formatted)) => formatted.to_display_string(),
        Err(other) => panic!("expected ConnectionFailed, got {other:?}"),
    }
}

#[test]
fn form_missing_root_certificate_is_reported_by_path() {
    let scratch = Scratch::new("form_root");
    let root = scratch.missing("ca.pem");

    let message = connect_error(&form_profile("verify-full", Some(root.clone()), None, None));

    assert!(
        message.contains("TLS root certificate") && message.contains(&root),
        "error should name the root certificate file: {message}"
    );
}

#[test]
fn form_missing_client_key_is_reported_by_path() {
    let scratch = Scratch::new("form_key");
    let cert = scratch.write("client.pem", TEST_CA_PEM);
    let key = scratch.missing("client.key");

    let message = connect_error(&form_profile(
        "require",
        None,
        Some(cert),
        Some(key.clone()),
    ));

    assert!(
        message.contains("TLS client key") && message.contains(&key),
        "error should name the client key file: {message}"
    );
}

#[test]
fn form_invalid_root_certificate_is_reported_by_path() {
    let scratch = Scratch::new("form_garbage");
    let root = scratch.write("ca.pem", "not a certificate");

    let message = connect_error(&form_profile("verify-ca", Some(root.clone()), None, None));

    assert!(
        message.contains("Invalid TLS root certificate") && message.contains(&root),
        "error should name the invalid file: {message}"
    );
}

#[test]
fn form_valid_certificates_reach_the_network() {
    let scratch = Scratch::new("form_valid");
    let root = scratch.write("ca.pem", TEST_CA_PEM);
    let cert = scratch.write("client.pem", TEST_CA_PEM);
    let key = scratch.write("client.key", TEST_CLIENT_KEY_PEM);

    let message = connect_error(&form_profile(
        "verify-full",
        Some(root),
        Some(cert),
        Some(key),
    ));

    assert!(
        message.contains("Connection refused"),
        "valid certificates should build the connector and fail only on the socket: {message}"
    );
}

#[test]
fn form_modes_without_chain_validation_ignore_a_stale_root_certificate() {
    let scratch = Scratch::new("form_stale_root");
    let root = scratch.missing("ca.pem");

    for mode in ["require", "prefer"] {
        let message = connect_error(&form_profile(mode, Some(root.clone()), None, None));
        assert!(
            !message.contains(&root) && message.contains("Connection refused"),
            "{mode} must not load the root certificate: {message}"
        );
    }
}

#[test]
fn prefer_with_missing_client_key_fails_before_any_connection_attempt() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let port = listener.local_addr().expect("addr").port();
    let scratch = Scratch::new("prefer_missing");
    let cert = scratch.write("client.pem", TEST_CA_PEM);
    let key = scratch.missing("client.key");

    let mut profile = form_profile("prefer", None, Some(cert), Some(key.clone()));
    if let DbConfig::Postgres { port: target, .. } = &mut profile.config {
        *target = port;
    }

    let message = connect_error(&profile);
    assert!(message.contains(&key), "{message}");
    assert!(
        listener.accept().is_err(),
        "no TCP connection must be attempted"
    );
}

#[test]
fn form_disable_ignores_certificate_paths() {
    let scratch = Scratch::new("form_disable");
    let root = scratch.missing("ca.pem");

    let message = connect_error(&form_profile("disable", Some(root), None, None));

    assert!(message.contains("Connection refused"), "{message}");
}

#[test]
fn uri_sslrootcert_is_accepted_and_configures_the_connector() {
    let scratch = Scratch::new("uri_root");
    let root = scratch.missing("ca.pem");
    let uri = format!(
        "postgresql://user@127.0.0.1:{}/db?sslmode=verify-full&sslrootcert={root}",
        closed_port()
    );

    let message = connect_error(&uri_profile(uri));

    assert!(
        !message.contains("unknown option"),
        "sslrootcert must not reach the wire parser: {message}"
    );
    assert!(
        message.contains("TLS root certificate") && message.contains(&root),
        "error should name the root certificate file: {message}"
    );
}

#[test]
fn uri_valid_certificates_reach_the_network() {
    let scratch = Scratch::new("uri_valid");
    let root = scratch.write("ca.pem", TEST_CA_PEM);
    let cert = scratch.write("client.pem", TEST_CA_PEM);
    let key = scratch.write("client.key", TEST_CLIENT_KEY_PEM);
    let uri = format!(
        "postgresql://user@127.0.0.1:{}/db?sslmode=verify-full&sslrootcert={root}&sslcert={cert}&sslkey={key}",
        closed_port()
    );

    let message = connect_error(&uri_profile(uri));

    assert!(!message.contains("unknown option"), "{message}");
    assert!(message.contains("Connection refused"), "{message}");
}

#[test]
fn uri_sslcert_without_sslkey_is_rejected() {
    let scratch = Scratch::new("uri_pair");
    let cert = scratch.write("client.pem", TEST_CA_PEM);
    let uri = format!(
        "postgresql://user@127.0.0.1:{}/db?sslmode=require&sslcert={cert}",
        closed_port()
    );

    let message = connect_error(&uri_profile(uri));

    assert!(
        message.contains("without a matching private key"),
        "{message}"
    );
}

#[test]
fn take_uri_tls_files_strips_the_libpq_parameters_and_keeps_the_rest() {
    let (uri, files) = super::take_uri_tls_files(
        "postgresql://user@host/db?sslmode=verify-full&sslrootcert=/path/ca.pem&sslcert=/path/client%20cert.pem&sslkey=/path/client.key&application_name=x",
    );

    assert_eq!(
        uri,
        "postgresql://user@host/db?sslmode=verify-full&application_name=x"
    );
    assert_eq!(
        files,
        super::PgTlsFiles {
            root_cert_path: Some("/path/ca.pem".to_string()),
            client_cert_path: Some("/path/client cert.pem".to_string()),
            client_key_path: Some("/path/client.key".to_string()),
        }
    );
}

#[test]
fn take_uri_tls_files_drops_an_empty_query_and_blank_values() {
    let (uri, files) = super::take_uri_tls_files("postgresql://user@host/db?sslrootcert=");

    assert_eq!(uri, "postgresql://user@host/db");
    assert_eq!(files, super::PgTlsFiles::default());
}

#[test]
fn take_uri_tls_files_leaves_a_uri_without_query_unchanged() {
    let (uri, files) = super::take_uri_tls_files("postgresql://user@host/db");

    assert_eq!(uri, "postgresql://user@host/db");
    assert_eq!(files, super::PgTlsFiles::default());
}

#[test]
fn take_uri_tls_files_last_occurrence_wins_and_keeps_unrelated_keys() {
    let (uri, files) = super::take_uri_tls_files(
        "postgresql://h/d?sslrootcert=/a&x=1&sslrootcert=%2Fb%2Fc.pem&flag&sslrootcertx=/z&sslkey=a+b",
    );

    assert_eq!(uri, "postgresql://h/d?x=1&flag&sslrootcertx=/z");
    assert_eq!(files.root_cert_path.as_deref(), Some("/b/c.pem"));
    assert_eq!(files.client_key_path.as_deref(), Some("a+b"));
}
