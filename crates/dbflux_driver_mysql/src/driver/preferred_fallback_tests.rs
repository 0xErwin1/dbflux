use super::is_tls_negotiation_failure;
use mysql::{DriverError, Error, MySqlError};
use std::io;

fn server_error(code: u16, state: &str, message: &str) -> Error {
    Error::MySqlError(MySqlError {
        state: state.to_string(),
        message: message.to_string(),
        code,
    })
}

#[test]
fn falls_back_when_the_server_does_not_offer_tls() {
    assert!(is_tls_negotiation_failure(&Error::DriverError(
        DriverError::TlsNotSupported
    )));
}

#[test]
fn falls_back_when_the_tls_handshake_fails() {
    let handshake = io::Error::new(
        io::ErrorKind::InvalidData,
        rustls::Error::General("handshake failure".to_string()),
    );

    assert!(is_tls_negotiation_failure(&Error::IoError(handshake)));
}

#[test]
fn reports_server_errors_without_a_plaintext_retry() {
    for error in [
        server_error(1045, "28000", "Access denied for user 'app'@'%'"),
        server_error(1049, "42000", "Unknown database 'missing'"),
        server_error(1044, "42000", "Access denied for user to database 'app'"),
    ] {
        assert!(!is_tls_negotiation_failure(&error), "{error}");
    }
}

#[test]
fn reports_network_errors_without_a_plaintext_retry() {
    for kind in [
        io::ErrorKind::ConnectionRefused,
        io::ErrorKind::TimedOut,
        io::ErrorKind::UnexpectedEof,
    ] {
        let error = Error::IoError(io::Error::from(kind));
        assert!(!is_tls_negotiation_failure(&error), "{kind:?}");
    }
}
