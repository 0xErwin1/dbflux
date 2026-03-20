use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Stdio,
    UnixSocket,
    Tcp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapConfig {
    pub enabled_transports: Vec<TransportKind>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BootstrapError {
    #[error("at least one transport must be enabled")]
    NoTransportEnabled,
    #[error("transport not supported in v1: tcp-only launch")]
    TcpOnlyNotSupported,
}

pub fn validate_v1_transport_profile(config: &BootstrapConfig) -> Result<(), BootstrapError> {
    if config.enabled_transports.is_empty() {
        return Err(BootstrapError::NoTransportEnabled);
    }

    let has_stdio = config
        .enabled_transports
        .iter()
        .any(|transport| matches!(transport, TransportKind::Stdio));
    let has_unix_socket = config
        .enabled_transports
        .iter()
        .any(|transport| matches!(transport, TransportKind::UnixSocket));
    let has_tcp = config
        .enabled_transports
        .iter()
        .any(|transport| matches!(transport, TransportKind::Tcp));

    if has_tcp && !has_stdio && !has_unix_socket {
        return Err(BootstrapError::TcpOnlyNotSupported);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{BootstrapConfig, BootstrapError, TransportKind, validate_v1_transport_profile};

    #[test]
    fn stdio_transport_is_allowed() {
        let result = validate_v1_transport_profile(&BootstrapConfig {
            enabled_transports: vec![TransportKind::Stdio],
        });

        assert_eq!(result, Ok(()));
    }

    #[test]
    fn unix_socket_transport_is_allowed() {
        let result = validate_v1_transport_profile(&BootstrapConfig {
            enabled_transports: vec![TransportKind::UnixSocket],
        });

        assert_eq!(result, Ok(()));
    }

    #[test]
    fn tcp_only_launch_is_rejected() {
        let result = validate_v1_transport_profile(&BootstrapConfig {
            enabled_transports: vec![TransportKind::Tcp],
        });

        assert_eq!(result, Err(BootstrapError::TcpOnlyNotSupported));
    }
}
