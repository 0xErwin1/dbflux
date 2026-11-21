use interprocess::local_socket::{GenericNamespaced, Name, ToNsName};
use std::io;

/// Returns the local socket name for the main DBFlux app-control channel.
///
/// Debug and release builds use distinct names so both can run simultaneously.
/// The underlying transport is platform-specific:
/// - Linux: abstract namespace Unix domain socket
/// - macOS: Unix domain socket in `/tmp/`
/// - Windows: named pipe
pub fn socket_name() -> io::Result<Name<'static>> {
    app_control_logical_name().to_ns_name::<GenericNamespaced>()
}

fn app_control_logical_name() -> String {
    let suffix = if cfg!(debug_assertions) { "-debug" } else { "" };
    format!("dbflux{suffix}.sock")
}

/// Native Tokio pipe opening avoids interprocess's blocking connection waiter.
#[cfg(all(windows, feature = "telemetry"))]
pub fn app_control_named_pipe_path() -> String {
    named_pipe_path(&app_control_logical_name())
}

#[cfg(all(feature = "telemetry", any(windows, test)))]
fn named_pipe_path(logical_name: &str) -> String {
    format!(r"\\.\pipe\{logical_name}")
}

#[cfg(all(feature = "telemetry", test))]
mod tests {
    #[test]
    fn native_pipe_mapping_preserves_logical_name() {
        let expected = if cfg!(debug_assertions) {
            r"\\.\pipe\dbflux-debug.sock"
        } else {
            r"\\.\pipe\dbflux.sock"
        };
        assert_eq!(
            super::named_pipe_path(&super::app_control_logical_name()),
            expected
        );
    }
}

/// Returns the local socket name for an IPC driver-host process.
///
/// Each driver-host gets a unique socket name based on its identifier.
pub fn driver_socket_name(id: &str) -> io::Result<Name<'static>> {
    let suffix = if cfg!(debug_assertions) { "-debug" } else { "" };
    format!("dbflux-driver-{id}{suffix}.sock").to_ns_name::<GenericNamespaced>()
}

pub fn auth_provider_socket_name(id: &str) -> io::Result<Name<'static>> {
    let suffix = if cfg!(debug_assertions) { "-debug" } else { "" };
    format!("dbflux-auth-provider-{id}{suffix}.sock").to_ns_name::<GenericNamespaced>()
}

pub fn secret_provider_socket_name(id: &str) -> io::Result<Name<'static>> {
    let suffix = if cfg!(debug_assertions) { "-debug" } else { "" };
    format!("dbflux-secret-provider-{id}{suffix}.sock").to_ns_name::<GenericNamespaced>()
}
