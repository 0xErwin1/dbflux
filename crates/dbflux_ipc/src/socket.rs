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
    let suffix = if cfg!(debug_assertions) { "-debug" } else { "" };
    format!("dbflux{suffix}.sock").to_ns_name::<GenericNamespaced>()
}

/// Returns the local socket name for an IPC driver-host process.
///
/// Each driver-host gets a unique socket name based on its identifier.
pub fn driver_socket_name(id: &str) -> io::Result<Name<'static>> {
    let suffix = if cfg!(debug_assertions) { "-debug" } else { "" };
    format!("dbflux-driver-{id}{suffix}.sock").to_ns_name::<GenericNamespaced>()
}
