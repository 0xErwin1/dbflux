use std::path::PathBuf;

/// Describes how the fmt tracing layer should write output.
///
/// `Stderr` and `File` are both acceptable for the GUI binary and driver host.
/// `NonBlockingFile` is required for the MCP server to avoid fmt output
/// blocking the async runtime.
pub enum FmtWriter {
    /// Write to stderr (blocking — acceptable for GUI and driver host).
    Stderr,
    /// Write to a file synchronously (small-volume tools).
    File(PathBuf),
    /// Write to a file via a non-blocking background writer (MCP server).
    ///
    /// Returns a `WorkerGuard` that must be kept alive for the subscriber lifetime.
    NonBlockingFile(PathBuf),
}
