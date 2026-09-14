//! Dump-analyzer seam for DBFlux.
//!
//! Defines the `DumpAnalyzer` trait and the value types it produces. Drivers
//! whose native export format can be scanned offline (e.g. a Redis RDB file)
//! implement `DumpAnalyzer` and return `Some(analyzer)` from
//! `DbDriver::dump_analyzer()`; all other drivers inherit the default `None`.
//!
//! The UI never inspects `driver_id` — it calls `driver.dump_analyzer()` to
//! decide whether to show the "Analyze dump" affordance, and reads
//! `size_caveat()` to explain that serialized size on disk is not the same
//! as the space a key occupies in live memory.
//!
//! Implementations MUST keep memory bounded regardless of dump size: the
//! largest-keys and prefix-rollup results are computed as a streaming
//! aggregation over the file, never by materializing a full key list.

use std::path::Path;

/// One entry in the bounded top-N largest-keys list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpKeyEntry {
    /// Key name, decoded lossily to UTF-8 for display.
    pub key: String,
    /// Driver-reported value type (e.g. `"string"`, `"hash"`, `"stream"`).
    pub type_name: String,
    /// Bytes consumed by this entry in the source dump's serialization format.
    ///
    /// This is the on-disk/on-wire size, not the key's footprint in live
    /// memory — allocator overhead, pointers, and in-memory encoding
    /// differences mean the two numbers diverge, sometimes by a lot.
    pub serialized_bytes: u64,
    /// Absolute expiry time in epoch milliseconds, if the key carries one.
    pub expires_at_ms: Option<i64>,
    /// Logical database index the key belongs to.
    pub database: u32,
}

/// One bucket in the bounded prefix-rollup aggregation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpPrefixEntry {
    /// Prefix bucket label (see the driver's prefix-splitting rule for how
    /// this is derived from key names).
    pub prefix: String,
    /// Number of keys aggregated into this bucket.
    pub key_count: u64,
    /// Sum of `serialized_bytes` for every key aggregated into this bucket.
    pub serialized_bytes: u64,
}

/// Result of analyzing one dump file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpAnalysisReport {
    /// Total number of keys observed across every logical database.
    pub total_keys: u64,
    /// Total serialized bytes consumed by every key entry observed.
    pub total_serialized_bytes: u64,
    /// Per-type breakdown: `(type_name, key_count, serialized_bytes)`.
    pub keys_by_type: Vec<(String, u64, u64)>,
    /// Bounded top-N largest keys by serialized size, descending.
    pub largest_keys: Vec<DumpKeyEntry>,
    /// Bounded by-prefix aggregation.
    pub prefix_rollup: Vec<DumpPrefixEntry>,
}

/// Error produced while analyzing a dump file.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DumpAnalysisError {
    /// Filesystem I/O failure while reading the dump.
    #[error("I/O error reading dump: {0}")]
    Io(String),

    /// The dump's binary format could not be parsed at the given byte offset.
    #[error("malformed dump at offset {offset}: {message}")]
    Format { offset: u64, message: String },

    /// Analysis was cancelled by the caller before completion.
    #[error("dump analysis cancelled")]
    Cancelled,
}

/// Analyzes a driver's native dump/export format offline, without a live connection.
///
/// Implementations MUST stream the file rather than loading it into memory,
/// and MUST keep the aggregated result bounded (top-N largest keys, capped
/// prefix rollup) regardless of how many keys the dump contains.
pub trait DumpAnalyzer: Send + Sync {
    /// Human-readable name for the dump format (e.g. `"Redis RDB"`).
    fn display_name(&self) -> &'static str;

    /// File extensions this analyzer accepts, without the leading dot (e.g. `["rdb"]`).
    fn file_extensions(&self) -> &'static [&'static str];

    /// Explains why serialized dump size and live memory usage differ.
    ///
    /// The UI surfaces this text verbatim next to the analysis results.
    fn size_caveat(&self) -> &'static str;

    /// Analyzes the dump file at `path`.
    ///
    /// `progress` is called periodically with `(bytes_read, total_bytes)`;
    /// `total_bytes` is `None` when the total size could not be determined.
    /// `cancelled` is polled periodically and analysis stops as soon as it
    /// returns `true`, returning `Err(DumpAnalysisError::Cancelled)`.
    fn analyze(
        &self,
        path: &Path,
        progress: &(dyn Fn(u64, Option<u64>) + Sync),
        cancelled: &(dyn Fn() -> bool + Sync),
    ) -> Result<DumpAnalysisReport, DumpAnalysisError>;
}
