//! Per-query execution metadata stored by `InfluxConnection`.

use dbflux_core::{InfluxVersion, QueryLanguage};

use crate::injection::ResolvedWindow;

/// Metadata captured after each InfluxDB query execution.
///
/// Exposed via `InfluxConnection::last_query_metadata()` for debugging and
/// UI display (e.g., showing the effective time window that was queried).
#[derive(Debug, Clone)]
pub struct InfluxQueryMetadata {
    /// The API version used for this query.
    pub version: InfluxVersion,
    /// The query language used.
    pub language: QueryLanguage,
    /// The resolved time window, if one was injected by the driver.
    /// `None` when the user wrote their own time predicate.
    pub resolved_window: Option<ResolvedWindow>,
    /// The bucket (v2) or database (v1) that was queried.
    pub bucket_or_database: String,
    /// Whether the driver injected a time window into this query.
    pub injected_window: bool,
}
