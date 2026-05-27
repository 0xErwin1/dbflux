//! Drift-check helpers for the dashboard sync flow.
//!
//! The actual fetch (one `GetDashboard` call) is performed by the driver via
//! the `DashboardSource` trait inside a `cx.background_executor().spawn`
//! block on `DashboardDocument`. This module owns the pure logic that
//! decides:
//!
//! - whether a cached drift outcome is still valid (60 s cache, per spec
//!   R2.6 / R6.3), and
//! - how to classify a freshly fetched upstream content hash against the
//!   locally stored hash (`DriftCheckOutcome`).
//!
//! Keeping these pieces pure makes them unit-testable without spinning up
//! GPUI windows or AWS clients.

use chrono::{DateTime, Duration, Utc};

use super::sync_pill::DriftCheckOutcome;

/// Drift cache window, in seconds, per spec R2.6.
///
/// A successful drift check is considered fresh for this many seconds; the
/// background task short-circuits when the cached result is within this
/// window.
pub const DRIFT_CACHE_TTL_SECONDS: i64 = 60;

/// Classify a fresh upstream content hash against the locally stored one.
///
/// Pure function; no IO. Used by the drift-check task once it has the
/// remote dashboard in hand.
pub fn classify_drift(stored_hash: Option<&str>, remote_hash: &str) -> DriftCheckOutcome {
    match stored_hash {
        Some(stored) if stored == remote_hash => DriftCheckOutcome::Clean,
        _ => DriftCheckOutcome::Drifted,
    }
}

/// Returns `true` when a previous successful drift check is still within
/// the cache window and a fresh network call should be skipped.
///
/// `last_checked_at` is the wall-clock timestamp of the last completed
/// check (`source_last_synced_at` on the dashboard row when the previous
/// check landed cleanly). `now` is injected so the function is
/// deterministic in tests.
pub fn drift_cache_is_fresh(last_checked_at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    match last_checked_at {
        Some(last) => now.signed_duration_since(last) < Duration::seconds(DRIFT_CACHE_TTL_SECONDS),
        None => false,
    }
}

// Unit tests live in `tests/dashboard_sync.rs` (integration test crate) so
// the lib-test crate's `#[test]` macro recursion budget is not inflated by
// this module's additions; see lib.rs `recursion_limit` note for context.
