//! Shared refresh constants used by both `ChartDocument` and `DashboardDocument`.
//!
//! Centralising here prevents the two call sites from drifting to different
//! interval lists or floor values.

use dbflux_components::SavedChartRefreshPolicy;

/// Minimum floor for any auto-refresh interval (10 seconds).
///
/// Enforced in `ChartDocument::update_refresh_timer` and any new timer-driven
/// entities. UI dropdown options must not offer values below this floor.
pub const MIN_REFRESH_FLOOR_SECS: u64 = 10;

/// Canonical ordered list of `(policy, label)` pairs shown in refresh dropdowns.
///
/// Order is significant — `index` lookups rely on stable position. Both
/// `DashboardDocument` and `ChartDocument` should import this slice rather
/// than defining their own.
pub const REFRESH_POLICY_OPTIONS: &[(SavedChartRefreshPolicy, &str)] = &[
    (SavedChartRefreshPolicy::Off, "Off"),
    (SavedChartRefreshPolicy::Interval { every_secs: 10 }, "10s"),
    (SavedChartRefreshPolicy::Interval { every_secs: 30 }, "30s"),
    (SavedChartRefreshPolicy::Interval { every_secs: 60 }, "1m"),
    (SavedChartRefreshPolicy::Interval { every_secs: 300 }, "5m"),
];
