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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_policy_options_minimum_interval_is_at_least_10s() {
        for (policy, label) in REFRESH_POLICY_OPTIONS {
            if let SavedChartRefreshPolicy::Interval { every_secs } = policy {
                assert!(
                    u64::from(*every_secs) >= MIN_REFRESH_FLOOR_SECS,
                    "Refresh option {:?} ({}s) is below the 10s floor",
                    label,
                    every_secs
                );
            }
        }
    }

    #[test]
    fn refresh_policy_options_has_no_sub_10s_interval() {
        let sub_10_count = REFRESH_POLICY_OPTIONS
            .iter()
            .filter(|(policy, _)| {
                matches!(policy, SavedChartRefreshPolicy::Interval { every_secs } if *every_secs < 10)
            })
            .count();

        assert_eq!(
            sub_10_count, 0,
            "No sub-10s refresh interval option should be offered to users"
        );
    }
}
