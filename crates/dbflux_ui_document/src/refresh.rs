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

/// Canonical ordered list of refresh policies shown in refresh dropdowns.
///
/// Order is significant — `index` lookups rely on stable position. Both
/// `DashboardDocument` and `ChartDocument` should import this slice rather
/// than defining their own. Labels are resolved separately through
/// [`refresh_policy_option_label`] so they route through the translation
/// catalog instead of living as a literal alongside the policy.
pub const REFRESH_POLICY_OPTIONS: &[SavedChartRefreshPolicy] = &[
    SavedChartRefreshPolicy::Off,
    SavedChartRefreshPolicy::Interval { every_secs: 10 },
    SavedChartRefreshPolicy::Interval { every_secs: 30 },
    SavedChartRefreshPolicy::Interval { every_secs: 60 },
    SavedChartRefreshPolicy::Interval { every_secs: 300 },
];

/// Translated label for a refresh policy dropdown option.
///
/// `Off` and `OnOpen` route through the catalog; a named interval renders
/// its seconds directly (`"{every_secs}s"`), which is a unit suffix, not
/// translated prose, so it stays outside the catalog — identical in every
/// locale DBFlux ships.
pub(crate) fn refresh_policy_option_label(policy: SavedChartRefreshPolicy) -> String {
    match policy {
        SavedChartRefreshPolicy::Off => dbflux_i18n::t!("document.shared.refresh.off"),
        SavedChartRefreshPolicy::Interval { every_secs } => format!("{every_secs}s"),
        SavedChartRefreshPolicy::OnOpen => dbflux_i18n::t!("document.shared.refresh.on_open"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_policy_options_minimum_interval_is_at_least_10s() {
        for policy in REFRESH_POLICY_OPTIONS {
            if let SavedChartRefreshPolicy::Interval { every_secs } = policy {
                assert!(
                    u64::from(*every_secs) >= MIN_REFRESH_FLOOR_SECS,
                    "Refresh option {policy:?} is below the 10s floor",
                );
            }
        }
    }

    #[test]
    fn refresh_policy_options_has_no_sub_10s_interval() {
        let sub_10_count = REFRESH_POLICY_OPTIONS
            .iter()
            .filter(|policy| {
                matches!(policy, SavedChartRefreshPolicy::Interval { every_secs } if *every_secs < 10)
            })
            .count();

        assert_eq!(
            sub_10_count, 0,
            "No sub-10s refresh interval option should be offered to users"
        );
    }

    #[test]
    fn refresh_policy_option_label_off_routes_through_catalog() {
        let value = refresh_policy_option_label(SavedChartRefreshPolicy::Off);

        assert_eq!(value, dbflux_i18n::t!("document.shared.refresh.off"));
        assert_ne!(value, "document.shared.refresh.off");
    }

    #[test]
    fn refresh_policy_option_label_interval_renders_seconds_as_unit_suffix() {
        let value =
            refresh_policy_option_label(SavedChartRefreshPolicy::Interval { every_secs: 30 });

        assert_eq!(value, "30s");
    }

    #[test]
    fn refresh_policy_option_label_on_open_routes_through_catalog() {
        let value = refresh_policy_option_label(SavedChartRefreshPolicy::OnOpen);

        assert_eq!(value, dbflux_i18n::t!("document.shared.refresh.on_open"));
        assert_ne!(value, "document.shared.refresh.on_open");
    }
}
