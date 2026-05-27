//! Dashboard sync status pill — state derivation and capability gating.
//!
//! The pill itself is rendered by the dashboard toolbar; this module owns the
//! pure logic that decides which of the four states applies and whether the
//! pill should appear at all. Keeping the derivation pure lets us unit test it
//! without spinning up a GPUI window.
//!
//! Visual states (also documented in design §E):
//! - `Unknown` — never refreshed since the tab opened. Click triggers a fetch.
//! - `Synced` — last fetch matched the stored hash.
//! - `Drifted` — last fetch produced a non-empty diff. Click opens the modal.
//! - `Detached` — dashboard is marked as imported but its upstream identity
//!   is no longer resolvable. Click is disabled; tooltip explains why.

use dbflux_core::DriverCapabilities;
use dbflux_ui_base::{DashboardSourceKind, DashboardSyncIdentity};

/// Four-way visual state for the dashboard sync pill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPillState {
    /// Initial state: identity present but no drift result yet.
    Unknown,
    /// Last fetch matched the stored hash.
    Synced,
    /// Last fetch produced a non-empty diff.
    Drifted,
    /// Imported but upstream identity cannot be resolved (account id missing).
    Detached,
}

impl SyncPillState {
    /// Human-readable label rendered inside the pill.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Unknown => "Checking",
            Self::Synced => "Synced",
            Self::Drifted => "Drifted",
            Self::Detached => "Detached",
        }
    }

    /// Tooltip text shown on hover. Includes guidance for non-default states.
    pub fn tooltip(&self) -> &'static str {
        match self {
            Self::Unknown => "Sync status not yet known. Click to refresh.",
            Self::Synced => "This dashboard matches its upstream source.",
            Self::Drifted => "Upstream changed. Click to review and apply.",
            Self::Detached => {
                "This dashboard is not linked to CloudWatch. Re-link via Import to enable sync."
            }
        }
    }

    /// Whether the pill is interactive in this state. `Detached` is informational only.
    pub fn is_clickable(&self) -> bool {
        !matches!(self, Self::Detached)
    }
}

/// Drift-check outcome consumed by `SyncPillState::from_identity_and_drift`.
///
/// Kept separate from the `DashboardDiff` value type so the pill never has to
/// know how diffs are computed — only whether the last completed check
/// reported drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DriftCheckOutcome {
    /// No completed check since the tab opened.
    #[default]
    NotChecked,
    /// Last check matched the stored hash.
    Clean,
    /// Last check produced a non-empty diff.
    Drifted,
}

impl SyncPillState {
    /// Pure state derivation. Inputs:
    /// - `identity`: the dashboard's persisted sync identity.
    /// - `drift`: outcome of the most recent drift check (live, not persisted).
    pub fn from_identity_and_drift(
        identity: &DashboardSyncIdentity,
        drift: DriftCheckOutcome,
    ) -> Self {
        if identity.is_detached() {
            return Self::Detached;
        }
        match drift {
            DriftCheckOutcome::NotChecked => Self::Unknown,
            DriftCheckOutcome::Clean => Self::Synced,
            DriftCheckOutcome::Drifted => Self::Drifted,
        }
    }
}

/// Capability + identity gate for the sync pill.
///
/// Returns `true` when the pill should be visible in the dashboard toolbar.
/// Never branches on `driver_id` (project rule: drivers expose capabilities,
/// the UI consumes them).
pub fn dashboard_sync_pill_visible(
    capabilities: DriverCapabilities,
    identity: &DashboardSyncIdentity,
) -> bool {
    capabilities.contains(DriverCapabilities::DASHBOARD_SYNC)
        && matches!(identity.source_kind, DashboardSourceKind::Cloudwatch)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn linked() -> DashboardSyncIdentity {
        DashboardSyncIdentity {
            source_kind: DashboardSourceKind::Cloudwatch,
            source_account_id: Some("acct".into()),
            source_dashboard_name: Some("d".into()),
            ..Default::default()
        }
    }

    fn detached() -> DashboardSyncIdentity {
        DashboardSyncIdentity {
            source_kind: DashboardSourceKind::Cloudwatch,
            source_account_id: None,
            source_dashboard_name: Some("d".into()),
            ..Default::default()
        }
    }

    #[test]
    fn local_dashboards_have_no_pill() {
        let local = DashboardSyncIdentity::default();
        assert!(!dashboard_sync_pill_visible(
            DriverCapabilities::DASHBOARD_SYNC,
            &local,
        ));
    }

    #[test]
    fn cloudwatch_with_capability_shows_pill() {
        assert!(dashboard_sync_pill_visible(
            DriverCapabilities::DASHBOARD_SYNC,
            &linked(),
        ));
    }

    #[test]
    fn capability_required_even_when_linked() {
        let no_cap = DriverCapabilities::DASHBOARD_IMPORT;
        assert!(!dashboard_sync_pill_visible(no_cap, &linked()));
    }

    #[test]
    fn detached_takes_precedence_over_drift_outcome() {
        let s = SyncPillState::from_identity_and_drift(&detached(), DriftCheckOutcome::Drifted);
        assert_eq!(s, SyncPillState::Detached);
        let s = SyncPillState::from_identity_and_drift(&detached(), DriftCheckOutcome::Clean);
        assert_eq!(s, SyncPillState::Detached);
    }

    #[test]
    fn linked_state_progression_unknown_clean_drifted() {
        let id = linked();
        assert_eq!(
            SyncPillState::from_identity_and_drift(&id, DriftCheckOutcome::NotChecked),
            SyncPillState::Unknown
        );
        assert_eq!(
            SyncPillState::from_identity_and_drift(&id, DriftCheckOutcome::Clean),
            SyncPillState::Synced
        );
        assert_eq!(
            SyncPillState::from_identity_and_drift(&id, DriftCheckOutcome::Drifted),
            SyncPillState::Drifted
        );
    }

    #[test]
    fn detached_is_not_clickable() {
        assert!(!SyncPillState::Detached.is_clickable());
        assert!(SyncPillState::Synced.is_clickable());
        assert!(SyncPillState::Drifted.is_clickable());
        assert!(SyncPillState::Unknown.is_clickable());
    }

    #[test]
    fn labels_and_tooltips_are_non_empty_for_every_state() {
        for s in [
            SyncPillState::Unknown,
            SyncPillState::Synced,
            SyncPillState::Drifted,
            SyncPillState::Detached,
        ] {
            assert!(!s.label().is_empty());
            assert!(!s.tooltip().is_empty());
        }
    }
}
