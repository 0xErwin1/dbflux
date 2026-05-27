//! Integration tests for the dashboard sync flow (drift detection, apply
//! algorithm, diff modal state, sync pill render).
//!
//! These live in the integration-test crate rather than `#[cfg(test)]`
//! modules inside the library so the lib-test crate's macro-expansion
//! recursion budget is not inflated by the dashboard subsystem (which
//! already crosses the default 128-level limit). Each `#[test]` here
//! lands in a separate compilation unit, which keeps macro expansion cheap.

use std::collections::HashMap;

use chrono::{Duration, Utc};
use dbflux_core::{DashboardDiff, ModifiedEntry, MovedEntry, UpstreamWidgetSnapshot};
use dbflux_ui_base::{
    DashboardPanel, DashboardPanelKind, DashboardSourceKind, DashboardSyncIdentity,
};
use dbflux_ui_document::dashboard::apply::{
    ApplyInputs, StructuralUpdate, apply_diff, panel_identity,
};
use dbflux_ui_document::dashboard::diff_modal::{DashboardDiffOutcome, DashboardDiffRequest};
use dbflux_ui_document::dashboard::drift::{
    DRIFT_CACHE_TTL_SECONDS, classify_drift, drift_cache_is_fresh,
};
use dbflux_ui_document::dashboard::sync_pill::{
    DriftCheckOutcome, SyncPillState, dashboard_sync_pill_visible,
};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Drift module (D2 pure logic)
// ---------------------------------------------------------------------------

#[test]
fn classify_drift_returns_clean_on_exact_hash_match() {
    assert_eq!(
        classify_drift(Some("v1:abc"), "v1:abc"),
        DriftCheckOutcome::Clean,
    );
}

#[test]
fn classify_drift_returns_drifted_when_hashes_differ() {
    assert_eq!(
        classify_drift(Some("v1:old"), "v1:new"),
        DriftCheckOutcome::Drifted,
    );
}

#[test]
fn classify_drift_returns_drifted_when_no_stored_hash() {
    assert_eq!(classify_drift(None, "v1:new"), DriftCheckOutcome::Drifted);
}

#[test]
fn drift_cache_is_stale_when_never_checked() {
    let now = Utc::now();
    assert!(!drift_cache_is_fresh(None, now));
}

#[test]
fn drift_cache_is_fresh_within_ttl() {
    let now = Utc::now();
    let last = now - Duration::seconds(30);
    assert!(drift_cache_is_fresh(Some(last), now));
}

#[test]
fn drift_cache_is_stale_past_ttl() {
    let now = Utc::now();
    let last = now - Duration::seconds(DRIFT_CACHE_TTL_SECONDS + 1);
    assert!(!drift_cache_is_fresh(Some(last), now));
}

#[test]
fn drift_cache_boundary_is_exclusive() {
    let now = Utc::now();
    let last = now - Duration::seconds(DRIFT_CACHE_TTL_SECONDS);
    assert!(!drift_cache_is_fresh(Some(last), now));
}

// ---------------------------------------------------------------------------
// Apply module (D4 pure logic)
// ---------------------------------------------------------------------------

fn make_panel(dashboard_id: Uuid, index: u32, title: Option<&str>) -> DashboardPanel {
    DashboardPanel {
        dashboard_id,
        panel_index: index,
        kind: DashboardPanelKind::Chart {
            saved_chart_id: Uuid::new_v4(),
        },
        title_override: title.map(String::from),
        grid_row: index,
        grid_column: 0,
        grid_width: 12,
        grid_height: 2,
        source_widget_index: Some(index),
        source_widget_hash: Some(format!("v1:old-{index}")),
    }
}

#[test]
fn modified_panel_gets_structural_update_but_preserves_title() {
    let dashboard_id = Uuid::new_v4();
    let mut panel = make_panel(dashboard_id, 0, Some("Prod Errors"));
    panel.grid_row = 0;
    panel.grid_column = 0;

    let id = panel_identity(&panel);
    let mut structural_updates = HashMap::new();
    structural_updates.insert(
        id.clone(),
        StructuralUpdate {
            source_widget_index: 0,
            source_widget_hash: "v1:new".into(),
            grid_row: 5,
            grid_column: 3,
            grid_width: 6,
            grid_height: 4,
        },
    );

    let diff = DashboardDiff {
        modified: vec![ModifiedEntry {
            local_panel_id: id,
            upstream_index: 0,
        }],
        ..Default::default()
    };

    let outcome = apply_diff(ApplyInputs {
        panels: vec![panel],
        diff: &diff,
        structural_updates,
        added_panels: vec![],
    });

    assert_eq!(outcome.panels.len(), 1);
    let merged = &outcome.panels[0];
    assert_eq!(merged.title_override.as_deref(), Some("Prod Errors"));
    assert_eq!(merged.source_widget_hash.as_deref(), Some("v1:new"));
    assert_eq!(merged.grid_row, 5);
    assert_eq!(merged.grid_column, 3);
    assert_eq!(merged.grid_width, 6);
}

#[test]
fn removed_panel_is_kept_in_place_with_badge() {
    let dashboard_id = Uuid::new_v4();
    let panel = make_panel(dashboard_id, 0, None);
    let id = panel_identity(&panel);

    let diff = DashboardDiff {
        removed: vec![id.clone()],
        ..Default::default()
    };

    let outcome = apply_diff(ApplyInputs {
        panels: vec![panel],
        diff: &diff,
        structural_updates: HashMap::new(),
        added_panels: vec![],
    });

    assert_eq!(outcome.panels.len(), 1, "removed panel must be kept");
    assert!(outcome.removed_upstream_panel_ids.contains(&id));
}

#[test]
fn added_panels_are_appended_after_existing() {
    let dashboard_id = Uuid::new_v4();
    let existing = make_panel(dashboard_id, 0, None);
    let added = make_panel(dashboard_id, 99, None);

    let diff = DashboardDiff::default();

    let outcome = apply_diff(ApplyInputs {
        panels: vec![existing],
        diff: &diff,
        structural_updates: HashMap::new(),
        added_panels: vec![added.clone()],
    });

    assert_eq!(outcome.panels.len(), 2);
    assert_eq!(outcome.panels[1].panel_index, 99);
}

#[test]
fn moved_panel_updates_structural_only() {
    let dashboard_id = Uuid::new_v4();
    let mut panel = make_panel(dashboard_id, 0, Some("KeepMe"));
    panel.grid_row = 0;

    let id = panel_identity(&panel);
    let mut structural_updates = HashMap::new();
    structural_updates.insert(
        id.clone(),
        StructuralUpdate {
            source_widget_index: 4,
            source_widget_hash: panel.source_widget_hash.clone().unwrap(),
            grid_row: 10,
            grid_column: 0,
            grid_width: 12,
            grid_height: 2,
        },
    );

    let diff = DashboardDiff {
        moved: vec![MovedEntry {
            local_panel_id: id,
            upstream_index: 4,
        }],
        ..Default::default()
    };

    let outcome = apply_diff(ApplyInputs {
        panels: vec![panel],
        diff: &diff,
        structural_updates,
        added_panels: vec![],
    });

    assert_eq!(outcome.panels[0].title_override.as_deref(), Some("KeepMe"));
    assert_eq!(outcome.panels[0].grid_row, 10);
    assert_eq!(outcome.panels[0].source_widget_index, Some(4));
}

#[test]
fn empty_diff_leaves_panels_untouched() {
    let dashboard_id = Uuid::new_v4();
    let panel = make_panel(dashboard_id, 0, Some("Same"));
    let before = panel.clone();

    let outcome = apply_diff(ApplyInputs {
        panels: vec![panel],
        diff: &DashboardDiff::default(),
        structural_updates: HashMap::new(),
        added_panels: vec![],
    });

    assert_eq!(outcome.panels.len(), 1);
    assert_eq!(outcome.panels[0].title_override, before.title_override);
    assert_eq!(outcome.panels[0].grid_row, before.grid_row);
    assert_eq!(
        outcome.panels[0].source_widget_hash,
        before.source_widget_hash,
    );
    assert!(outcome.removed_upstream_panel_ids.is_empty());
}

// ---------------------------------------------------------------------------
// Diff modal (D3 / E2)
// ---------------------------------------------------------------------------

fn synthetic_diff() -> DashboardDiff {
    DashboardDiff {
        added: vec![UpstreamWidgetSnapshot {
            index: 4,
            widget_hash: "v1:add".into(),
            widget_kind: "metric".into(),
            structural_key: Some(("AWS/EC2".into(), "CPUUtilization".into())),
        }],
        removed: vec!["panel-removed".into()],
        modified: vec![ModifiedEntry {
            local_panel_id: "panel-mod".into(),
            upstream_index: 0,
        }],
        moved: vec![MovedEntry {
            local_panel_id: "panel-moved".into(),
            upstream_index: 2,
        }],
        local_only_preserved: vec!["panel-user".into()],
    }
}

#[test]
fn diff_request_carries_every_category() {
    let request = DashboardDiffRequest {
        dashboard_name: "prod-overview".into(),
        diff: synthetic_diff(),
    };

    assert_eq!(request.dashboard_name, "prod-overview");
    assert_eq!(request.diff.added.len(), 1);
    assert_eq!(request.diff.removed.len(), 1);
    assert_eq!(request.diff.modified.len(), 1);
    assert_eq!(request.diff.moved.len(), 1);
    assert_eq!(request.diff.local_only_preserved.len(), 1);
}

#[test]
fn diff_outcome_is_either_apply_all_or_cancelled() {
    // Compile-time + runtime check that the outcome enum is what the apply
    // flow can react to. ApplyAll triggers the atomic update; Cancelled is a
    // no-op.
    let apply = DashboardDiffOutcome::ApplyAll;
    let cancel = DashboardDiffOutcome::Cancelled;
    assert_ne!(apply, cancel);
}

// E2 — exercise the request payload covering one Added + one Removed + one
// Modified, asserting each row has the expected classification reachable for
// rendering.
#[test]
fn diff_request_added_removed_modified_each_reachable() {
    let diff = DashboardDiff {
        added: vec![UpstreamWidgetSnapshot {
            index: 0,
            widget_hash: "v1:add".into(),
            widget_kind: "metric".into(),
            structural_key: None,
        }],
        removed: vec!["p-removed".into()],
        modified: vec![ModifiedEntry {
            local_panel_id: "p-mod".into(),
            upstream_index: 0,
        }],
        moved: vec![],
        local_only_preserved: vec![],
    };

    let request = DashboardDiffRequest {
        dashboard_name: "test".into(),
        diff,
    };

    assert_eq!(request.diff.added[0].widget_kind, "metric");
    assert_eq!(request.diff.removed[0], "p-removed");
    assert_eq!(request.diff.modified[0].local_panel_id, "p-mod");
}

// ---------------------------------------------------------------------------
// Sync pill (E1) — non-render assertions; render compiles via the lib build
// ---------------------------------------------------------------------------

fn linked_identity() -> DashboardSyncIdentity {
    DashboardSyncIdentity {
        source_kind: DashboardSourceKind::Cloudwatch,
        source_account_id: Some("acct".into()),
        source_dashboard_name: Some("dash".into()),
        ..Default::default()
    }
}

fn detached_identity() -> DashboardSyncIdentity {
    DashboardSyncIdentity {
        source_kind: DashboardSourceKind::Cloudwatch,
        source_account_id: None,
        source_dashboard_name: Some("dash".into()),
        ..Default::default()
    }
}

#[test]
fn sync_pill_unknown_state_renders_label_and_tooltip() {
    let state =
        SyncPillState::from_identity_and_drift(&linked_identity(), DriftCheckOutcome::NotChecked);
    assert_eq!(state, SyncPillState::Unknown);
    assert!(!state.label().is_empty());
    assert!(!state.tooltip().is_empty());
    assert!(state.is_clickable());
}

#[test]
fn sync_pill_synced_state_renders_label_and_tooltip() {
    let state =
        SyncPillState::from_identity_and_drift(&linked_identity(), DriftCheckOutcome::Clean);
    assert_eq!(state, SyncPillState::Synced);
    assert!(!state.label().is_empty());
    assert!(!state.tooltip().is_empty());
    assert!(state.is_clickable());
}

#[test]
fn sync_pill_drifted_state_renders_label_and_tooltip() {
    let state =
        SyncPillState::from_identity_and_drift(&linked_identity(), DriftCheckOutcome::Drifted);
    assert_eq!(state, SyncPillState::Drifted);
    assert!(!state.label().is_empty());
    assert!(!state.tooltip().is_empty());
    assert!(state.is_clickable());
}

#[test]
fn sync_pill_detached_state_is_non_clickable() {
    let state =
        SyncPillState::from_identity_and_drift(&detached_identity(), DriftCheckOutcome::NotChecked);
    assert_eq!(state, SyncPillState::Detached);
    assert!(!state.is_clickable());
    assert!(!state.tooltip().is_empty());
}

#[test]
fn sync_pill_visibility_requires_capability_and_kind() {
    use dbflux_core::DriverCapabilities;
    assert!(dashboard_sync_pill_visible(
        DriverCapabilities::DASHBOARD_SYNC,
        &linked_identity(),
    ));
    assert!(!dashboard_sync_pill_visible(
        DriverCapabilities::empty(),
        &linked_identity(),
    ));
    assert!(!dashboard_sync_pill_visible(
        DriverCapabilities::DASHBOARD_SYNC,
        &DashboardSyncIdentity::default(),
    ));
}
