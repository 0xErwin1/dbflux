//! Dashboard sync seam for DBFlux.
//!
//! Defines the `DashboardSource` trait that drivers implement to fetch and
//! reconcile dashboards previously imported from upstream. Drivers must
//! advertise `DriverCapabilities::DASHBOARD_SYNC`. All non-syncing drivers
//! inherit the default `None` return from `Connection::dashboard_source()`.
//!
//! Also exposes the pure `reconcile` algorithm and per-field merge helper
//! used by the apply flow; both are decoupled from any IO so they can be
//! unit-tested with synthetic snapshots.

use async_trait::async_trait;

use crate::DbError;

/// A dashboard fetched from upstream (e.g. CloudWatch `GetDashboard`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDashboard {
    /// Dashboard name as reported by upstream.
    pub name: String,
    /// Account that owns the dashboard.
    pub account_id: String,
    /// Home region for the dashboard.
    pub home_region: String,
    /// Raw upstream JSON body, exactly as returned.
    pub body_json: String,
    /// Canonicalized SHA256 of `body_json` with `"v1:"` prefix.
    pub content_hash: String,
    /// `lastModified` from `ListDashboards`, ISO8601 when known.
    pub last_modified: Option<String>,
}

/// A reference to an upstream dashboard, returned from listing calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DashboardRef {
    /// Dashboard name as reported by upstream.
    pub name: String,
    /// `lastModified` from `ListDashboards`, ISO8601 when known.
    pub last_modified: Option<String>,
}

/// Trait implemented by drivers that can fetch dashboards for sync/refresh.
///
/// Drivers register an instance via `Connection::dashboard_source()` and
/// MUST advertise `DriverCapabilities::DASHBOARD_SYNC` in their metadata.
#[async_trait]
pub trait DashboardSource: Send + Sync {
    /// Fetches the dashboard named `name` and returns the populated
    /// `RemoteDashboard` with its canonicalized `content_hash` already set.
    async fn fetch_dashboard(&self, name: &str) -> Result<RemoteDashboard, DbError>;

    /// Lists dashboards available in the upstream account / region.
    async fn list_dashboards(&self) -> Result<Vec<DashboardRef>, DbError>;

    /// Returns the cached account id when known. Construction may succeed
    /// even when account resolution fails (per spec R7.1); in that case
    /// `None` is returned and dependent dashboards are treated as detached.
    fn account_id(&self) -> Option<&str>;

    /// Home region this source is bound to.
    fn home_region(&self) -> &str;
}

// ---------------------------------------------------------------------------
// Reconcile algorithm
// ---------------------------------------------------------------------------

/// Snapshot of a local dashboard panel used by the reconcile algorithm.
///
/// Only the fields needed for matching and merging are included; the apply
/// path operates on richer panel rows downstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPanelSnapshot {
    /// Stable panel identifier within the local dashboard (typically the
    /// row's primary key string).
    pub panel_id: String,
    /// `viz_dashboard_panels.source_widget_index`. `None` for user-added panels.
    pub source_widget_index: Option<usize>,
    /// `viz_dashboard_panels.source_widget_hash`. `None` for user-added panels.
    pub source_widget_hash: Option<String>,
    /// `viz_dashboard_panels.panel_kind` (`"chart"` or `"divider"`).
    pub panel_kind: String,
    /// Set when the user has overridden the title of this panel.
    pub has_title_override: bool,
    /// Structural identity for fallback compatibility checks.
    /// For chart panels: first series `(namespace, metric_name)`. For divider
    /// panels: `("divider", "")`.
    pub structural_key: Option<(String, String)>,
}

/// Snapshot of an upstream widget used by the reconcile algorithm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamWidgetSnapshot {
    /// Position in the upstream widget array (zero-based).
    pub index: usize,
    /// Canonical widget hash (`"v1:..."`).
    pub widget_hash: String,
    /// `"metric"`, `"text"`, etc.
    pub widget_kind: String,
    /// Structural identity used as a fallback when `widget_hash` doesn't
    /// match an existing panel: typically first series `(namespace, metric_name)`.
    /// For text widgets: `("divider", "")`.
    pub structural_key: Option<(String, String)>,
}

/// Classification of an upstream widget against a local panel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifiedEntry {
    pub local_panel_id: String,
    pub upstream_index: usize,
}

/// Classification of a moved panel: identical hash, different array position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MovedEntry {
    pub local_panel_id: String,
    pub upstream_index: usize,
}

/// Diff result produced by `reconcile`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DashboardDiff {
    /// Upstream widgets with no matching local panel.
    pub added: Vec<UpstreamWidgetSnapshot>,
    /// Local panels whose upstream widget has disappeared.
    pub removed: Vec<String>,
    /// Local panels where structural fields must be overwritten.
    pub modified: Vec<ModifiedEntry>,
    /// Local panels matched by hash at a different upstream index.
    pub moved: Vec<MovedEntry>,
    /// Local panels with `source_widget_index = None` — preserved unchanged.
    pub local_only_preserved: Vec<String>,
}

impl DashboardDiff {
    /// Returns `true` when no structural change is needed.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.modified.is_empty()
            && self.moved.is_empty()
    }
}

/// Pure reconcile: classifies each upstream widget against each local panel.
///
/// Algorithm (per design §D):
/// 1. Local panels with `source_widget_index = None` are recorded as
///    `local_only_preserved` and never touched.
/// 2. For each linked local panel, the upstream widget array is searched for
///    a matching `widget_hash`. A match at the same index = `Unchanged` (not
///    surfaced in the diff). A match at a different index = `moved`.
/// 3. When no hash match is found, the upstream widget at
///    `source_widget_index` is tested for structural compatibility (same
///    kind and same `structural_key`). A compatible widget => `modified`.
/// 4. Anything else => `removed`.
/// 5. Upstream widgets with no consuming local panel become `added`.
pub fn reconcile(
    local: &[LocalPanelSnapshot],
    upstream: &[UpstreamWidgetSnapshot],
) -> DashboardDiff {
    let mut diff = DashboardDiff::default();
    let mut consumed_upstream: Vec<bool> = vec![false; upstream.len()];

    for panel in local {
        if panel.source_widget_index.is_none() {
            diff.local_only_preserved.push(panel.panel_id.clone());
            continue;
        }

        let stored_idx = panel.source_widget_index.expect("checked above");
        let stored_hash = panel.source_widget_hash.as_deref();

        // 1. Exact hash match anywhere in upstream.
        if let Some(hash) = stored_hash
            && let Some((found_idx, _w)) = upstream
                .iter()
                .enumerate()
                .find(|(i, w)| !consumed_upstream[*i] && w.widget_hash == hash)
        {
            consumed_upstream[found_idx] = true;
            if found_idx != stored_idx {
                diff.moved.push(MovedEntry {
                    local_panel_id: panel.panel_id.clone(),
                    upstream_index: found_idx,
                });
            }
            continue;
        }

        // 2. Structural fallback at stored index.
        if let Some(candidate) = upstream.get(stored_idx)
            && !consumed_upstream[stored_idx]
            && structurally_compatible(panel, candidate)
        {
            consumed_upstream[stored_idx] = true;
            diff.modified.push(ModifiedEntry {
                local_panel_id: panel.panel_id.clone(),
                upstream_index: stored_idx,
            });
            continue;
        }

        // 3. Removed upstream.
        diff.removed.push(panel.panel_id.clone());
    }

    for (idx, widget) in upstream.iter().enumerate() {
        if !consumed_upstream[idx] {
            diff.added.push(widget.clone());
        }
    }

    diff
}

fn structurally_compatible(panel: &LocalPanelSnapshot, widget: &UpstreamWidgetSnapshot) -> bool {
    let panel_is_divider = panel.panel_kind == "divider";
    let widget_is_text = widget.widget_kind == "text";

    if panel_is_divider != widget_is_text {
        return false;
    }

    match (&panel.structural_key, &widget.structural_key) {
        (Some(a), Some(b)) => a == b,
        (None, None) => true,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Per-field merge rules
// ---------------------------------------------------------------------------

/// Inputs for the per-field merge decision applied to a `Modified` panel.
///
/// All structural fields come from upstream; presentation fields are
/// preserved from local. The single non-obvious case is divider markdown:
/// upstream wins ONLY when the local panel's recorded `source_widget_hash`
/// matches the previous upstream hash exactly (i.e. the user has not
/// edited the divider locally). The caller computes this match using the
/// returned `MergePlan::apply_divider_markdown` flag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeInput<'a> {
    pub panel_kind: &'a str,
    /// Local divider markdown when the panel is a divider.
    pub local_divider_markdown: Option<&'a str>,
    /// Recorded `source_widget_hash` (from the previous import) on the local
    /// panel. `None` for user-added panels (which never reach merge).
    pub previous_source_widget_hash: Option<&'a str>,
    /// Upstream widget hash from this fetch.
    pub upstream_widget_hash: &'a str,
    /// Whether the user has set a `title_override` on this panel.
    pub has_title_override: bool,
}

/// Decision produced by [`plan_merge`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MergePlan {
    /// Apply upstream's divider markdown. Only meaningful when the panel is
    /// a divider and the previous hash matched (i.e. the local divider has
    /// NOT been edited since import).
    pub apply_divider_markdown: bool,
    /// Apply structural fields (layout, metric tuple, view kind, label) from
    /// upstream. Always `true` for the modified path.
    pub apply_structural: bool,
    /// Preserve the local `title_override`. Always `true` when present.
    pub preserve_title_override: bool,
}

/// Computes the merge plan for a single panel classified as `Modified`.
///
/// Pure function: no IO. The caller feeds the result into its persistence
/// layer to know which columns to update vs. preserve.
pub fn plan_merge(input: &MergeInput<'_>) -> MergePlan {
    let is_divider = input.panel_kind == "divider";

    let divider_unedited = input
        .previous_source_widget_hash
        .is_some_and(|stored| stored == input.upstream_widget_hash);

    MergePlan {
        apply_divider_markdown: is_divider && divider_unedited,
        apply_structural: true,
        preserve_title_override: input.has_title_override,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn linked_panel(id: &str, idx: usize, hash: &str) -> LocalPanelSnapshot {
        LocalPanelSnapshot {
            panel_id: id.into(),
            source_widget_index: Some(idx),
            source_widget_hash: Some(hash.into()),
            panel_kind: "chart".into(),
            has_title_override: false,
            structural_key: Some(("AWS/EC2".into(), "CPUUtilization".into())),
        }
    }

    fn upstream(idx: usize, hash: &str) -> UpstreamWidgetSnapshot {
        UpstreamWidgetSnapshot {
            index: idx,
            widget_hash: hash.into(),
            widget_kind: "metric".into(),
            structural_key: Some(("AWS/EC2".into(), "CPUUtilization".into())),
        }
    }

    #[test]
    fn all_unchanged_produces_empty_diff() {
        let local = vec![
            linked_panel("p0", 0, "v1:aaa"),
            linked_panel("p1", 1, "v1:bbb"),
        ];
        let upstream = vec![upstream(0, "v1:aaa"), upstream(1, "v1:bbb")];

        let diff = reconcile(&local, &upstream);
        assert!(diff.is_empty(), "expected empty diff, got {diff:?}");
    }

    #[test]
    fn modified_by_hash_mismatch_same_index() {
        let local = vec![linked_panel("p0", 0, "v1:old")];
        let upstream = vec![upstream(0, "v1:new")];

        let diff = reconcile(&local, &upstream);
        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.modified[0].local_panel_id, "p0");
        assert_eq!(diff.modified[0].upstream_index, 0);
        assert!(diff.removed.is_empty());
        assert!(diff.added.is_empty());
    }

    #[test]
    fn removed_widget() {
        let local = vec![linked_panel("p0", 0, "v1:gone")];
        let upstream: Vec<UpstreamWidgetSnapshot> = vec![];

        let diff = reconcile(&local, &upstream);
        assert_eq!(diff.removed, vec!["p0".to_string()]);
        assert!(diff.added.is_empty());
        assert!(diff.modified.is_empty());
    }

    #[test]
    fn added_widget_with_no_matching_local() {
        let local: Vec<LocalPanelSnapshot> = vec![];
        let upstream = vec![upstream(0, "v1:new")];

        let diff = reconcile(&local, &upstream);
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].widget_hash, "v1:new");
    }

    #[test]
    fn reorder_with_identical_content_is_moved_not_modified() {
        let local = vec![
            linked_panel("p0", 0, "v1:aaa"),
            linked_panel("p1", 1, "v1:bbb"),
        ];
        // Upstream swapped positions.
        let upstream = vec![upstream(0, "v1:bbb"), upstream(1, "v1:aaa")];

        let diff = reconcile(&local, &upstream);
        assert_eq!(diff.moved.len(), 2);
        assert!(diff.modified.is_empty());
        assert!(diff.removed.is_empty());
        assert!(diff.added.is_empty());

        // Each panel ended up at the OTHER index.
        let p0 = diff
            .moved
            .iter()
            .find(|m| m.local_panel_id == "p0")
            .expect("p0 moved");
        let p1 = diff
            .moved
            .iter()
            .find(|m| m.local_panel_id == "p1")
            .expect("p1 moved");
        assert_eq!(p0.upstream_index, 1);
        assert_eq!(p1.upstream_index, 0);
    }

    #[test]
    fn local_only_panel_never_in_added_removed_modified() {
        let local_only = LocalPanelSnapshot {
            panel_id: "u0".into(),
            source_widget_index: None,
            source_widget_hash: None,
            panel_kind: "chart".into(),
            has_title_override: false,
            structural_key: None,
        };
        let linked = linked_panel("p0", 0, "v1:aaa");
        let local = vec![local_only, linked];
        let upstream = vec![upstream(0, "v1:aaa")];

        let diff = reconcile(&local, &upstream);
        assert_eq!(diff.local_only_preserved, vec!["u0".to_string()]);
        assert!(diff.added.is_empty());
        assert!(diff.removed.is_empty());
        assert!(diff.modified.is_empty());
        assert!(diff.moved.is_empty());
    }

    #[test]
    fn title_override_survives_modified_merge() {
        let plan = plan_merge(&MergeInput {
            panel_kind: "chart",
            local_divider_markdown: None,
            previous_source_widget_hash: Some("v1:old"),
            upstream_widget_hash: "v1:new",
            has_title_override: true,
        });
        assert!(plan.preserve_title_override);
        assert!(plan.apply_structural);
        assert!(!plan.apply_divider_markdown);
    }

    #[test]
    fn divider_markdown_applied_when_hash_matches_previous() {
        // Local divider whose previous source hash equals the upstream hash
        // means the user has NOT edited the divider locally; upstream wins.
        let plan = plan_merge(&MergeInput {
            panel_kind: "divider",
            local_divider_markdown: Some("# old"),
            previous_source_widget_hash: Some("v1:same"),
            upstream_widget_hash: "v1:same",
            has_title_override: false,
        });
        assert!(plan.apply_divider_markdown);
    }

    #[test]
    fn divider_markdown_locked_when_hash_differs() {
        // User has edited the divider locally — upstream divider must NOT
        // overwrite the local markdown.
        let plan = plan_merge(&MergeInput {
            panel_kind: "divider",
            local_divider_markdown: Some("# user edited"),
            previous_source_widget_hash: Some("v1:old"),
            upstream_widget_hash: "v1:new",
            has_title_override: false,
        });
        assert!(!plan.apply_divider_markdown);
    }

    #[test]
    fn structural_fallback_when_hash_unrecognised() {
        // Hash mismatch but same kind + structural key at stored index =>
        // classified as Modified.
        let local = vec![linked_panel("p0", 0, "v1:was")];
        let upstream = vec![upstream(0, "v1:now")];

        let diff = reconcile(&local, &upstream);
        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.modified[0].local_panel_id, "p0");
    }

    /// Round-trip: parse synthetic CloudWatch JSON, compute hashes, mutate
    /// the upstream body (one widget edited, one new widget appended),
    /// re-hash, and run `reconcile`. The diff must classify the changes as
    /// `modified` + `added` while keeping the structurally-identical first
    /// widget unchanged.
    ///
    /// This exercises the full identity pipeline (content hash → widget hash
    /// → reconcile) that the import path now relies on.
    #[test]
    fn round_trip_modify_and_add_reconciles_correctly() {
        use crate::connection::dashboard_hash::{content_hash, widget_hash};
        use serde_json::json;

        // Synthetic CW dashboard. Two metric widgets at positions 0, 1.
        let upstream_v1 = json!({
            "widgets": [
                {
                    "type": "metric",
                    "x": 0, "y": 0, "width": 12, "height": 6,
                    "properties": {
                        "metrics": [["AWS/EC2", "CPUUtilization"]],
                        "view": "timeSeries",
                        "stat": "Average",
                        "period": 60,
                    },
                },
                {
                    "type": "metric",
                    "x": 12, "y": 0, "width": 12, "height": 6,
                    "properties": {
                        "metrics": [["AWS/RDS", "DatabaseConnections"]],
                        "view": "timeSeries",
                        "stat": "Sum",
                        "period": 300,
                    },
                },
            ],
        });

        let body_v1 = serde_json::to_string(&upstream_v1).unwrap();
        let hash_v1 = content_hash(&body_v1).expect("hash v1");
        assert!(
            hash_v1.starts_with("v1:"),
            "content hash must carry v1 prefix"
        );

        let widgets_v1 = upstream_v1["widgets"].as_array().unwrap();
        let w0_hash = widget_hash(&widgets_v1[0]);
        let w1_hash = widget_hash(&widgets_v1[1]);

        // Simulate an import: the dashboard has two linked panels carrying the
        // hashes recorded at import time.
        let local = vec![
            LocalPanelSnapshot {
                panel_id: "p0".into(),
                source_widget_index: Some(0),
                source_widget_hash: Some(w0_hash.clone()),
                panel_kind: "chart".into(),
                has_title_override: true, // user-customised title, must be preserved
                structural_key: Some(("AWS/EC2".into(), "CPUUtilization".into())),
            },
            LocalPanelSnapshot {
                panel_id: "p1".into(),
                source_widget_index: Some(1),
                source_widget_hash: Some(w1_hash.clone()),
                panel_kind: "chart".into(),
                has_title_override: false,
                structural_key: Some(("AWS/RDS".into(), "DatabaseConnections".into())),
            },
        ];

        // Upstream changes: widget at index 1 gets a new statistic (Modified)
        // and a third widget is appended (Added). Widget at index 0 is
        // untouched.
        let upstream_v2 = json!({
            "widgets": [
                widgets_v1[0].clone(),
                {
                    "type": "metric",
                    "x": 12, "y": 0, "width": 12, "height": 6,
                    "properties": {
                        "metrics": [["AWS/RDS", "DatabaseConnections"]],
                        "view": "timeSeries",
                        "stat": "Maximum",  // changed from Sum
                        "period": 300,
                    },
                },
                {
                    "type": "metric",
                    "x": 0, "y": 6, "width": 24, "height": 6,
                    "properties": {
                        "metrics": [["AWS/Lambda", "Errors"]],
                        "view": "timeSeries",
                        "stat": "Sum",
                        "period": 60,
                    },
                },
            ],
        });

        let body_v2 = serde_json::to_string(&upstream_v2).unwrap();
        let hash_v2 = content_hash(&body_v2).expect("hash v2");
        assert_ne!(
            hash_v1, hash_v2,
            "modified+added widgets must change the dashboard-level content hash"
        );

        let widgets_v2 = upstream_v2["widgets"].as_array().unwrap();
        let upstream_snapshots: Vec<UpstreamWidgetSnapshot> = widgets_v2
            .iter()
            .enumerate()
            .map(|(i, w)| UpstreamWidgetSnapshot {
                index: i,
                widget_hash: widget_hash(w),
                widget_kind: "metric".into(),
                structural_key: Some(match i {
                    0 => ("AWS/EC2".into(), "CPUUtilization".into()),
                    1 => ("AWS/RDS".into(), "DatabaseConnections".into()),
                    _ => ("AWS/Lambda".into(), "Errors".into()),
                }),
            })
            .collect();

        let diff = reconcile(&local, &upstream_snapshots);

        // Widget 0 is byte-identical: must not appear in any diff section.
        assert!(
            !diff.modified.iter().any(|m| m.local_panel_id == "p0"),
            "p0 was unchanged: {diff:?}"
        );
        assert!(
            !diff.moved.iter().any(|m| m.local_panel_id == "p0"),
            "p0 stayed at index 0: {diff:?}"
        );

        // Widget 1 was modified at the same index.
        let p1_modified = diff
            .modified
            .iter()
            .find(|m| m.local_panel_id == "p1")
            .expect("p1 modified");
        assert_eq!(p1_modified.upstream_index, 1);

        // Widget 2 is new.
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].index, 2);

        // No removals: every local panel found a match.
        assert!(diff.removed.is_empty(), "no removals: {diff:?}");

        // p0 keeps its title override across the merge.
        let p0_plan = plan_merge(&MergeInput {
            panel_kind: "chart",
            local_divider_markdown: None,
            previous_source_widget_hash: Some(&w0_hash),
            upstream_widget_hash: &w0_hash,
            has_title_override: true,
        });
        assert!(
            p0_plan.preserve_title_override,
            "title override must survive a no-op merge"
        );
    }

    #[test]
    fn structural_mismatch_at_same_index_is_removed_and_added() {
        // Different metric => not structurally compatible => removed/added.
        let local = vec![LocalPanelSnapshot {
            panel_id: "p0".into(),
            source_widget_index: Some(0),
            source_widget_hash: Some("v1:was".into()),
            panel_kind: "chart".into(),
            has_title_override: false,
            structural_key: Some(("AWS/EC2".into(), "CPUUtilization".into())),
        }];
        let upstream = vec![UpstreamWidgetSnapshot {
            index: 0,
            widget_hash: "v1:totally-different".into(),
            widget_kind: "metric".into(),
            structural_key: Some(("AWS/RDS".into(), "DatabaseConnections".into())),
        }];

        let diff = reconcile(&local, &upstream);
        assert_eq!(diff.removed, vec!["p0".to_string()]);
        assert_eq!(diff.added.len(), 1);
    }
}
