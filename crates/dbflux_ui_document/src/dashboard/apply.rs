//! Pure apply layer for the dashboard refresh flow.
//!
//! Given the current panel list and a `DashboardDiff` produced by
//! `dbflux_core::reconcile`, produces:
//!
//! - the updated `DashboardPanel` set ready to be passed to
//!   `DashboardManager::replace_panels`, and
//! - the set of panel ids that should render a transient
//!   "removed upstream" badge until the user resolves them.
//!
//! Structural fields are taken from the upstream snapshot; presentation
//! fields (`title_override`, ordering of user-added panels, grid_*) are
//! preserved per spec R3.5 / R3.6.
//!
//! Persistence of the "removed upstream" state is **transient by design**:
//! the spec (R3.8) requires the panel to persist with a badge until the user
//! resolves it, but does NOT require persisting the badge itself across tab
//! reloads. Keeping it transient avoids another schema migration; once the
//! upstream is re-fetched, the same diff classification is reproducible.

use std::collections::{HashMap, HashSet};

use dbflux_core::DashboardDiff;
use dbflux_ui_base::DashboardPanel;

/// Structural fields taken from upstream for a Modified panel.
///
/// Upstream widget JSON itself doesn't carry layout in the shape this
/// crate uses (the importer maps widget → `WidgetImportSpec`); the caller
/// is responsible for translating each `UpstreamWidgetSnapshot` it cares
/// about into one of these records before invoking `apply_diff`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuralUpdate {
    pub source_widget_index: u32,
    pub source_widget_hash: String,
    pub grid_row: u32,
    pub grid_column: u32,
    pub grid_width: u32,
    pub grid_height: u32,
}

/// Outcome of [`apply_diff`].
///
/// `panels` is the merged set; `removed_upstream_panel_ids` is the
/// transient badge set the document should render. Panels that were
/// classified as "removed upstream" are intentionally **not** dropped from
/// `panels` — per spec R3.8 they persist until the user resolves them.
/// `DashboardPanel` is not `PartialEq` in `dbflux_ui_base`, so this struct
/// derives only `Debug` + `Clone` + `Default`. Tests compare individual
/// fields explicitly.
#[derive(Debug, Clone, Default)]
pub struct ApplyOutcome {
    pub panels: Vec<DashboardPanel>,
    pub removed_upstream_panel_ids: HashSet<String>,
}

/// Inputs needed to apply a `DashboardDiff` to a panel set.
///
/// `panels` is the current in-memory panel list. `structural_updates`
/// supplies the structural fields keyed by `local_panel_id` — the caller
/// resolves those from the upstream widget array. `added_panels` is the
/// set of brand-new `DashboardPanel` rows the caller has constructed for
/// `diff.added` (this module is pure and never invents new panels).
pub struct ApplyInputs<'a> {
    pub panels: Vec<DashboardPanel>,
    pub diff: &'a DashboardDiff,
    pub structural_updates: HashMap<String, StructuralUpdate>,
    pub added_panels: Vec<DashboardPanel>,
}

/// Apply a `DashboardDiff` to the current panel list and return the
/// merged result + the transient "removed upstream" badge set.
///
/// Rules:
/// - `modified` and `moved` panels get their structural fields overwritten
///   from `structural_updates`. `title_override` is preserved (R3.5/R3.6).
/// - `removed` panels are kept in place but added to
///   `removed_upstream_panel_ids` so the UI can render a badge (R3.8).
/// - `added_panels` are appended after every existing panel.
/// - `local_only_preserved` panels (user-added; `source_widget_index = None`)
///   are never touched (R3.7).
///
/// Panel ids are produced via `panel_identity` so this module stays
/// independent of the storage layer's id format.
pub fn apply_diff(inputs: ApplyInputs<'_>) -> ApplyOutcome {
    let ApplyInputs {
        mut panels,
        diff,
        structural_updates,
        added_panels,
    } = inputs;

    // Modified + moved: overwrite structural fields, preserve title_override.
    let mut structural_targets: HashMap<String, &StructuralUpdate> = HashMap::new();
    for m in &diff.modified {
        if let Some(u) = structural_updates.get(&m.local_panel_id) {
            structural_targets.insert(m.local_panel_id.clone(), u);
        }
    }
    for m in &diff.moved {
        if let Some(u) = structural_updates.get(&m.local_panel_id) {
            structural_targets.insert(m.local_panel_id.clone(), u);
        }
    }

    for panel in panels.iter_mut() {
        let id = panel_identity(panel);
        if let Some(update) = structural_targets.get(&id) {
            panel.source_widget_index = Some(update.source_widget_index);
            panel.source_widget_hash = Some(update.source_widget_hash.clone());
            panel.grid_row = update.grid_row;
            panel.grid_column = update.grid_column;
            panel.grid_width = update.grid_width;
            panel.grid_height = update.grid_height;
            // title_override is intentionally NOT touched.
        }
    }

    let removed_upstream_panel_ids: HashSet<String> = diff.removed.iter().cloned().collect();

    // Append new upstream-added panels after the current set.
    panels.extend(added_panels);

    ApplyOutcome {
        panels,
        removed_upstream_panel_ids,
    }
}

/// Canonical identity used to map `DashboardDiff` entries back to
/// `DashboardPanel` rows. Uses the panel's stable `(dashboard_id,
/// panel_index)` pair encoded as a string, matching the convention the
/// reconcile feed will produce in the caller.
pub fn panel_identity(panel: &DashboardPanel) -> String {
    format!("{}:{}", panel.dashboard_id, panel.panel_index)
}

/// Helper for the caller: build a `LocalPanelSnapshot` id that matches
/// `panel_identity` so `apply_diff` can resolve the local panel that owns
/// each diff entry.
pub fn snapshot_id(dashboard_id: uuid::Uuid, panel_index: u32) -> String {
    format!("{}:{}", dashboard_id, panel_index)
}

// Unit tests live in `tests/dashboard_sync.rs` (integration test crate) so
// the lib-test crate's macro-expansion recursion budget is not inflated; see
// lib.rs `recursion_limit` note for context.

