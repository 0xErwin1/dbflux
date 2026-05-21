//! Chart view-mode state and helpers for `AuditDocument`.
//!
//! File-level extraction following the KeyValueView/LogStreamView pattern:
//! `impl Render` stays in `mod.rs` (single `Context<T>` borrow constraint);
//! chart-specific state, helpers, and the render sub-function live here.
//!
//! Layout when in Chart mode:
//!   ┌──────────────────────────────────────────────┐
//!   │ shared toolbar (existing AuditDocument bar)  │
//!   ├──────────────────────────────────────────────┤
//!   │ chart area (ChartShell via Standalone host)  │
//!   └──────────────────────────────────────────────┘

use super::{AuditDocument, AuditDocumentSource};
use crate::ui::document::chart::ChartShell;
use crate::ui::tokens::Spacing;
use dbflux_audit::{AuditAggregateParams, AuditGroupColumn};
use dbflux_components::chart::{AggKind, AuditGroupBy, BindingSpec};
use dbflux_core::ColumnKind;
use dbflux_core::QueryResult;
use gpui::prelude::*;
use gpui::{AnyElement, Context, Entity, Task, Window};
use std::sync::Arc;

/// Which content panel the audit document currently shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuditViewMode {
    /// The default event-table view.
    #[default]
    Table,
    /// Aggregated chart view.
    Chart,
}

/// All chart-specific state owned by `AuditDocument`.
///
/// Stored as a single field `chart: AuditChartState` on the document so the
/// Table branch is completely unaffected when no chart work has been done.
pub struct AuditChartState {
    /// The `ChartShell` entity driven directly (Standalone host pattern).
    pub chart_shell: Entity<ChartShell>,

    /// Which dimension to group the aggregate by.
    pub group_by: AuditGroupBy,

    /// The most recent result successfully delivered to the shell.
    pub last_result: Option<Arc<QueryResult>>,

    /// Pending result from the background aggregate task, drained in `render`.
    ///
    /// `Ok` → deliver to shell; `Err` → show as a toast.
    pub pending_result: Option<Result<QueryResult, String>>,

    /// Monotonically increasing counter incremented before each aggregate
    /// background task is spawned. Stale completions carrying an older id
    /// are silently dropped.
    pub load_id: u64,

    /// Set to `true` once we have successfully seeded the `BindingSpec` on the
    /// shell for the first aggregate result. Reset to `false` after a group-by
    /// change so the binding is re-seeded for the new schema presentation.
    pub binding_seeded: bool,
}

impl AuditChartState {
    /// Creates the initial chart state for a new `AuditDocument`.
    pub fn new(cx: &mut Context<AuditDocument>) -> Self {
        let chart_shell = cx.new(ChartShell::new_standalone);

        Self {
            chart_shell,
            group_by: AuditGroupBy::Category,
            last_result: None,
            pending_result: None,
            load_id: 0,
            binding_seeded: false,
        }
    }
}

// ---------------------------------------------------------------------------
// AuditDocument impl block — chart helpers (physically in chart_view.rs)
// ---------------------------------------------------------------------------

impl AuditDocument {
    /// Converts a components-layer `AuditGroupBy` to the storage-layer
    /// `AuditGroupColumn` required by `AuditAggregateParams`.
    pub(super) fn audit_group_column(group_by: AuditGroupBy) -> AuditGroupColumn {
        match group_by {
            AuditGroupBy::Category => AuditGroupColumn::Category,
            AuditGroupBy::Outcome => AuditGroupColumn::Outcome,
            AuditGroupBy::Level => AuditGroupColumn::Level,
        }
    }

    /// Computes a sensible bucket width in milliseconds for the active time
    /// window, targeting roughly 60–120 buckets.
    ///
    /// Falls back to 1 h when the window is open-ended or effectively zero.
    pub(super) fn compute_bucket_ms(start_ms: Option<i64>, end_ms: Option<i64>) -> i64 {
        const FALLBACK_BUCKET_MS: i64 = 3_600_000; // 1 h
        const MIN_BUCKET_MS: i64 = 60_000; // 1 min
        const TARGET_BUCKETS: i64 = 120;

        let span_ms = match (start_ms, end_ms) {
            (Some(s), Some(e)) if e > s => e - s,
            _ => return FALLBACK_BUCKET_MS,
        };

        (span_ms / TARGET_BUCKETS).max(MIN_BUCKET_MS)
    }

    /// Spawns a background aggregate task for the current filters and chart
    /// group-by, then stashes the result in `chart.pending_result`.
    ///
    /// Only operates when the source is `Internal` (the local audit store).
    /// Increments `chart.load_id` so that stale completions can be detected.
    pub(super) fn trigger_chart_aggregate(&mut self, cx: &mut Context<Self>) {
        let AuditDocumentSource::Internal { adapter } = &self.source else {
            return;
        };

        self.chart.load_id += 1;
        let load_id = self.chart.load_id;

        let storage_filter = self.active_filter(None, None);
        let bucket_ms = Self::compute_bucket_ms(self.filters.start_ms, self.filters.end_ms);
        let group_by = Self::audit_group_column(self.chart.group_by);
        let adapter = adapter.clone();

        let agg_task: Task<Result<QueryResult, String>> =
            cx.background_executor().spawn(async move {
                let params = AuditAggregateParams {
                    bucket_ms,
                    group_by,
                    filter: storage_filter,
                };
                adapter.aggregate(&params)
            });

        cx.spawn(async move |this, cx| {
            let result = agg_task.await;

            let _ = cx.update(|cx| {
                let Some(entity) = this.upgrade() else {
                    return;
                };

                entity.update(cx, |doc, cx| {
                    if doc.chart.load_id != load_id {
                        // Stale result — a newer request has been issued.
                        return;
                    }

                    doc.chart.pending_result = Some(result);
                    cx.notify();
                });
            });
        })
        .detach();
    }

    /// Drains `chart.pending_result` (called from within `render`).
    ///
    /// On success, delivers the result to the `ChartShell` and seeds bindings
    /// when not yet done for this group-by setting.
    /// On error, stores a pending toast so the user sees the failure.
    pub(super) fn drain_chart_pending_result(&mut self, cx: &mut Context<Self>) {
        let Some(result) = self.chart.pending_result.take() else {
            return;
        };

        match result {
            Ok(query_result) => {
                let arc = Arc::new(query_result);
                let was_chart_mode = self.chart.last_result.is_some();

                self.chart.chart_shell.update(cx, |shell, cx| {
                    shell.set_result(&arc, was_chart_mode, cx);
                });

                // Seed the BindingSpec once per group-by setting so the chart
                // always opens with deterministic axis assignments.
                //
                // Wide schema: col 0 = bucket_ms (Timestamp X), cols 1..N =
                // one Integer series per distinct group value.  Collect all
                // Integer column indices as Y series; no group_by needed since
                // each group is already its own column.
                if !self.chart.binding_seeded {
                    self.chart.binding_seeded = true;

                    let y_cols: Vec<usize> = arc
                        .columns
                        .iter()
                        .enumerate()
                        .filter(|(_, col)| col.kind == ColumnKind::Integer)
                        .map(|(idx, _)| idx)
                        .collect();

                    self.chart.chart_shell.update(cx, |shell, cx| {
                        shell.apply_bindings(
                            BindingSpec {
                                x: 0,
                                y: y_cols,
                                group_by: None,
                                filter: None,
                                aggregation: AggKind::None,
                            },
                            cx,
                        );
                    });
                }

                self.chart.last_result = Some(arc);
            }

            Err(msg) => {
                use crate::ui::components::toast::PendingToast;
                self.pending_toast = Some(PendingToast {
                    message: msg,
                    is_error: true,
                });
            }
        }
    }

    /// Renders the chart area when `view_mode == Chart`.
    ///
    /// Called from `impl Render for AuditDocument` in `mod.rs` to keep the
    /// single `Context<AuditDocument>` borrow constraint satisfied.
    pub(super) fn render_chart_area(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        use dbflux_components::primitives::Text;
        use gpui::*;

        // Ensure the chart view is built for the current result.
        if let Some(result) = self.chart.last_result.clone() {
            self.chart.chart_shell.update(cx, |shell, cx| {
                shell.ensure_chart_view(&result, cx);
            });
        }

        let chart_view_entity = self.chart.chart_shell.read(cx).chart_view().cloned();

        if let Some(chart_entity) = chart_view_entity {
            div()
                .size_full()
                .p(Spacing::SM)
                .child(chart_entity)
                .into_any_element()
        } else {
            let msg = if self.chart.last_result.is_none() {
                "Aggregating audit events…"
            } else {
                "No chartable data detected in current result."
            };

            div()
                .size_full()
                .flex()
                .items_center()
                .justify_center()
                .child(Text::muted(msg))
                .into_any_element()
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_components::chart::AuditGroupBy;

    // T-CV-01: compute_bucket_ms — open-ended window uses fallback
    #[test]
    fn bucket_ms_open_ended_window_uses_fallback() {
        assert_eq!(
            AuditDocument::compute_bucket_ms(None, None),
            3_600_000,
            "open-ended window should fall back to 1 h bucket"
        );
    }

    // T-CV-02: compute_bucket_ms — inverted/zero range uses fallback
    #[test]
    fn bucket_ms_inverted_range_uses_fallback() {
        assert_eq!(
            AuditDocument::compute_bucket_ms(Some(1_000), Some(500)),
            3_600_000,
            "end <= start should fall back to 1 h bucket"
        );
    }

    // T-CV-03: compute_bucket_ms — 24 h window produces ≥ 1 min bucket
    #[test]
    fn bucket_ms_24h_window_above_minimum() {
        let start_ms: i64 = 0;
        let end_ms: i64 = 24 * 3_600_000;

        let bucket = AuditDocument::compute_bucket_ms(Some(start_ms), Some(end_ms));

        assert!(
            bucket >= 60_000,
            "bucket must be at least 1 min, got {bucket}"
        );
        assert!(
            bucket <= end_ms,
            "bucket must fit within the window, got {bucket}"
        );
    }

    // T-CV-04: compute_bucket_ms — very short window clamps to minimum
    #[test]
    fn bucket_ms_very_short_window_clamped_to_min() {
        let bucket = AuditDocument::compute_bucket_ms(Some(0), Some(1_000));
        assert_eq!(
            bucket, 60_000,
            "short window should clamp to 1 min min bucket"
        );
    }

    // T-CV-05: audit_group_column — correct mapping for all three variants
    #[test]
    fn audit_group_column_maps_all_variants() {
        assert!(matches!(
            AuditDocument::audit_group_column(AuditGroupBy::Category),
            AuditGroupColumn::Category
        ));
        assert!(matches!(
            AuditDocument::audit_group_column(AuditGroupBy::Outcome),
            AuditGroupColumn::Outcome
        ));
        assert!(matches!(
            AuditDocument::audit_group_column(AuditGroupBy::Level),
            AuditGroupColumn::Level
        ));
    }

    // T-CV-06: AuditViewMode — default is Table
    #[test]
    fn view_mode_default_is_table() {
        assert_eq!(AuditViewMode::default(), AuditViewMode::Table);
    }
}
