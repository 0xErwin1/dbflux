//! Offline analysis report for a driver's native dump/export file (e.g. a
//! Redis RDB file), opened from the "Analyze database dump…" command.
//!
//! The document never inspects `driver_id`: it is constructed with an
//! already-resolved `Arc<dyn DumpAnalyzer>` (see
//! `dbflux_core::connection::dump_analysis`) and only reads that trait's
//! generic surface (`display_name`, `size_caveat`, `analyze`).

mod pane;
mod render;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dbflux_components::components::data_table::{DataTable, DataTableEvent, DataTableState};
use dbflux_core::{
    DumpAnalysisError, DumpAnalysisReport, DumpAnalyzer, SortDirection, TaskKind, TaskStatus,
};
use dbflux_ui_base::AppStateEntity;
use gpui::*;

use super::handle::DocumentEvent;
use super::task_runner::DocumentTaskRunner;
use super::types::{DocumentId, DocumentState};

/// The document's current state, driven by the background analysis task.
enum DumpAnalysisPhase {
    /// The dump file is being streamed and aggregated on a background
    /// executor. `bytes_read`/`total_bytes` mirror the analyzer's own
    /// progress callback.
    Parsing {
        bytes_read: u64,
        total_bytes: Option<u64>,
    },
    /// Analysis was cancelled before completion (via the inline Cancel
    /// button or the Tasks panel).
    Cancelled,
    /// Analysis failed; the error is kept so its display message can be
    /// rendered and re-derived without losing detail (e.g. the byte offset
    /// of a malformed dump).
    Failed(DumpAnalysisError),
    /// Analysis completed. The report's `largest_keys`/`prefix_rollup`
    /// vectors are re-sorted in place when the user clicks a table header.
    Done(DumpAnalysisReport),
}

/// Searchable, read-only analysis report opened for a driver's native
/// dump/export file, without a live connection to any profile.
pub struct DumpAnalysisDocument {
    id: DocumentId,
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,
    is_active_tab: bool,
    path: PathBuf,
    analyzer_display_name: &'static str,
    size_caveat: &'static str,
    /// `true` when more than one registered driver's analyzer matched the
    /// dump file's extension and the first (by display name) was used —
    /// surfaced in the header so the choice isn't silent.
    multiple_analyzers_matched: bool,
    phase: DumpAnalysisPhase,
    runner: DocumentTaskRunner,
    largest_keys_state: Option<Entity<DataTableState>>,
    largest_keys_table: Option<Entity<DataTable>>,
    prefix_rollup_state: Option<Entity<DataTableState>>,
    prefix_rollup_table: Option<Entity<DataTable>>,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<DocumentEvent> for DumpAnalysisDocument {}

/// Computes the fraction (0.0–1.0) of a dump file read so far, for the
/// task-manager progress bar. Returns `None` when the analyzer could not
/// determine the dump's total size upfront, in which case progress stays
/// indeterminate rather than showing a misleading bar.
pub(crate) fn progress_fraction(bytes_read: u64, total_bytes: Option<u64>) -> Option<f32> {
    let total = total_bytes.filter(|&total| total > 0)?;
    Some((bytes_read as f32 / total as f32).clamp(0.0, 1.0))
}

impl DumpAnalysisDocument {
    pub fn new(
        analyzer: Arc<dyn DumpAnalyzer>,
        path: PathBuf,
        multiple_analyzers_matched: bool,
        app_state: Entity<AppStateEntity>,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut doc = Self {
            id: DocumentId::new(),
            app_state: app_state.clone(),
            focus_handle: cx.focus_handle(),
            is_active_tab: true,
            path: path.clone(),
            analyzer_display_name: analyzer.display_name(),
            size_caveat: analyzer.size_caveat(),
            multiple_analyzers_matched,
            phase: DumpAnalysisPhase::Parsing {
                bytes_read: 0,
                total_bytes: None,
            },
            runner: DocumentTaskRunner::new(app_state),
            largest_keys_state: None,
            largest_keys_table: None,
            prefix_rollup_state: None,
            prefix_rollup_table: None,
            _subscriptions: Vec::new(),
        };

        doc.start_analysis(analyzer, cx);
        doc
    }

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        let file_name = self
            .path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| self.path.display().to_string());

        crate::labels::dump_analysis_title(self.analyzer_display_name, &file_name)
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub fn can_close(&self) -> bool {
        true
    }

    pub fn state(&self) -> DocumentState {
        match &self.phase {
            DumpAnalysisPhase::Parsing { .. } => DocumentState::Loading,
            DumpAnalysisPhase::Cancelled => DocumentState::Clean,
            DumpAnalysisPhase::Failed(_) => DocumentState::Error,
            DumpAnalysisPhase::Done(_) => DocumentState::Clean,
        }
    }

    /// Always `None` — this document never carries a live connection.
    pub fn connection_id(&self) -> Option<uuid::Uuid> {
        None
    }

    pub fn refresh_policy(&self) -> dbflux_core::RefreshPolicy {
        dbflux_core::RefreshPolicy::Manual
    }

    pub fn set_refresh_policy(
        &mut self,
        _policy: dbflux_core::RefreshPolicy,
        _cx: &mut Context<Self>,
    ) {
        // A one-shot offline report has nothing to periodically refresh.
    }

    pub fn set_active_tab(&mut self, active: bool) {
        self.is_active_tab = active;
    }

    pub fn active_context(&self) -> dbflux_app::keymap::ContextId {
        dbflux_app::keymap::ContextId::Results
    }

    pub fn dispatch_command(
        &mut self,
        _cmd: dbflux_app::keymap::Command,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> bool {
        // Table navigation is handled internally by the embedded `DataTable`
        // entities via their own focus handles; this document has no
        // additional commands to intercept.
        false
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(state) = &self.largest_keys_state {
            let handle = state.read(cx).focus_handle().clone();
            handle.focus(window);
        } else {
            self.focus_handle.focus(window);
        }
    }

    fn start_analysis(&mut self, analyzer: Arc<dyn DumpAnalyzer>, cx: &mut Context<Self>) {
        let file_name = self
            .path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| self.path.display().to_string());
        let description =
            crate::labels::dump_analysis_task_label(self.analyzer_display_name, &file_name);

        let (task_id, cancel_token) =
            self.runner
                .start_primary(TaskKind::DumpAnalysis, description, cx);

        let progress = Arc::new(Mutex::new((0u64, None::<u64>)));

        let ticker_progress = Arc::clone(&progress);
        cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor()
                    .timer(Duration::from_millis(150))
                    .await;

                let still_running = cx
                    .update(|cx| {
                        this.update(cx, |doc, cx| {
                            let is_running = doc
                                .app_state
                                .read(cx)
                                .tasks()
                                .get(task_id)
                                .map(|snapshot| snapshot.status == TaskStatus::Running)
                                .unwrap_or(false);

                            if !is_running {
                                return false;
                            }

                            let (bytes_read, total_bytes) = *ticker_progress
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());

                            doc.phase = DumpAnalysisPhase::Parsing {
                                bytes_read,
                                total_bytes,
                            };

                            if let Some(fraction) = progress_fraction(bytes_read, total_bytes) {
                                doc.app_state.update(cx, |state, _cx| {
                                    state.tasks_mut().update_progress(task_id, fraction);
                                });
                            }

                            cx.notify();
                            true
                        })
                        .unwrap_or(false)
                    })
                    .unwrap_or(false);

                if !still_running {
                    break;
                }
            }
        })
        .detach();

        let worker_progress = Arc::clone(&progress);
        let worker_path = self.path.clone();

        cx.spawn(async move |this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move {
                    analyzer.analyze(
                        &worker_path,
                        &|bytes_read, total_bytes| {
                            if let Ok(mut guard) = worker_progress.lock() {
                                *guard = (bytes_read, total_bytes);
                            }
                        },
                        &|| cancel_token.is_cancelled(),
                    )
                })
                .await;

            cx.update(|cx| {
                this.update(cx, |doc, cx| {
                    // A prior cancellation (button or Tasks panel) already
                    // moved the document to its terminal state; a late
                    // result from the background executor must not
                    // overwrite it.
                    if matches!(doc.phase, DumpAnalysisPhase::Cancelled) {
                        return;
                    }

                    match result {
                        Ok(report) => {
                            doc.runner.complete_primary(task_id, cx);
                            doc.build_tables(report, cx);
                        }
                        Err(DumpAnalysisError::Cancelled) => {
                            doc.phase = DumpAnalysisPhase::Cancelled;
                        }
                        Err(other) => {
                            let message = crate::labels::dump_analysis_error_message(&other);
                            doc.runner.fail_primary(task_id, message, cx);
                            doc.phase = DumpAnalysisPhase::Failed(other);
                        }
                    }

                    cx.notify();
                })
            })
            .ok();
        })
        .detach();
    }

    fn cancel_analysis(&mut self, cx: &mut Context<Self>) {
        self.runner.cancel_primary(cx);
    }

    fn build_tables(&mut self, report: DumpAnalysisReport, cx: &mut Context<Self>) {
        let largest_model = Arc::new(render::largest_keys_table_model(&report.largest_keys));
        let largest_keys_state = cx.new(|cx| DataTableState::new(largest_model, cx));
        let largest_keys_subscription = cx.subscribe(
            &largest_keys_state,
            |this, _, event: &DataTableEvent, cx| {
                if let DataTableEvent::SortChanged(Some(sort)) = event {
                    this.sort_largest_keys(sort.column_ix, sort.direction, cx);
                }
            },
        );
        let largest_keys_table = cx
            .new(|cx| DataTable::new("dump-analysis-largest-keys", largest_keys_state.clone(), cx));

        let prefix_model = Arc::new(render::prefix_rollup_table_model(&report.prefix_rollup));
        let prefix_rollup_state = cx.new(|cx| DataTableState::new(prefix_model, cx));
        let prefix_rollup_subscription = cx.subscribe(
            &prefix_rollup_state,
            |this, _, event: &DataTableEvent, cx| {
                if let DataTableEvent::SortChanged(Some(sort)) = event {
                    this.sort_prefix_rollup(sort.column_ix, sort.direction, cx);
                }
            },
        );
        let prefix_rollup_table =
            cx.new(|cx| DataTable::new("dump-analysis-by-prefix", prefix_rollup_state.clone(), cx));

        self.largest_keys_state = Some(largest_keys_state);
        self.largest_keys_table = Some(largest_keys_table);
        self.prefix_rollup_state = Some(prefix_rollup_state);
        self.prefix_rollup_table = Some(prefix_rollup_table);
        self._subscriptions
            .extend([largest_keys_subscription, prefix_rollup_subscription]);
        self.phase = DumpAnalysisPhase::Done(report);
    }

    fn sort_largest_keys(
        &mut self,
        column_ix: usize,
        direction: SortDirection,
        cx: &mut Context<Self>,
    ) {
        let DumpAnalysisPhase::Done(report) = &mut self.phase else {
            return;
        };

        render::sort_largest_keys(&mut report.largest_keys, column_ix, direction);
        let model = Arc::new(render::largest_keys_table_model(&report.largest_keys));

        if let Some(state) = &self.largest_keys_state {
            state.update(cx, |state, cx| state.set_model(model, cx));
        }
    }

    fn sort_prefix_rollup(
        &mut self,
        column_ix: usize,
        direction: SortDirection,
        cx: &mut Context<Self>,
    ) {
        let DumpAnalysisPhase::Done(report) = &mut self.phase else {
            return;
        };

        render::sort_prefix_rollup(&mut report.prefix_rollup, column_ix, direction);
        let model = Arc::new(render::prefix_rollup_table_model(&report.prefix_rollup));

        if let Some(state) = &self.prefix_rollup_state {
            state.update(cx, |state, cx| state.set_model(model, cx));
        }
    }
}

#[cfg(test)]
mod tests {
    // Import only what we need — avoid `use super::*` which pulls in
    // `gpui::*` and triggers macro recursion (see `task_runner.rs`).
    use super::progress_fraction;

    #[test]
    fn progress_fraction_is_none_when_total_unknown() {
        assert_eq!(progress_fraction(1024, None), None);
    }

    #[test]
    fn progress_fraction_is_none_when_total_is_zero() {
        assert_eq!(progress_fraction(0, Some(0)), None);
    }

    #[test]
    fn progress_fraction_computes_ratio_when_total_known() {
        assert_eq!(progress_fraction(50, Some(200)), Some(0.25));
    }

    #[test]
    fn progress_fraction_clamps_above_one() {
        // The analyzer's progress callback may briefly overshoot the
        // reported total (e.g. trailing checksum bytes); the fraction must
        // never exceed 1.0.
        assert_eq!(progress_fraction(300, Some(200)), Some(1.0));
    }
}
