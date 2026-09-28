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

use dbflux_components::components::data_table::{
    DataTable, DataTableEvent, DataTableState, ModelSwap,
};
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

/// The two report tables, for keyboard focus.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DumpTable {
    LargestKeys,
    PrefixRollup,
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
    /// The table the keyboard was last in, whose columns the pane actions
    /// sort.
    focused_table: DumpTable,
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
            focused_table: DumpTable::LargestKeys,
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

    /// Table navigation runs inside the embedded `DataTable`s through their
    /// own key context. The document answers the keys around them: Escape
    /// cancels a running analysis, and the result tab keys move between the
    /// two tables. Anything else, `m` included, falls through: the
    /// workspace lists the pane actions when no menu answers `m`.
    pub fn dispatch_command(
        &mut self,
        command: dbflux_app::keymap::Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        use dbflux_app::keymap::Command;

        match command {
            Command::Cancel if matches!(self.phase, DumpAnalysisPhase::Parsing { .. }) => {
                self.cancel_analysis(cx);
                true
            }
            Command::NextResultTab | Command::PrevResultTab
                if self.prefix_rollup_state.is_some() =>
            {
                let next = match self.focused_table {
                    DumpTable::LargestKeys => DumpTable::PrefixRollup,
                    DumpTable::PrefixRollup => DumpTable::LargestKeys,
                };
                self.focus_table(next, window, cx);
                true
            }
            _ => false,
        }
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.table_state(self.focused_table).is_some() {
            self.focus_table(self.focused_table, window, cx);
        } else {
            self.focus_handle.focus(window, cx);
        }
    }

    fn table_state(&self, table: DumpTable) -> Option<&Entity<DataTableState>> {
        match table {
            DumpTable::LargestKeys => self.largest_keys_state.as_ref(),
            DumpTable::PrefixRollup => self.prefix_rollup_state.as_ref(),
        }
    }

    fn focus_table(&mut self, table: DumpTable, window: &mut Window, cx: &mut Context<Self>) {
        let Some(state) = self.table_state(table) else {
            return;
        };

        let handle = state.read(cx).focus_handle().clone();
        handle.focus(window, cx);
        self.focused_table = table;
        cx.notify();
    }

    /// The Cancel button while the dump is read, then the other table and a
    /// sort per column of the focused table, as its header clicks sort it.
    pub(crate) fn pane_actions(
        &self,
        this: &Entity<Self>,
        cx: &App,
    ) -> Vec<crate::pane::PaneAction> {
        use crate::pane::PaneAction;
        use dbflux_components::icons::AppIcon;

        let mut actions = Vec::new();

        if matches!(self.phase, DumpAnalysisPhase::Parsing { .. }) {
            let target = this.downgrade();
            actions.push(
                PaneAction::callback(
                    "dump-cancel",
                    dbflux_i18n::t!("document.dump_analysis.parsing.cancel"),
                    move |_window, cx| {
                        if let Some(document) = target.upgrade() {
                            document.update(cx, |document, cx| document.cancel_analysis(cx));
                        }
                    },
                )
                .icon(AppIcon::X),
            );
        }

        if self.prefix_rollup_state.is_none() {
            return actions;
        }

        let (switch_id, switch_label, other) = match self.focused_table {
            DumpTable::LargestKeys => (
                "dump-show-by-prefix",
                dbflux_i18n::t!("document.dump_analysis.done.by_prefix.title"),
                DumpTable::PrefixRollup,
            ),
            DumpTable::PrefixRollup => (
                "dump-show-largest-keys",
                dbflux_i18n::t!("document.dump_analysis.done.largest_keys.title"),
                DumpTable::LargestKeys,
            ),
        };
        let target = this.downgrade();
        actions.push(PaneAction::callback(
            switch_id,
            switch_label,
            move |window, cx| {
                if let Some(document) = target.upgrade() {
                    document.update(cx, |document, cx| document.focus_table(other, window, cx));
                }
            },
        ));

        let Some(state) = self.table_state(self.focused_table) else {
            return actions;
        };

        for (index, column) in state.read(cx).model().columns.iter().enumerate() {
            let state = state.downgrade();
            actions.push(PaneAction::callback(
                format!("dump-sort-{index}"),
                dbflux_i18n::t!(
                    "document.dump_analysis.pane_actions.sort_by",
                    column = column.title
                ),
                move |_window, cx| {
                    if let Some(state) = state.upgrade() {
                        state.update(cx, |state, cx| state.cycle_sort(index, cx));
                    }
                },
            ));
        }

        actions
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

                let still_running = cx.update(|cx| {
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
                });

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
            |this, _, event: &DataTableEvent, cx| match event {
                DataTableEvent::SortChanged(Some(sort)) => {
                    this.sort_largest_keys(sort.column_ix, sort.direction, cx);
                }
                DataTableEvent::Focused => this.focused_table = DumpTable::LargestKeys,
                _ => {}
            },
        );
        let largest_keys_table = cx
            .new(|cx| DataTable::new("dump-analysis-largest-keys", largest_keys_state.clone(), cx));

        let prefix_model = Arc::new(render::prefix_rollup_table_model(&report.prefix_rollup));
        let prefix_rollup_state = cx.new(|cx| DataTableState::new(prefix_model, cx));
        let prefix_rollup_subscription = cx.subscribe(
            &prefix_rollup_state,
            |this, _, event: &DataTableEvent, cx| match event {
                DataTableEvent::SortChanged(Some(sort)) => {
                    this.sort_prefix_rollup(sort.column_ix, sort.direction, cx);
                }
                DataTableEvent::Focused => this.focused_table = DumpTable::PrefixRollup,
                _ => {}
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
            state.update(cx, |state, cx| {
                state.set_model(model, ModelSwap::KeepCursor, cx)
            });
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
            state.update(cx, |state, cx| {
                state.set_model(model, ModelSwap::KeepCursor, cx)
            });
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

    mod keyboard {
        use super::super::DumpAnalysisDocument;
        use crate::keyboard_test_support::{host_document, init_keyboard_runtime};
        use crate::pane::PaneActionRun;
        use dbflux_app::keymap::Command;
        use dbflux_core::{
            DumpAnalysisError, DumpAnalysisReport, DumpAnalyzer, DumpKeyEntry, DumpPrefixEntry,
        };
        use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
        use std::path::{Path, PathBuf};
        use std::sync::Arc;

        struct FakeAnalyzer;

        impl DumpAnalyzer for FakeAnalyzer {
            fn display_name(&self) -> &'static str {
                "Fake dump"
            }

            fn file_extensions(&self) -> &'static [&'static str] {
                &["fake"]
            }

            fn size_caveat(&self) -> &'static str {
                "caveat"
            }

            fn analyze(
                &self,
                _path: &Path,
                _progress: &(dyn Fn(u64, Option<u64>) + Sync),
                cancelled: &(dyn Fn() -> bool + Sync),
            ) -> Result<DumpAnalysisReport, DumpAnalysisError> {
                if cancelled() {
                    return Err(DumpAnalysisError::Cancelled);
                }

                let key = |key: &str, bytes| DumpKeyEntry {
                    key: key.to_string(),
                    type_name: "string".to_string(),
                    serialized_bytes: bytes,
                    expires_at_ms: None,
                    database: 0,
                };

                Ok(DumpAnalysisReport {
                    total_keys: 2,
                    total_serialized_bytes: 30,
                    keys_by_type: Vec::new(),
                    largest_keys: vec![key("big", 20), key("alpha", 10)],
                    prefix_rollup: vec![DumpPrefixEntry {
                        prefix: "user".to_string(),
                        key_count: 2,
                        serialized_bytes: 30,
                    }],
                })
            }
        }

        fn app_state(cx: &mut TestAppContext) -> Entity<dbflux_ui_base::AppStateEntity> {
            cx.update(|cx| {
                cx.new(|_| {
                    let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                        .expect("in-memory storage");
                    dbflux_ui_base::AppStateEntity::new_with_storage_runtime(runtime)
                        .expect("test storage setup")
                })
            })
        }

        fn open(
            cx: &mut TestAppContext,
            cancel_first: bool,
        ) -> (Entity<DumpAnalysisDocument>, &mut VisualTestContext) {
            init_keyboard_runtime(cx);
            let app_state = app_state(cx);

            let (host, window) = host_document(
                cx,
                move |window, cx| {
                    let document = cx.new(|cx| {
                        DumpAnalysisDocument::new(
                            Arc::new(FakeAnalyzer),
                            PathBuf::from("/tmp/dump.fake"),
                            false,
                            app_state,
                            cx,
                        )
                    });
                    if cancel_first {
                        document.update(cx, |document, cx| {
                            document.dispatch_command(Command::Cancel, window, cx);
                        });
                    }
                    document
                },
                |document, _cx| document.active_context(),
                |document, command, window, cx| document.dispatch_command(command, window, cx),
            );

            let document = window.update(|_, cx| host.read(cx).document.clone());
            window.update(|window, cx| document.update(cx, |d, cx| d.focus(window, cx)));
            window.run_until_parked();

            (document, window)
        }

        fn prefix_table_has_focus(
            document: &Entity<DumpAnalysisDocument>,
            window: &mut VisualTestContext,
        ) -> bool {
            window.update(|window, cx| {
                document
                    .read(cx)
                    .prefix_rollup_state
                    .as_ref()
                    .is_some_and(|state| state.read(cx).focus_handle().is_focused(window))
            })
        }

        #[gpui::test]
        fn alt_l_and_alt_h_switch_between_the_two_tables(cx: &mut TestAppContext) {
            let (document, window) = open(cx, false);
            assert!(!prefix_table_has_focus(&document, window));

            window.simulate_keystrokes("alt-l");
            assert!(
                prefix_table_has_focus(&document, window),
                "Alt+L moves to By prefix"
            );

            window.simulate_keystrokes("alt-h");
            assert!(
                !prefix_table_has_focus(&document, window),
                "Alt+H moves back"
            );
        }

        #[gpui::test]
        fn the_pane_actions_sort_the_focused_table(cx: &mut TestAppContext) {
            let (document, window) = open(cx, false);

            let actions = window.update(|_, cx| document.read(cx).pane_actions(&document, cx));
            let ids: Vec<String> = actions.iter().map(|action| action.id.to_string()).collect();
            assert_eq!(
                ids,
                [
                    "dump-show-by-prefix",
                    "dump-sort-0",
                    "dump-sort-1",
                    "dump-sort-2",
                    "dump-sort-3"
                ]
            );

            let Some(PaneActionRun::Callback(sort_by_key)) =
                actions.get(1).map(|action| action.run.clone())
            else {
                panic!("sorting is a callback");
            };
            window.update(|window, cx| sort_by_key(window, cx));
            window.run_until_parked();

            let first_key = window.update(|_, cx| match &document.read(cx).phase {
                super::super::DumpAnalysisPhase::Done(report) => report.largest_keys[0].key.clone(),
                _ => String::new(),
            });
            assert_eq!(first_key, "alpha", "sorting by Key orders the keys by name");
        }

        #[gpui::test]
        fn cancel_stops_a_running_analysis(cx: &mut TestAppContext) {
            let (document, window) = open(cx, true);

            assert!(matches!(
                window.update(|_, cx| document.read(cx).state()),
                crate::types::DocumentState::Clean
            ));
            assert!(window.update(|_, cx| matches!(
                document.read(cx).phase,
                super::super::DumpAnalysisPhase::Cancelled
            )));
        }
    }

    #[test]
    fn progress_fraction_clamps_above_one() {
        // The analyzer's progress callback may briefly overshoot the
        // reported total (e.g. trailing checksum bytes); the fraction must
        // never exceed 1.0.
        assert_eq!(progress_fraction(300, Some(200)), Some(1.0));
    }
}
