use super::data_grid_panel::{DataGridEvent, DataGridPanel};
use super::handle::DocumentEvent;
use super::task_runner::DocumentTaskRunner;
use super::types::{DocumentId, DocumentState};
use crate::app::AppState;
use crate::keymap::{Command, ContextId};
use crate::ui::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::ui::history_modal::{HistoryModal, HistoryQuerySelected};
use crate::ui::icons::AppIcon;
use crate::ui::toast::ToastExt;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{
    DangerousQueryKind, DbError, DiagnosticSeverity as CoreDiagnosticSeverity,
    EditorDiagnostic as CoreEditorDiagnostic, HistoryEntry, QueryRequest, QueryResult,
    RefreshPolicy, ValidationResult, detect_dangerous_query,
};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::highlighter::{
    Diagnostic as InputDiagnostic, DiagnosticSeverity as InputDiagnosticSeverity,
};
use gpui_component::input::{
    CompletionProvider, Input, InputEvent, InputState, Position as InputPosition, Rope,
};
use gpui_component::resizable::{resizable_panel, v_resizable};
use lsp_types::{
    CompletionContext, CompletionItem, CompletionItemKind, CompletionResponse, InsertTextFormat,
};
use std::cmp::min;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

mod completion;
mod diagnostics;
mod execution;
mod focus;
mod render;

use completion::QueryCompletionProvider;

/// A single result tab within the SqlQueryDocument.
struct ResultTab {
    id: Uuid,
    title: String,
    grid: Entity<DataGridPanel>,
    _subscription: Subscription,
}

/// Internal layout of the document.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum SqlQueryLayout {
    #[default]
    Split,
    EditorOnly,
    ResultsOnly,
}

/// Where focus is within the document.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SqlQueryFocus {
    #[default]
    Editor,
    Results,
}

pub struct SqlQueryDocument {
    // Identity
    id: DocumentId,
    title: String,
    state: DocumentState,
    connection_id: Option<Uuid>,

    // Dependencies
    app_state: Entity<AppState>,

    // Editor
    input_state: Entity<InputState>,
    _input_subscriptions: Vec<Subscription>,
    original_content: String,
    saved_query_id: Option<Uuid>,

    // Execution
    execution_history: Vec<ExecutionRecord>,
    active_execution_index: Option<usize>,
    pending_result: Option<PendingQueryResult>,

    // Result tabs
    result_tabs: Vec<ResultTab>,
    active_result_index: Option<usize>,
    result_tab_counter: usize,
    run_in_new_tab: bool,

    // History modal
    history_modal: Entity<HistoryModal>,
    _history_subscriptions: Vec<Subscription>,
    pending_set_query: Option<HistoryQuerySelected>,

    // Layout/focus
    layout: SqlQueryLayout,
    focus_handle: FocusHandle,
    focus_mode: SqlQueryFocus,
    results_maximized: bool,

    // Task runner (query execution)
    runner: DocumentTaskRunner,
    refresh_policy: RefreshPolicy,
    refresh_dropdown: Entity<Dropdown>,
    pending_auto_refresh: bool,
    _refresh_timer: Option<Task<()>>,
    _refresh_subscriptions: Vec<Subscription>,

    // Dangerous query confirmation
    pending_dangerous_query: Option<PendingDangerousQuery>,

    // Diagnostic debounce: incremental request id to discard stale results.
    diagnostic_request_id: u64,
    _diagnostic_debounce: Option<Task<()>>,
}

struct PendingQueryResult {
    task_id: dbflux_core::TaskId,
    exec_id: Uuid,
    query: String,
    result: Result<QueryResult, DbError>,
}

/// Pending dangerous query confirmation.
struct PendingDangerousQuery {
    query: String,
    kind: DangerousQueryKind,
    in_new_tab: bool,
}

/// Record of a query execution.
#[derive(Clone)]
pub struct ExecutionRecord {
    pub id: Uuid,
    pub started_at: Instant,
    pub finished_at: Option<Instant>,
    pub result: Option<Arc<QueryResult>>,
    pub error: Option<String>,
    pub rows_affected: Option<u64>,
}

impl SqlQueryDocument {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let connection_id = app_state.read(cx).active_connection_id();

        // Get query language from the active connection, default to SQL
        let query_language = connection_id
            .and_then(|id| app_state.read(cx).connections().get(&id))
            .map(|conn| conn.connection.metadata().query_language)
            .unwrap_or(dbflux_core::QueryLanguage::Sql);

        let editor_mode = query_language.editor_mode();
        let placeholder = query_language.placeholder();

        let input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor(editor_mode)
                .line_number(true)
                .soft_wrap(false)
                .placeholder(placeholder)
        });

        let completion_provider: Rc<dyn CompletionProvider> = Rc::new(
            QueryCompletionProvider::new(query_language, app_state.clone(), connection_id),
        );
        input_state.update(cx, |state, _cx| {
            state.lsp.completion_provider = Some(completion_provider);
        });

        let input_change_sub = cx.subscribe_in(
            &input_state,
            window,
            |this, _input, event: &InputEvent, _window, cx| match event {
                InputEvent::Change => {
                    this.schedule_diagnostic_refresh(cx);
                }
                InputEvent::Focus => {
                    this.enter_editor_mode(cx);
                }
                InputEvent::Blur | InputEvent::PressEnter { .. } => {}
            },
        );

        // Create history modal
        let history_modal = cx.new(|cx| HistoryModal::new(app_state.clone(), window, cx));

        // Subscribe to history modal events
        let query_selected_sub = cx.subscribe(
            &history_modal,
            |this, _, event: &HistoryQuerySelected, cx| {
                this.pending_set_query = Some(event.clone());
                cx.notify();
            },
        );

        let runner = {
            let mut r = DocumentTaskRunner::new(app_state.clone());
            if let Some(pid) = connection_id {
                r.set_profile_id(pid);
            }
            r
        };

        let refresh_dropdown = cx.new(|_cx| {
            let items = RefreshPolicy::ALL
                .iter()
                .map(|policy| DropdownItem::new(policy.label()))
                .collect();

            Dropdown::new("sql-auto-refresh")
                .items(items)
                .selected_index(Some(RefreshPolicy::Manual.index()))
                .compact_trigger(true)
        });

        let refresh_policy_sub = cx.subscribe_in(
            &refresh_dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, window, cx| {
                let policy = RefreshPolicy::from_index(event.index);

                if policy.is_auto() && !this.can_auto_refresh(cx) {
                    this.refresh_dropdown.update(cx, |dd, cx| {
                        dd.set_selected_index(Some(RefreshPolicy::Manual.index()), cx);
                    });
                    cx.toast_warning("Auto-refresh blocked: query modifies data", window);
                    return;
                }

                this.set_refresh_policy(policy, cx);
            },
        );

        Self {
            id: DocumentId::new(),
            title: "Query 1".to_string(),
            state: DocumentState::Clean,
            connection_id,
            app_state,
            input_state,
            _input_subscriptions: vec![input_change_sub],
            original_content: String::new(),
            saved_query_id: None,
            execution_history: Vec::new(),
            active_execution_index: None,
            pending_result: None,
            result_tabs: Vec::new(),
            active_result_index: None,
            result_tab_counter: 0,
            run_in_new_tab: false,
            history_modal,
            _history_subscriptions: vec![query_selected_sub],
            pending_set_query: None,
            layout: SqlQueryLayout::EditorOnly,
            focus_handle: cx.focus_handle(),
            focus_mode: SqlQueryFocus::Editor,
            results_maximized: false,
            runner,
            refresh_policy: RefreshPolicy::Manual,
            refresh_dropdown,
            pending_auto_refresh: false,
            _refresh_timer: None,
            _refresh_subscriptions: vec![refresh_policy_sub],
            pending_dangerous_query: None,
            diagnostic_request_id: 0,
            _diagnostic_debounce: None,
        }
    }

    pub fn can_auto_refresh(&self, cx: &App) -> bool {
        dbflux_core::is_safe_read_query(&self.input_state.read(cx).value())
    }

    pub fn set_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        if self.refresh_policy == policy {
            return;
        }

        self.refresh_policy = policy;
        self.update_refresh_timer(cx);
        cx.notify();
    }

    fn update_refresh_timer(&mut self, cx: &mut Context<Self>) {
        self._refresh_timer = None;

        let Some(duration) = self.refresh_policy.duration() else {
            return;
        };

        self._refresh_timer = Some(cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor().timer(duration).await;

                let _ = cx.update(|cx| {
                    let Some(entity) = this.upgrade() else {
                        return;
                    };

                    entity.update(cx, |doc, cx| {
                        if !doc.refresh_policy.is_auto() || doc.runner.is_primary_active() {
                            return;
                        }

                        doc.pending_auto_refresh = true;
                        cx.notify();
                    });
                });
            }
        }));
    }

    /// Sets the document content.
    pub fn set_content(&mut self, sql: &str, window: &mut Window, cx: &mut Context<Self>) {
        let sql_owned = sql.to_string();
        self.input_state
            .update(cx, |state, cx| state.set_value(&sql_owned, window, cx));
        self.original_content = sql_owned;
        self.refresh_editor_diagnostics(window, cx);
    }

    /// Creates document with specific title.
    pub fn with_title(mut self, title: String) -> Self {
        self.title = title;
        self
    }

    // === Accessors for DocumentHandle ===

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        self.title.clone()
    }

    pub fn state(&self) -> DocumentState {
        self.state
    }

    pub fn refresh_policy(&self) -> RefreshPolicy {
        self.refresh_policy
    }

    pub fn connection_id(&self) -> Option<Uuid> {
        self.connection_id
    }

    pub fn can_close(&self, cx: &App) -> bool {
        !self.has_unsaved_changes(cx)
    }

    /// Returns true if the editor content differs from the original content.
    pub fn has_unsaved_changes(&self, cx: &App) -> bool {
        let current = self.input_state.read(cx).value();
        current != self.original_content
    }

    // === Command Dispatch ===

    /// Route commands to the history modal when it's visible.
    fn dispatch_to_history_modal(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match cmd {
            Command::Cancel => {
                self.history_modal.update(cx, |modal, cx| modal.close(cx));
                true
            }
            Command::SelectNext => {
                self.history_modal
                    .update(cx, |modal, cx| modal.select_next(cx));
                true
            }
            Command::SelectPrev => {
                self.history_modal
                    .update(cx, |modal, cx| modal.select_prev(cx));
                true
            }
            Command::Execute => {
                self.history_modal
                    .update(cx, |modal, cx| modal.execute_selected(window, cx));
                true
            }
            Command::Delete => {
                self.history_modal
                    .update(cx, |modal, cx| modal.delete_selected(cx));
                true
            }
            Command::ToggleFavorite => {
                self.history_modal
                    .update(cx, |modal, cx| modal.toggle_favorite_selected(cx));
                true
            }
            Command::Rename => {
                self.history_modal
                    .update(cx, |modal, cx| modal.start_rename_selected(window, cx));
                true
            }
            Command::FocusSearch => {
                self.history_modal
                    .update(cx, |modal, cx| modal.focus_search(window, cx));
                true
            }
            Command::SaveQuery => {
                self.history_modal
                    .update(cx, |modal, cx| modal.save_selected_history(window, cx));
                true
            }
            // Other commands are not handled by the modal
            _ => false,
        }
    }

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        // When dangerous query confirmation is showing, handle only modal commands
        if self.pending_dangerous_query.is_some() {
            match cmd {
                Command::Cancel => {
                    self.cancel_dangerous_query(cx);
                    return true;
                }
                Command::Execute => {
                    self.confirm_dangerous_query(false, window, cx);
                    return true;
                }
                _ => return false,
            }
        }

        // When history modal is open, route commands to it first
        if self.history_modal.read(cx).is_visible()
            && self.dispatch_to_history_modal(cmd, window, cx)
        {
            return true;
        }

        // When focused on results, delegate to active DataGridPanel
        if self.focus_mode == SqlQueryFocus::Results
            && let Some(grid) = self.active_result_grid()
        {
            // Special handling for FocusUp to exit results
            if cmd == Command::FocusUp {
                self.focus_mode = SqlQueryFocus::Editor;
                cx.notify();
                return true;
            }

            // Delegate to grid
            let handled = grid.update(cx, |g, cx| g.dispatch_command(cmd, window, cx));
            if handled {
                return true;
            }
        }

        match cmd {
            Command::RunQuery => {
                self.run_query(window, cx);
                true
            }
            Command::RunQueryInNewTab => {
                self.run_query_in_new_tab(window, cx);
                true
            }
            Command::Cancel | Command::CancelQuery => {
                if self.runner.is_primary_active() {
                    self.cancel_query(cx);
                    true
                } else {
                    false
                }
            }

            // Focus navigation from editor to results
            Command::FocusDown => {
                if self.focus_mode == SqlQueryFocus::Editor && !self.result_tabs.is_empty() {
                    self.focus_mode = SqlQueryFocus::Results;
                    cx.notify();
                    true
                } else {
                    false
                }
            }

            // Layout toggles
            Command::ToggleEditor => {
                self.layout = match self.layout {
                    SqlQueryLayout::EditorOnly => SqlQueryLayout::Split,
                    _ => SqlQueryLayout::EditorOnly,
                };
                cx.notify();
                true
            }
            Command::ToggleResults | Command::TogglePanel => {
                self.layout = match self.layout {
                    SqlQueryLayout::ResultsOnly => SqlQueryLayout::Split,
                    _ => SqlQueryLayout::ResultsOnly,
                };
                cx.notify();
                true
            }

            // History modal commands
            Command::ToggleHistoryDropdown => {
                let is_open = self.history_modal.read(cx).is_visible();
                if is_open {
                    self.history_modal.update(cx, |modal, cx| modal.close(cx));
                } else {
                    self.history_modal
                        .update(cx, |modal, cx| modal.open(window, cx));
                }
                true
            }
            Command::OpenSavedQueries => {
                self.history_modal
                    .update(cx, |modal, cx| modal.open_saved_tab(window, cx));
                true
            }
            Command::SaveQuery => {
                let sql = self.input_state.read(cx).value().to_string();
                if sql.trim().is_empty() {
                    cx.toast_warning("Enter a query to save", window);
                } else {
                    self.history_modal
                        .update(cx, |modal, cx| modal.open_save(sql, window, cx));
                }
                true
            }

            _ => false,
        }
    }
}

impl EventEmitter<DocumentEvent> for SqlQueryDocument {}
