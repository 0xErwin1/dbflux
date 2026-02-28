use super::data_grid_panel::{DataGridEvent, DataGridPanel};
use super::handle::DocumentEvent;
use super::task_runner::DocumentTaskRunner;
use super::types::{DocumentId, DocumentState};
use crate::app::{AppState, AppStateChanged};
use crate::keymap::{Command, ContextId};
use crate::ui::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::ui::history_modal::{HistoryModal, HistoryQuerySelected};
use crate::ui::icons::AppIcon;
use crate::ui::toast::ToastExt;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{
    DangerousAction, DangerousQueryKind, DbError, DiagnosticSeverity as CoreDiagnosticSeverity,
    DriverCapabilities, EditorDiagnostic as CoreEditorDiagnostic, ExecutionContext, HistoryEntry,
    QueryLanguage, QueryRequest, QueryResult, RefreshPolicy, SchemaLoadingStrategy,
    ValidationResult, detect_dangerous_query,
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
    CompletionContext, CompletionItem, CompletionItemKind, CompletionResponse, CompletionTextEdit,
    InsertTextFormat, Position as LspPosition, Range as LspRange, TextEdit,
};
use std::cmp::min;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

mod completion;
mod context_bar;
mod diagnostics;
mod execution;
mod file_ops;
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
    ContextBar,
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

    // File backing
    path: Option<PathBuf>,
    is_dirty: bool,
    suppress_dirty: bool,
    query_language: QueryLanguage,

    // Execution context (per-document, independent of global connection)
    exec_ctx: ExecutionContext,
    connection_dropdown: Entity<Dropdown>,
    database_dropdown: Entity<Dropdown>,
    schema_dropdown: Entity<Dropdown>,
    _context_subscriptions: Vec<Subscription>,

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
    context_bar_index: usize,
    results_maximized: bool,

    // Task runner (query execution)
    runner: DocumentTaskRunner,
    refresh_policy: RefreshPolicy,
    refresh_dropdown: Entity<Dropdown>,
    pending_auto_refresh: bool,
    _refresh_timer: Option<Task<()>>,
    _refresh_subscriptions: Vec<Subscription>,

    is_active_tab: bool,

    // Dangerous query confirmation
    pending_dangerous_query: Option<PendingDangerousQuery>,

    // Diagnostic debounce: incremental request id to discard stale results.
    diagnostic_request_id: u64,
    _diagnostic_debounce: Option<Task<()>>,

    // Pending file I/O
    _pending_save: Option<Task<()>>,

    // Session persistence (auto-save to disk)
    scratch_path: Option<PathBuf>,
    shadow_path: Option<PathBuf>,
    _auto_save_debounce: Option<Task<()>>,
    show_saved_label: bool,
    _saved_label_timer: Option<Task<()>>,

    // Pending error to show as toast (set from async context without window access)
    pending_error: Option<String>,
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
            .map(|conn| conn.connection.metadata().query_language.clone())
            .unwrap_or(QueryLanguage::Sql);

        Self::new_with_language(app_state, connection_id, query_language, window, cx)
    }

    /// Create a document with an explicit language (used when opening files).
    pub fn new_with_language(
        app_state: Entity<AppState>,
        connection_id: Option<Uuid>,
        query_language: QueryLanguage,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
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
            QueryCompletionProvider::new(query_language.clone(), app_state.clone(), connection_id),
        );
        input_state.update(cx, |state, _cx| {
            state.lsp.completion_provider = Some(completion_provider);
        });

        let input_change_sub = cx.subscribe_in(
            &input_state,
            window,
            |this, _input, event: &InputEvent, _window, cx| match event {
                InputEvent::Change => {
                    if this.suppress_dirty {
                        this.suppress_dirty = false;
                    } else {
                        this.mark_dirty(cx);
                    }
                    this.schedule_auto_save(cx);
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

        let default_refresh = app_state
            .read(cx)
            .general_settings()
            .resolve_refresh_policy();

        let refresh_dropdown = cx.new(|_cx| {
            let items = RefreshPolicy::ALL
                .iter()
                .map(|policy| DropdownItem::new(policy.label()))
                .collect();

            Dropdown::new("sql-auto-refresh")
                .items(items)
                .selected_index(Some(default_refresh.index()))
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

        let doc_id = DocumentId::new();

        let scratch_path = app_state.read(cx).session_store().map(|store| {
            store.scratch_path(&doc_id.0.to_string(), query_language.default_extension())
        });

        let initial_database = connection_id.and_then(|id| {
            let connections = app_state.read(cx).connections();
            let connected = connections.get(&id)?;

            connected.active_database.clone().or_else(|| {
                connected
                    .schema
                    .as_ref()
                    .and_then(|s| s.current_database().map(String::from))
            })
        });

        let mut exec_ctx = ExecutionContext {
            connection_id,
            database: initial_database,
            ..Default::default()
        };

        // Pre-select "public" schema when available (PostgreSQL default).
        let schema_items = Self::schema_items_for_connection(&app_state, &exec_ctx, cx);
        if schema_items
            .iter()
            .any(|item| item.value.as_ref() == "public")
        {
            exec_ctx.schema = Some("public".to_string());
        }

        let (connection_dropdown, conn_sub) =
            Self::create_connection_dropdown(&app_state, &exec_ctx, window, cx);
        let (database_dropdown, db_sub) =
            Self::create_database_dropdown(&app_state, &exec_ctx, window, cx);
        let (schema_dropdown, schema_sub) =
            Self::create_schema_dropdown(&app_state, &exec_ctx, window, cx);

        let refresh_policy = app_state
            .read(cx)
            .general_settings()
            .resolve_refresh_policy();

        Self {
            id: doc_id,
            title: "Query 1".to_string(),
            state: DocumentState::Clean,
            connection_id,
            app_state,
            input_state,
            _input_subscriptions: vec![input_change_sub],
            original_content: String::new(),
            saved_query_id: None,
            path: None,
            is_dirty: false,
            suppress_dirty: false,
            query_language,
            exec_ctx,
            connection_dropdown,
            database_dropdown,
            schema_dropdown,
            _context_subscriptions: vec![conn_sub, db_sub, schema_sub],
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
            context_bar_index: 0,
            results_maximized: false,
            runner,
            refresh_policy,
            refresh_dropdown,
            pending_auto_refresh: false,
            _refresh_timer: None,
            _refresh_subscriptions: vec![refresh_policy_sub],
            is_active_tab: true,
            pending_dangerous_query: None,
            diagnostic_request_id: 0,
            _diagnostic_debounce: None,
            _pending_save: None,
            scratch_path,
            shadow_path: None,
            _auto_save_debounce: None,
            show_saved_label: false,
            _saved_label_timer: None,
            pending_error: None,
        }
    }

    pub fn can_auto_refresh(&self, cx: &App) -> bool {
        dbflux_core::is_safe_read_query(&self.input_state.read(cx).value())
    }

    pub fn set_active_tab(&mut self, active: bool) {
        self.is_active_tab = active;
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

                        let settings = doc.app_state.read(cx).general_settings();

                        if settings.auto_refresh_pause_on_error && doc.state == DocumentState::Error
                        {
                            return;
                        }

                        if settings.auto_refresh_only_if_visible && !doc.is_active_tab {
                            return;
                        }

                        doc.pending_auto_refresh = true;
                        cx.notify();
                    });
                });
            }
        }));
    }

    /// Sets the document content (without marking dirty).
    pub fn set_content(&mut self, sql: &str, window: &mut Window, cx: &mut Context<Self>) {
        let sql_owned = sql.to_string();
        self.suppress_dirty = true;
        self.input_state
            .update(cx, |state, cx| state.set_value(&sql_owned, window, cx));
        self.original_content = sql_owned;
        self.is_dirty = false;
        self.refresh_editor_diagnostics(window, cx);
    }

    /// Creates document with specific title.
    pub fn with_title(mut self, title: String) -> Self {
        self.title = title;
        self
    }

    /// Attach a file path (used after opening or "Save As").
    pub fn with_path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    /// Set the execution context (e.g. parsed from file header).
    pub fn with_exec_ctx(mut self, ctx: ExecutionContext) -> Self {
        if let Some(conn_id) = ctx.connection_id {
            self.connection_id = Some(conn_id);
            self.exec_ctx = ctx;
        }
        self
    }

    // === File backing ===

    pub fn path(&self) -> Option<&PathBuf> {
        self.path.as_ref()
    }

    pub fn is_file_backed(&self) -> bool {
        self.path.is_some()
    }

    #[allow(dead_code)]
    pub fn query_language(&self) -> QueryLanguage {
        self.query_language.clone()
    }

    /// Returns true if the editor content is empty or whitespace-only.
    pub fn is_content_empty(&self, cx: &App) -> bool {
        self.input_state.read(cx).value().trim().is_empty()
    }

    fn mark_dirty(&mut self, cx: &mut Context<Self>) {
        if !self.is_dirty {
            self.is_dirty = true;

            if self.is_file_backed() && self.shadow_path.is_none() {
                self.shadow_path = self
                    .app_state
                    .read(cx)
                    .session_store()
                    .map(|store| store.shadow_path(&self.id.0.to_string()));
            }

            cx.emit(DocumentEvent::MetaChanged);
            cx.notify();
        }
    }

    fn mark_clean(&mut self, cx: &mut Context<Self>) {
        if self.is_dirty {
            self.is_dirty = false;
            self.original_content = self.input_state.read(cx).value().to_string();
            self._auto_save_debounce = None;

            if let Some(shadow) = self.shadow_path.take()
                && let Some(store) = self.app_state.read(cx).session_store()
            {
                store.delete(&shadow);
            }

            cx.emit(DocumentEvent::MetaChanged);
            cx.notify();
        }
    }

    // === Accessors for DocumentHandle ===

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        if let Some(path) = &self.path {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("Untitled");

            if self.is_dirty {
                format!("{}*", name)
            } else {
                name.to_string()
            }
        } else {
            self.title.clone()
        }
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

    #[allow(dead_code)]
    pub fn exec_ctx(&self) -> &ExecutionContext {
        &self.exec_ctx
    }

    pub fn scratch_path(&self) -> Option<&PathBuf> {
        self.scratch_path.as_ref()
    }

    pub fn shadow_path(&self) -> Option<&PathBuf> {
        self.shadow_path.as_ref()
    }

    /// Override session paths (used during session restore).
    pub fn set_session_paths(&mut self, scratch: Option<PathBuf>, shadow: Option<PathBuf>) {
        self.scratch_path = scratch;
        self.shadow_path = shadow;
    }

    /// Mark the document as dirty without assigning a new shadow path.
    /// Used during session restore when we already have the shadow from the manifest.
    pub fn restore_dirty(&mut self, cx: &mut Context<Self>) {
        if !self.is_dirty {
            self.is_dirty = true;
            cx.emit(DocumentEvent::MetaChanged);
            cx.notify();
        }
    }

    pub fn can_close(&self, cx: &App) -> bool {
        !self.has_unsaved_changes(cx)
    }

    pub fn has_unsaved_changes(&self, cx: &App) -> bool {
        if self.is_file_backed() {
            return self.is_dirty;
        }

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

        if self.focus_mode == SqlQueryFocus::ContextBar
            && self.dispatch_context_bar_command(cmd, window, cx)
        {
            return true;
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

            Command::FocusUp => {
                if self.focus_mode == SqlQueryFocus::Editor {
                    self.enter_context_bar(window, cx);
                    return true;
                }
                false
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
                if self.is_file_backed() {
                    self.save_file(window, cx);
                } else {
                    self.save_file_as(window, cx);
                }
                true
            }

            Command::SaveFileAs => {
                self.save_file_as(window, cx);
                true
            }

            _ => false,
        }
    }
}

impl EventEmitter<DocumentEvent> for SqlQueryDocument {}
