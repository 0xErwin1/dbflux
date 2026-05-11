use super::data_grid_panel::{DataGridEvent, DataGridPanel};
use super::handle::DocumentEvent;
use super::task_runner::DocumentTaskRunner;
use super::types::{DocumentId, DocumentState};
use crate::app::{AppStateChanged, AppStateEntity};
use crate::keymap::{Command, ContextId};
use crate::ui::components::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::ui::components::multi_select::{MultiSelect, MultiSelectChanged};
use crate::ui::components::toast::{Toast, copy_action, now_hms};
use crate::ui::icons::AppIcon;
use crate::ui::overlays::history_modal::{HistoryModal, HistoryModalClosed, HistoryQuerySelected};
use crate::ui::overlays::modals::schema_drift::{
    ModalSchemaDrift, SchemaDriftContinue, SchemaDriftDismissed, SchemaDriftRefresh,
};
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_components::controls::{
    CompletionProvider, GpuiInput as Input, InputEvent, InputPosition, InputState, Rope,
};
use dbflux_core::observability::actions as audit_actions;
use dbflux_core::observability::{
    AuditAction, AuditContext, EventActorType, EventCategory, EventOrigin, EventOutcome,
    EventRecord, EventSeverity, EventSourceId,
};
use dbflux_core::{
    DangerousAction, DangerousQueryKind, DbError, DiagnosticSeverity as CoreDiagnosticSeverity,
    DriftOutcome, DriverCapabilities, EditorDiagnostic as CoreEditorDiagnostic, ExecutionContext,
    ExecutionSourceContext, HistoryEntry, OutputReceiver, QueryLanguage, QueryRequest, QueryResult,
    RefreshPolicy, SchemaDriftDetected, SchemaLoadingStrategy, TaskTarget, ValidationResult,
    check_schema_drift, detect_dangerous_query,
};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::highlighter::{
    Diagnostic as InputDiagnostic, DiagnosticSeverity as InputDiagnosticSeverity,
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
mod live_output;
mod render;

use completion::QueryCompletionProvider;
use live_output::LiveOutputState;

/// A single result tab within the CodeDocument.
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
enum ContextBarSlot {
    #[default]
    Connection,
    Database,
    Schema,
    SourceQueryMode,
    SourceTargets,
    SourceStart,
    SourceEnd,
}

/// Counts lines added and removed between two text strings using a set-based
/// line delta. Lines in `current` not in `original` are "added"; lines in
/// `original` not in `current` are "removed". Reorderings are counted as both
/// an add and a remove — good enough for a change-summary label.
pub(crate) fn diff_stats_from_pair(original: &str, current: &str) -> (usize, usize) {
    if original == current {
        return (0, 0);
    }

    let original_lines: std::collections::HashSet<&str> = original.lines().collect();
    let current_lines: std::collections::HashSet<&str> = current.lines().collect();

    let added = current_lines.difference(&original_lines).count();
    let removed = original_lines.difference(&current_lines).count();

    (added, removed)
}

fn build_source_window_context(
    query_mode: Option<String>,
    targets: &[String],
    start_ms: Option<i64>,
    end_ms: Option<i64>,
) -> Result<ExecutionSourceContext, &'static str> {
    let query_mode = query_mode.filter(|value| !value.trim().is_empty());
    let requires_targets = query_mode.as_deref() != Some("sql");

    if requires_targets && targets.is_empty() {
        return Err("Select at least one source");
    }

    let Some(start_ms) = start_ms else {
        return Err("Start time is required");
    };

    let Some(end_ms) = end_ms else {
        return Err("End time is required");
    };

    if start_ms > end_ms {
        return Err("Start time must be earlier than end time");
    }

    Ok(ExecutionSourceContext::CollectionWindow {
        targets: targets.to_vec(),
        start_ms,
        end_ms,
        query_mode,
    })
}

fn format_source_datetime_input(timestamp_ms: i64) -> String {
    dbflux_core::chrono::DateTime::from_timestamp_millis(timestamp_ms)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_default()
}

fn source_input_values_from_context(source: &ExecutionSourceContext) -> Option<(String, String)> {
    match source {
        ExecutionSourceContext::CollectionWindow {
            start_ms, end_ms, ..
        } => Some((
            format_source_datetime_input(*start_ms),
            format_source_datetime_input(*end_ms),
        )),
    }
}

fn query_request_for_execution(
    query: String,
    active_database: Option<String>,
    exec_ctx: &ExecutionContext,
) -> QueryRequest {
    QueryRequest::new(query)
        .with_database(active_database)
        .with_execution_context(Some(exec_ctx.clone()))
}

pub struct CodeDocument {
    // Identity
    id: DocumentId,
    title: String,
    state: DocumentState,
    connection_id: Option<Uuid>,

    // Dependencies
    app_state: Entity<AppStateEntity>,

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
    source_query_mode_dropdown: Entity<Dropdown>,
    source_targets: Entity<MultiSelect>,
    source_start_input: Entity<InputState>,
    source_end_input: Entity<InputState>,
    pending_source_input_values: Option<(String, String)>,
    _context_subscriptions: Vec<Subscription>,

    // Execution
    execution_history: Vec<ExecutionRecord>,
    active_execution_index: Option<usize>,
    pending_result: Option<PendingQueryResult>,
    live_output: Option<LiveOutputState>,
    _live_output_drain: Option<Task<()>>,
    active_query_task: Option<ActiveQueryTask>,

    // Result tabs
    result_tabs: Vec<ResultTab>,
    active_result_index: Option<usize>,
    result_tab_counter: usize,
    run_in_new_tab: bool,

    // History modal
    history_modal: Entity<HistoryModal>,
    _history_subscriptions: Vec<Subscription>,
    pending_set_query: Option<HistoryQuerySelected>,
    pending_history_focus_restore: bool,

    // Layout/focus
    layout: SqlQueryLayout,
    focus_handle: FocusHandle,
    focus_mode: SqlQueryFocus,
    context_bar_slot: ContextBarSlot,
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

    // Schema drift detection
    schema_drift_modal: Entity<ModalSchemaDrift>,
    _schema_drift_subscriptions: Vec<Subscription>,
    /// Query paused while the drift preflight background task is running or
    /// while the user is responding to the drift modal.
    pending_drift_query: Option<PendingDriftQuery>,
    /// True while the drift preflight I/O task is in flight.
    drift_preflight_running: bool,

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
    /// Whether this execution is a script (vs a database query).
    /// Determines the audit event category and whether connection context is required.
    is_script: bool,
}

struct ActiveQueryTask {
    task_id: dbflux_core::TaskId,
    target: TaskTarget,
}

/// Pending dangerous query confirmation.
struct PendingDangerousQuery {
    query: String,
    kind: DangerousQueryKind,
    in_new_tab: bool,
}

/// Action resolved by the schema-drift modal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DriftAction {
    /// Waiting for user response — do not execute yet.
    Pending,
    /// No drift (or driver doesn't support parsing) — execute immediately and
    /// apply transparent cache refreshes first.
    ExecuteNow,
    /// User chose "Continue with stale schema" — proceed without touching the cache.
    ContinueStale,
}

/// A query paused by the schema-drift preflight or drift modal awaiting execution.
struct PendingDriftQuery {
    query: String,
    in_new_tab: bool,
    action: DriftAction,
    /// Cache updates to apply before execution when action is `ExecuteNow` or
    /// after "Refresh & re-run". Each entry is `(database, table, TableInfo)`.
    cache_updates: Vec<(String, String, dbflux_core::TableInfo)>,
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
    /// Whether this execution is a script (vs a database query).
    /// Used to determine audit event category on cancellation.
    pub is_script: bool,
}

impl CodeDocument {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
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
        app_state: Entity<AppStateEntity>,
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
        let supports_connection_context = query_language.supports_connection_context();

        input_state.update(cx, |state, _cx| {
            state.lsp.completion_provider =
                supports_connection_context.then_some(completion_provider.clone());
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

        let history_closed_sub =
            cx.subscribe(&history_modal, |this, _, _: &HistoryModalClosed, cx| {
                this.pending_history_focus_restore = true;
                cx.notify();
            });

        // Create schema drift modal and wire up action subscriptions.
        let schema_drift_modal = cx.new(ModalSchemaDrift::new);

        let drift_refresh_sub = cx.subscribe(
            &schema_drift_modal,
            |this, _, _event: &SchemaDriftRefresh, cx| {
                this.on_schema_drift_refresh(cx);
            },
        );

        let drift_continue_sub = cx.subscribe(
            &schema_drift_modal,
            |this, _, _event: &SchemaDriftContinue, cx| {
                this.on_schema_drift_continue(cx);
            },
        );

        let drift_dismissed_sub = cx.subscribe(
            &schema_drift_modal,
            |this, _, _event: &SchemaDriftDismissed, cx| {
                this.pending_drift_query = None;
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
            .effective_settings_for_connection(connection_id)
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
            |this, _, event: &DropdownSelectionChanged, _window, cx| {
                let policy = RefreshPolicy::from_index(event.index);

                if policy.is_auto() && !this.can_auto_refresh(cx) {
                    this.refresh_dropdown.update(cx, |dd, cx| {
                        dd.set_selected_index(Some(RefreshPolicy::Manual.index()), cx);
                    });
                    Toast::warning("Auto-refresh blocked: query modifies data")
                        .meta_right(now_hms())
                        .push(cx);
                    return;
                }

                this.set_refresh_policy(policy, cx);
            },
        );

        let doc_id = DocumentId::new();

        let scratch_path = Some(
            app_state
                .read(cx)
                .scratch_path(&doc_id.0.to_string(), query_language.default_extension()),
        );

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
        let source_query_mode_dropdown = cx.new(|_cx| {
            Dropdown::new("ctx-source-query-mode")
                .placeholder("Syntax")
                .toolbar_style(true)
        });
        let source_targets =
            cx.new(|_cx| MultiSelect::new("ctx-source-targets").placeholder("Sources"));
        let source_start_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("2026-04-24T00:00:00Z"));
        let source_end_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("2026-04-24T01:00:00Z"));
        let source_query_mode_sub = cx.subscribe_in(
            &source_query_mode_dropdown,
            window,
            |this, _, event: &DropdownSelectionChanged, _window, cx| {
                this.on_source_query_mode_changed(&event.item, cx);
            },
        );
        let source_targets_sub = cx.subscribe(
            &source_targets,
            |this, entity, _event: &MultiSelectChanged, cx| {
                let selected_targets = entity
                    .read(cx)
                    .selected_values()
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>();

                this.on_source_targets_changed(selected_targets, cx);
            },
        );
        let source_start_sub = cx.subscribe_in(
            &source_start_input,
            window,
            |this, _input, event: &InputEvent, _window, cx| {
                if matches!(event, InputEvent::Change) {
                    this.on_source_time_range_changed(cx);
                }
            },
        );
        let source_end_sub = cx.subscribe_in(
            &source_end_input,
            window,
            |this, _input, event: &InputEvent, _window, cx| {
                if matches!(event, InputEvent::Change) {
                    this.on_source_time_range_changed(cx);
                }
            },
        );
        let app_state_sub = cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
            this.sync_context_dropdowns(cx);
        });

        let refresh_policy = default_refresh;

        let mut document = Self {
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
            source_query_mode_dropdown,
            source_targets,
            source_start_input,
            source_end_input,
            pending_source_input_values: None,
            _context_subscriptions: vec![
                conn_sub,
                db_sub,
                schema_sub,
                source_query_mode_sub,
                source_targets_sub,
                source_start_sub,
                source_end_sub,
                app_state_sub,
            ],
            execution_history: Vec::new(),
            active_execution_index: None,
            pending_result: None,
            live_output: None,
            _live_output_drain: None,
            active_query_task: None,
            result_tabs: Vec::new(),
            active_result_index: None,
            result_tab_counter: 0,
            run_in_new_tab: false,
            history_modal,
            _history_subscriptions: vec![query_selected_sub, history_closed_sub],
            pending_set_query: None,
            pending_history_focus_restore: false,
            layout: SqlQueryLayout::EditorOnly,
            focus_handle: cx.focus_handle(),
            focus_mode: SqlQueryFocus::Editor,
            context_bar_slot: ContextBarSlot::Connection,
            results_maximized: false,
            runner,
            refresh_policy,
            refresh_dropdown,
            pending_auto_refresh: false,
            _refresh_timer: None,
            _refresh_subscriptions: vec![refresh_policy_sub],
            is_active_tab: true,
            pending_dangerous_query: None,
            schema_drift_modal,
            _schema_drift_subscriptions: vec![
                drift_refresh_sub,
                drift_continue_sub,
                drift_dismissed_sub,
            ],
            pending_drift_query: None,
            drift_preflight_running: false,
            diagnostic_request_id: 0,
            _diagnostic_debounce: None,
            _pending_save: None,
            scratch_path,
            shadow_path: None,
            _auto_save_debounce: None,
            show_saved_label: false,
            _saved_label_timer: None,
            pending_error: None,
        };

        document.sync_context_dropdowns(cx);
        document
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
    pub fn with_exec_ctx(mut self, ctx: ExecutionContext, cx: &mut Context<Self>) -> Self {
        self.pending_source_input_values = ctx
            .source
            .as_ref()
            .and_then(source_input_values_from_context);
        self.connection_id = ctx.connection_id;
        self.exec_ctx = ctx;
        self.sync_context_dropdowns(cx);
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
                self.shadow_path =
                    Some(self.app_state.read(cx).shadow_path(&self.id.0.to_string()));
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

            if let Some(shadow) = self.shadow_path.take() {
                let _ = std::fs::remove_file(&shadow);
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

    /// Counts lines added and removed relative to `original_content`.
    pub fn diff_stats(&self, cx: &App) -> (usize, usize) {
        let current = self.input_state.read(cx).value().to_string();
        diff_stats_from_pair(&self.original_content, &current)
    }

    /// Short summary of pending edits for the dirty-dot tooltip.
    ///
    /// Returns `None` when the document has no unsaved changes.
    pub fn change_summary(&self, cx: &App) -> Option<String> {
        let (added, removed) = self.diff_stats(cx);

        if added == 0 && removed == 0 {
            None
        } else {
            Some(format!("+{}/−{} lines", added, removed))
        }
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
                self.input_state
                    .update(cx, |state, cx| state.focus(window, cx));
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
            Command::Cancel | Command::CancelQuery if self.runner.is_primary_active() => {
                self.cancel_query(cx);
                true
            }
            Command::Cancel | Command::CancelQuery => false,

            Command::FocusUp if self.focus_mode == SqlQueryFocus::Editor => {
                self.enter_context_bar(window, cx);
                true
            }

            Command::FocusDown
                if self.focus_mode == SqlQueryFocus::Editor && !self.result_tabs.is_empty() =>
            {
                self.focus_mode = SqlQueryFocus::Results;
                if let Some(grid) = self.active_result_grid() {
                    grid.update(cx, |g, cx| g.focus_active_view(window, cx));
                }
                cx.notify();
                true
            }
            Command::FocusDown => false,

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

    /// Emits an audit event for a query or script execution.
    #[allow(clippy::too_many_arguments)]
    fn emit_audit_event(
        &self,
        cx: &mut Context<Self>,
        category: EventCategory,
        action: AuditAction,
        outcome: EventOutcome,
        summary: String,
        query: Option<&str>,
        duration_ms: Option<i64>,
        error: Option<&str>,
    ) {
        // Scripts don't require a connection context
        let (conn_id, database_name, driver_id) = if category == EventCategory::Script {
            (None, None, None)
        } else {
            let Some(conn_id) = self.connection_id else {
                // For non-script queries, require connection context
                return;
            };
            let (database_name, driver_id) = self
                .app_state
                .read(cx)
                .connections()
                .get(&conn_id)
                .map(|c| {
                    let db = self.exec_ctx.database.clone().or(c.active_database.clone());
                    (Some(db.unwrap_or_default()), Some(c.profile.driver_id()))
                })
                .unwrap_or((None, None));
            (Some(conn_id), database_name, driver_id)
        };

        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let severity = match outcome {
            EventOutcome::Success => EventSeverity::Info,
            EventOutcome::Failure => EventSeverity::Error,
            EventOutcome::Cancelled => EventSeverity::Warn,
            EventOutcome::Pending => EventSeverity::Debug,
        };

        let mut event = EventRecord::new(ts_ms, severity, category, outcome)
            .with_typed_action(action)
            .with_summary(&summary);

        if let Some(conn_id) = conn_id
            && let (Some(db), Some(driver)) = (database_name, driver_id)
        {
            event = event.with_connection_context(conn_id.to_string(), db, driver);
        }

        if category == EventCategory::Script {
            event = event.with_origin(EventOrigin::script());
        } else {
            event = event.with_origin(EventOrigin::local());
        }

        if let Some(query) = query {
            event.details_json = Some(serde_json::json!({ "query": query }).to_string());
        }

        if let Some(duration) = duration_ms {
            event.duration_ms = Some(duration);
        }

        if let Some(error) = error {
            event.error_message = Some(error.to_string());
        }

        if let Err(e) = self.app_state.read(cx).audit_service().record(event) {
            log::warn!("Failed to emit audit event: {}", e);
        }
    }

    /// Emits an audit event for a dangerous query confirmation.
    fn emit_dangerous_query_audit_event(&self, cx: &mut Context<Self>, kind: DangerousQueryKind) {
        let Some(conn_id) = self.connection_id else {
            return;
        };

        let (database_name, driver_id) = self
            .app_state
            .read(cx)
            .connections()
            .get(&conn_id)
            .map(|c| {
                let db = self.exec_ctx.database.clone().or(c.active_database.clone());
                (db.unwrap_or_default(), c.profile.driver_id())
            })
            .unwrap_or_default();

        let summary = format!("Dangerous query confirmed: {}", kind.message());
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let event = EventRecord::new(
            ts_ms,
            EventSeverity::Warn,
            EventCategory::Query,
            EventOutcome::Success,
        )
        .with_typed_action(audit_actions::DANGEROUS_QUERY_CONFIRMED)
        .with_summary(&summary)
        .with_connection_context(conn_id.to_string(), database_name, driver_id);

        let mut e = event;
        e = e.with_origin(EventOrigin::local());
        e.details_json = Some(serde_json::json!({ "dangerous_kind": kind.message() }).to_string());

        if let Err(err) = self.app_state.read(cx).audit_service().record(e) {
            log::warn!("Failed to emit dangerous query audit event: {}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{diff_stats_from_pair, source_input_values_from_context};
    use dbflux_core::ExecutionSourceContext;

    #[test]
    fn diff_stats_identical_text_returns_zero() {
        let (added, removed) = diff_stats_from_pair("SELECT 1", "SELECT 1");
        assert_eq!(added, 0);
        assert_eq!(removed, 0);
    }

    #[test]
    fn diff_stats_pure_addition() {
        let original = "SELECT 1";
        let current = "SELECT 1\nSELECT 2\nSELECT 3";
        let (added, removed) = diff_stats_from_pair(original, current);
        assert_eq!(added, 2);
        assert_eq!(removed, 0);
    }

    #[test]
    fn diff_stats_pure_removal() {
        let original = "SELECT 1\nSELECT 2\nSELECT 3";
        let current = "SELECT 1";
        let (added, removed) = diff_stats_from_pair(original, current);
        assert_eq!(added, 0);
        assert_eq!(removed, 2);
    }

    #[test]
    fn diff_stats_mixed_edits() {
        let original = "SELECT a\nSELECT b\nSELECT c";
        let current = "SELECT a\nSELECT x\nSELECT y";
        let (added, removed) = diff_stats_from_pair(original, current);
        assert_eq!(added, 2);
        assert_eq!(removed, 2);
    }

    #[test]
    fn source_input_values_restore_start_and_end_strings() {
        let values = source_input_values_from_context(&ExecutionSourceContext::CollectionWindow {
            targets: vec!["/aws/lambda/app".to_string()],
            start_ms: 1_704_067_200_000,
            end_ms: 1_704_070_800_000,
            query_mode: Some("cwli".to_string()),
        })
        .expect("source input values");

        assert_eq!(values.0, "2024-01-01T00:00:00Z");
        assert_eq!(values.1, "2024-01-01T01:00:00Z");
    }
}

impl EventEmitter<DocumentEvent> for CodeDocument {}
