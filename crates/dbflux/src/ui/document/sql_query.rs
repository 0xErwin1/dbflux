#![allow(dead_code)]

use super::data_grid_panel::{DataGridEvent, DataGridPanel};
use super::handle::DocumentEvent;
use super::types::{DocumentId, DocumentState};
use crate::app::AppState;
use crate::keymap::{Command, ContextId};
use crate::ui::history_modal::{HistoryModal, HistoryQuerySelected};
use crate::ui::icons::AppIcon;
use crate::ui::toast::ToastExt;
use crate::ui::tokens::{Radii, Spacing};
use dbflux_core::{CancelToken, DbError, HistoryEntry, QueryRequest, QueryResult};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::{Input, InputState};
use gpui_component::resizable::{resizable_panel, v_resizable};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Internal layout of the document.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum SqlQueryLayout {
    #[default]
    Split,
    EditorOnly,
    ResultsOnly,
}

/// Where focus is within the document.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
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
    original_content: String,
    saved_query_id: Option<Uuid>,

    // Execution
    execution_history: Vec<ExecutionRecord>,
    active_execution_index: Option<usize>,
    data_grid: Option<Entity<DataGridPanel>>,
    _data_grid_subscription: Option<Subscription>,
    pending_result: Option<PendingQueryResult>,

    // History modal
    history_modal: Entity<HistoryModal>,
    _history_subscriptions: Vec<Subscription>,
    pending_set_query: Option<HistoryQuerySelected>,

    // Layout/focus
    layout: SqlQueryLayout,
    focus_handle: FocusHandle,
    focus_mode: SqlQueryFocus,
    active_cancel_token: Option<CancelToken>,
}

struct PendingQueryResult {
    exec_id: Uuid,
    query: String,
    result: Result<QueryResult, DbError>,
}

/// Record of a query execution.
#[derive(Clone)]
pub struct ExecutionRecord {
    pub id: Uuid,
    pub query: String,
    pub started_at: Instant,
    pub finished_at: Option<Instant>,
    pub result: Option<Arc<QueryResult>>,
    pub error: Option<String>,
    pub rows_affected: Option<u64>,
}

impl SqlQueryDocument {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor("sql")
                .line_number(true)
                .soft_wrap(false)
                .placeholder("-- Enter SQL here...")
        });

        let connection_id = app_state.read(cx).active_connection_id;

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

        Self {
            id: DocumentId::new(),
            title: "Query 1".to_string(),
            state: DocumentState::Clean,
            connection_id,
            app_state,
            input_state,
            original_content: String::new(),
            saved_query_id: None,
            execution_history: Vec::new(),
            active_execution_index: None,
            data_grid: None,
            _data_grid_subscription: None,
            pending_result: None,
            history_modal,
            _history_subscriptions: vec![query_selected_sub],
            pending_set_query: None,
            layout: SqlQueryLayout::EditorOnly,
            focus_handle: cx.focus_handle(),
            focus_mode: SqlQueryFocus::Editor,
            active_cancel_token: None,
        }
    }

    /// Sets the document content.
    pub fn set_content(&mut self, sql: &str, window: &mut Window, cx: &mut Context<Self>) {
        let sql_owned = sql.to_string();
        self.input_state
            .update(cx, |state, cx| state.set_value(&sql_owned, window, cx));
        self.original_content = sql_owned;
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

    pub fn connection_id(&self) -> Option<Uuid> {
        self.connection_id
    }

    pub fn can_close(&self) -> bool {
        // TODO: check for unsaved changes
        true
    }

    pub fn focus(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
    }

    /// Returns the active context for keyboard handling based on internal focus.
    pub fn active_context(&self, cx: &App) -> ContextId {
        // Check if history modal is open
        if self.history_modal.read(cx).is_visible() {
            return ContextId::HistoryModal;
        }

        match self.focus_mode {
            SqlQueryFocus::Editor => ContextId::Editor,
            SqlQueryFocus::Results => ContextId::Results,
        }
    }

    // === Query Execution ===

    pub fn run_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let query = self.input_state.read(cx).value().to_string();
        if query.trim().is_empty() {
            cx.toast_warning("Enter a query to run", window);
            return;
        }

        let Some(conn_id) = self.connection_id else {
            cx.toast_error("No active connection", window);
            return;
        };

        let connection = self
            .app_state
            .read(cx)
            .connections
            .get(&conn_id)
            .map(|c| c.connection.clone());

        let Some(connection) = connection else {
            cx.toast_error("Connection not found", window);
            return;
        };

        // Create cancel token for this execution
        let cancel_token = CancelToken::new();
        self.active_cancel_token = Some(cancel_token.clone());

        // Create execution record
        let exec_id = Uuid::new_v4();
        let record = ExecutionRecord {
            id: exec_id,
            query: query.clone(),
            started_at: Instant::now(),
            finished_at: None,
            result: None,
            error: None,
            rows_affected: None,
        };
        self.execution_history.push(record);
        self.active_execution_index = Some(self.execution_history.len() - 1);

        // Change state
        self.state = DocumentState::Executing;
        cx.emit(DocumentEvent::ExecutionStarted);
        cx.notify();

        // Get active database for MySQL/MariaDB
        let active_database = self
            .app_state
            .read(cx)
            .connections
            .get(&conn_id)
            .and_then(|c| c.active_database.clone());

        // Execute in background
        let request = QueryRequest::new(query.clone()).with_database(active_database);

        let task = cx.background_executor().spawn({
            let connection = connection.clone();
            async move { connection.execute(&request) }
        });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            cx.update(|cx| {
                let _ = this.update(cx, |doc, cx| {
                    // Store pending result to be processed in render (where we have window)
                    doc.pending_result = Some(PendingQueryResult {
                        exec_id,
                        query,
                        result,
                    });
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    /// Process pending query selected from history modal (called from render).
    fn process_pending_set_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(selected) = self.pending_set_query.take() else {
            return;
        };

        // Set the query content in the editor
        self.input_state
            .update(cx, |state, cx| state.set_value(&selected.sql, window, cx));

        // Update title if a name was provided
        if let Some(name) = selected.name {
            self.title = name;
        }

        // Track the saved query ID if this came from saved queries
        self.saved_query_id = selected.saved_query_id;

        // Focus back on editor
        self.focus_mode = SqlQueryFocus::Editor;

        cx.emit(DocumentEvent::MetaChanged);
        cx.notify();
    }

    /// Process pending query result (called from render where we have window access).
    fn process_pending_result(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(pending) = self.pending_result.take() else {
            return;
        };

        self.active_cancel_token = None;
        self.state = DocumentState::Clean;

        let Some(record) = self
            .execution_history
            .iter_mut()
            .find(|r| r.id == pending.exec_id)
        else {
            return;
        };

        record.finished_at = Some(Instant::now());

        match pending.result {
            Ok(qr) => {
                let row_count = qr.rows.len();
                let execution_time = qr.execution_time;
                record.rows_affected = Some(row_count as u64);
                let arc_result = Arc::new(qr);
                record.result = Some(arc_result.clone());

                // Add to global history
                let (database, connection_name) = self
                    .connection_id
                    .and_then(|id| self.app_state.read(cx).connections.get(&id))
                    .map(|c| (c.active_database.clone(), Some(c.profile.name.clone())))
                    .unwrap_or((None, None));

                let history_entry = HistoryEntry::new(
                    pending.query.clone(),
                    database,
                    connection_name,
                    execution_time,
                    Some(row_count),
                );
                self.app_state.update(cx, |state, _| {
                    state.add_history_entry(history_entry);
                });

                self.setup_data_grid(arc_result, pending.query, window, cx);

                if self.layout == SqlQueryLayout::EditorOnly {
                    self.layout = SqlQueryLayout::Split;
                }

                self.focus_mode = SqlQueryFocus::Results;
            }
            Err(e) => {
                let error_msg = e.to_string();
                record.error = Some(error_msg);
                self.state = DocumentState::Error;
            }
        }

        cx.emit(DocumentEvent::ExecutionFinished);
        cx.emit(DocumentEvent::MetaChanged);
    }

    fn setup_data_grid(
        &mut self,
        result: Arc<QueryResult>,
        query: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(grid) = &self.data_grid {
            grid.update(cx, |g, cx| g.set_query_result(result, query, cx));
            return;
        }

        let app_state = self.app_state.clone();
        let grid = cx.new(|cx| DataGridPanel::new_for_result(result, query, app_state, window, cx));

        let subscription = cx.subscribe(
            &grid,
            |_this, _grid, event: &DataGridEvent, cx| match event {
                DataGridEvent::PromoteResult { result, query } => {
                    cx.emit(DocumentEvent::PromoteResult {
                        result: result.clone(),
                        query: query.clone(),
                    });
                }
            },
        );

        self.data_grid = Some(grid);
        self._data_grid_subscription = Some(subscription);
    }

    pub fn cancel_query(&mut self, cx: &mut Context<Self>) {
        if let Some(token) = self.active_cancel_token.take() {
            token.cancel();
            self.state = DocumentState::Clean;
            cx.emit(DocumentEvent::MetaChanged);
            cx.notify();
        }
    }

    /// Cycle between layouts.
    pub fn cycle_layout(&mut self, cx: &mut Context<Self>) {
        self.layout = match self.layout {
            SqlQueryLayout::Split => SqlQueryLayout::EditorOnly,
            SqlQueryLayout::EditorOnly => SqlQueryLayout::ResultsOnly,
            SqlQueryLayout::ResultsOnly => SqlQueryLayout::Split,
        };
        cx.notify();
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
        // When history modal is open, route commands to it first
        if self.history_modal.read(cx).is_visible() {
            if self.dispatch_to_history_modal(cmd, window, cx) {
                return true;
            }
        }

        match cmd {
            Command::RunQuery => {
                self.run_query(window, cx);
                true
            }
            Command::Cancel | Command::CancelQuery => {
                if self.active_cancel_token.is_some() {
                    self.cancel_query(cx);
                    true
                } else {
                    false
                }
            }

            // Internal focus navigation
            Command::FocusUp => {
                if self.focus_mode == SqlQueryFocus::Results {
                    self.focus_mode = SqlQueryFocus::Editor;
                    cx.notify();
                    true
                } else {
                    false
                }
            }
            Command::FocusDown => {
                if self.focus_mode == SqlQueryFocus::Editor && self.data_grid.is_some() {
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

            // Results navigation - forward to DataGridPanel when focused on results
            Command::SelectNext => {
                if self.focus_mode == SqlQueryFocus::Results {
                    if let Some(grid) = &self.data_grid {
                        grid.update(cx, |g, cx| g.select_next(cx));
                    }
                    true
                } else {
                    false
                }
            }
            Command::SelectPrev => {
                if self.focus_mode == SqlQueryFocus::Results {
                    if let Some(grid) = &self.data_grid {
                        grid.update(cx, |g, cx| g.select_prev(cx));
                    }
                    true
                } else {
                    false
                }
            }
            Command::SelectFirst => {
                if self.focus_mode == SqlQueryFocus::Results {
                    if let Some(grid) = &self.data_grid {
                        grid.update(cx, |g, cx| g.select_first(cx));
                    }
                    true
                } else {
                    false
                }
            }
            Command::SelectLast => {
                if self.focus_mode == SqlQueryFocus::Results {
                    if let Some(grid) = &self.data_grid {
                        grid.update(cx, |g, cx| g.select_last(cx));
                    }
                    true
                } else {
                    false
                }
            }
            Command::ColumnLeft => {
                if self.focus_mode == SqlQueryFocus::Results {
                    if let Some(grid) = &self.data_grid {
                        grid.update(cx, |g, cx| g.column_left(cx));
                    }
                    true
                } else {
                    false
                }
            }
            Command::ColumnRight => {
                if self.focus_mode == SqlQueryFocus::Results {
                    if let Some(grid) = &self.data_grid {
                        grid.update(cx, |g, cx| g.column_right(cx));
                    }
                    true
                } else {
                    false
                }
            }
            Command::ResultsNextPage => {
                if let Some(grid) = &self.data_grid {
                    grid.update(cx, |g, cx| g.go_to_next_page(window, cx));
                }
                true
            }
            Command::ResultsPrevPage => {
                if let Some(grid) = &self.data_grid {
                    grid.update(cx, |g, cx| g.go_to_prev_page(window, cx));
                }
                true
            }
            Command::ExportResults => {
                if let Some(grid) = &self.data_grid {
                    grid.update(cx, |g, cx| g.export_results(window, cx));
                }
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

    // === Render ===

    fn render_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_executing = self.state == DocumentState::Executing;

        let (run_icon, run_label, run_enabled) = if is_executing {
            (AppIcon::X, "Cancel", true)
        } else {
            (AppIcon::Play, "Run", true)
        };

        let btn_bg = theme.secondary;
        let primary = theme.primary;

        let execution_time = self
            .active_execution_index
            .and_then(|i| self.execution_history.get(i))
            .and_then(|r| {
                r.finished_at
                    .map(|finished| finished.duration_since(r.started_at))
            });

        let has_results = self.data_grid.is_some();
        let results_visible = self.layout != SqlQueryLayout::EditorOnly;

        div()
            .id("sql-toolbar")
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(
                div()
                    .id("run-query-btn")
                    .flex()
                    .items_center()
                    .gap_1()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .text_xs()
                    .when(run_enabled, |el| {
                        el.bg(if is_executing { theme.danger } else { primary })
                            .text_color(theme.background)
                            .hover(|d| d.opacity(0.9))
                    })
                    .when(!run_enabled, |el| {
                        el.bg(btn_bg)
                            .text_color(theme.muted_foreground)
                            .cursor_not_allowed()
                    })
                    .on_click(cx.listener(move |this, _, window, cx| {
                        if this.state == DocumentState::Executing {
                            this.cancel_query(cx);
                        } else {
                            this.run_query(window, cx);
                        }
                    }))
                    .child(
                        svg()
                            .path(run_icon.path())
                            .size_3()
                            .text_color(if run_enabled {
                                theme.background
                            } else {
                                theme.muted_foreground
                            }),
                    )
                    .child(run_label),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child("Ctrl+Enter"),
            )
            .child(div().flex_1())
            .when_some(execution_time, |el, duration| {
                el.child(
                    div()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child(format!("{:.2}s", duration.as_secs_f64())),
                )
            })
            .when(has_results, |el| {
                let icon_color = if results_visible {
                    theme.foreground
                } else {
                    theme.muted_foreground
                };

                el.child(
                    div()
                        .id("toggle-results-btn")
                        .flex()
                        .items_center()
                        .justify_center()
                        .size_6()
                        .rounded(Radii::SM)
                        .cursor_pointer()
                        .hover(|el| el.bg(theme.secondary))
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.layout = if results_visible {
                                SqlQueryLayout::EditorOnly
                            } else {
                                SqlQueryLayout::Split
                            };
                            cx.notify();
                        }))
                        .child(
                            svg()
                                .path(AppIcon::Rows3.path())
                                .size_4()
                                .text_color(icon_color),
                        ),
                )
            })
    }

    fn render_editor(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_focused = self.focus_mode == SqlQueryFocus::Editor;
        let bg = cx.theme().background;
        let accent = cx.theme().accent;

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(bg)
            .when(is_focused, |el| {
                el.border_2().border_color(accent.opacity(0.3))
            })
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .child(Input::new(&self.input_state).appearance(false)),
            )
    }

    fn render_results(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_focused = self.focus_mode == SqlQueryFocus::Results;
        let bg = cx.theme().background;
        let accent = cx.theme().accent;

        let error = self
            .active_execution_index
            .and_then(|i| self.execution_history.get(i))
            .and_then(|r| r.error.clone());

        let has_error = error.is_some();
        let has_grid = self.data_grid.is_some();

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(bg)
            .when(is_focused, |el| {
                el.border_2().border_color(accent.opacity(0.3))
            })
            .when_some(error, |el, err| el.child(self.render_error_state(&err, cx)))
            .when_some(self.data_grid.clone(), |el, grid| el.child(grid))
            .when(!has_grid && !has_error, |el| {
                el.child(self.render_empty_results(cx))
            })
    }

    fn render_error_state(&self, error: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let error_color = cx.theme().danger;
        let muted_fg = cx.theme().muted_foreground;

        div()
            .size_full()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .gap_2()
            .child(
                div()
                    .text_color(error_color)
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child("Query Error"),
            )
            .child(
                div()
                    .text_color(muted_fg)
                    .text_sm()
                    .max_w(px(500.0))
                    .text_center()
                    .child(error.to_string()),
            )
    }

    fn render_empty_results(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let muted_fg = cx.theme().muted_foreground;

        div()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .child(
                div()
                    .text_color(muted_fg)
                    .child("Run a query to see results"),
            )
    }
}

impl Render for SqlQueryDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Process any pending query result (needs window access)
        self.process_pending_result(window, cx);

        // Process any pending query from history modal selection
        self.process_pending_set_query(window, cx);

        let toolbar = self.render_toolbar(cx).into_any_element();
        let editor_view = self.render_editor(window, cx).into_any_element();
        let results_view = self.render_results(window, cx).into_any_element();

        let bg = cx.theme().background;

        div()
            .id(ElementId::Name(format!("sql-doc-{}", self.id.0).into()))
            .size_full()
            .flex()
            .flex_col()
            .bg(bg)
            .track_focus(&self.focus_handle)
            // Toolbar at top
            .child(toolbar)
            // Content area (editor/results split)
            .child(
                div().flex_1().overflow_hidden().child(match self.layout {
                    SqlQueryLayout::Split => {
                        v_resizable(SharedString::from(format!("sql-split-{}", self.id.0)))
                            .child(
                                resizable_panel()
                                    .size(px(200.0))
                                    .size_range(px(100.0)..px(1000.0))
                                    .child(editor_view),
                            )
                            .child(
                                resizable_panel()
                                    .size(px(200.0))
                                    .size_range(px(100.0)..px(1000.0))
                                    .child(results_view),
                            )
                            .into_any_element()
                    }

                    SqlQueryLayout::EditorOnly => editor_view,

                    SqlQueryLayout::ResultsOnly => results_view,
                }),
            )
            // History modal overlay
            .child(self.history_modal.clone())
    }
}

impl EventEmitter<DocumentEvent> for SqlQueryDocument {}
