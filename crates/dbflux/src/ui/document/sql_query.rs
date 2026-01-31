#![allow(dead_code)]

use super::handle::DocumentEvent;
use super::types::{DocumentId, DocumentState};
use crate::app::AppState;
use crate::keymap::Command;
use crate::ui::components::data_table::{DataTable, DataTableState, TableModel};
use crate::ui::toast::ToastExt;
use crate::ui::tokens::Spacing;
use dbflux_core::{CancelToken, DbError, QueryRequest, QueryResult};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::{Input, InputState};
use gpui_component::resizable::{resizable_panel, v_resizable};
use std::sync::Arc;
use std::time::Instant;
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

    // Results (embedded in the document)
    execution_history: Vec<ExecutionRecord>,
    active_execution_index: Option<usize>,
    data_table: Option<Entity<DataTable>>,
    table_state: Option<Entity<DataTableState>>,

    // Layout
    layout: SqlQueryLayout,

    // Focus
    focus_handle: FocusHandle,
    focus_mode: SqlQueryFocus,

    // Active execution
    active_cancel_token: Option<CancelToken>,
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
            data_table: None,
            table_state: None,
            layout: SqlQueryLayout::Split,
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
        let request = QueryRequest::new(query).with_database(active_database);

        let task = cx.background_executor().spawn({
            let connection = connection.clone();
            async move { connection.execute(&request) }
        });

        cx.spawn(async move |this, cx| {
            let result = task.await;

            cx.update(|cx| {
                let _ = this.update(cx, |doc, cx| {
                    doc.on_query_completed(exec_id, result, cx);
                });
            })
            .ok();
        })
        .detach();
    }

    fn on_query_completed(
        &mut self,
        exec_id: Uuid,
        result: Result<QueryResult, DbError>,
        cx: &mut Context<Self>,
    ) {
        self.active_cancel_token = None;
        self.state = DocumentState::Clean;

        let Some(record) = self.execution_history.iter_mut().find(|r| r.id == exec_id) else {
            return;
        };

        record.finished_at = Some(Instant::now());

        match result {
            Ok(qr) => {
                record.rows_affected = Some(qr.rows.len() as u64);
                let arc_result = Arc::new(qr);
                record.result = Some(arc_result.clone());

                // Create/update DataTable
                self.setup_data_table(arc_result, cx);

                // Switch focus to results
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
        cx.notify();
    }

    fn setup_data_table(&mut self, result: Arc<QueryResult>, cx: &mut Context<Self>) {
        let model = Arc::new(TableModel::from(result.as_ref()));
        let table_state = cx.new(|cx| DataTableState::new(model, cx));
        let table_id = ElementId::Name(format!("sql-doc-table-{}", self.id.0).into());
        let data_table = cx.new(|cx| DataTable::new(table_id, table_state.clone(), cx));

        self.table_state = Some(table_state);
        self.data_table = Some(data_table);
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

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
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
                if self.focus_mode == SqlQueryFocus::Editor && self.data_table.is_some() {
                    self.focus_mode = SqlQueryFocus::Results;
                    cx.notify();
                    true
                } else {
                    false
                }
            }
            Command::ToggleEditor => {
                self.layout = match self.layout {
                    SqlQueryLayout::EditorOnly => SqlQueryLayout::Split,
                    _ => SqlQueryLayout::EditorOnly,
                };
                cx.notify();
                true
            }
            Command::ToggleResults => {
                self.layout = match self.layout {
                    SqlQueryLayout::ResultsOnly => SqlQueryLayout::Split,
                    _ => SqlQueryLayout::ResultsOnly,
                };
                cx.notify();
                true
            }
            _ => false,
        }
    }

    // === Render ===

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

        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(bg)
            .when(is_focused, |el| {
                el.border_2().border_color(accent.opacity(0.3))
            })
            .child(self.render_results_toolbar(cx))
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .when_some(self.data_table.clone(), |el, dt| el.child(dt))
                    .when(self.data_table.is_none(), |el| {
                        el.child(self.render_empty_results(cx))
                    }),
            )
    }

    fn render_results_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let record = self
            .active_execution_index
            .and_then(|i| self.execution_history.get(i));

        let row_count = record
            .and_then(|r| r.result.as_ref())
            .map(|r| r.rows.len())
            .unwrap_or(0);

        let duration = record.and_then(|r| {
            r.finished_at
                .map(|end| end.duration_since(r.started_at).as_millis())
        });

        let error = record.and_then(|r| r.error.clone());

        let border_color = cx.theme().border;
        let panel_bg = cx.theme().tab_bar;
        let muted_fg = cx.theme().muted_foreground;
        let error_color = cx.theme().danger;

        div()
            .h(px(32.0))
            .w_full()
            .flex()
            .items_center()
            .justify_between()
            .px(Spacing::MD)
            .border_b_1()
            .border_color(border_color)
            .bg(panel_bg)
            // Info left
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::MD)
                    .when(error.is_none(), |el| {
                        el.child(
                            div()
                                .text_sm()
                                .text_color(muted_fg)
                                .child(format!("{} rows", row_count)),
                        )
                    })
                    .when_some(duration, |el, ms| {
                        el.child(
                            div()
                                .text_sm()
                                .text_color(muted_fg)
                                .child(format!("{}ms", ms)),
                        )
                    })
                    .when_some(error, |el, err| {
                        el.child(div().text_sm().text_color(error_color).child(err))
                    }),
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
            .child(match self.layout {
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
            })
    }
}

impl EventEmitter<DocumentEvent> for SqlQueryDocument {}
