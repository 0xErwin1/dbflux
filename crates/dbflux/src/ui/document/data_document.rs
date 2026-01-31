use super::data_grid_panel::{DataGridEvent, DataGridPanel, DataSource};
use super::types::{DataSourceKind, DocumentId, DocumentState};
use crate::app::AppState;
use crate::keymap::{Command, ContextId};
use dbflux_core::{QueryResult, TableRef};
use gpui::*;
use std::sync::Arc;
use uuid::Uuid;

/// Document for displaying data in a standalone tab.
/// Used for both table browsing (click on sidebar) and promoted query results.
pub struct DataDocument {
    id: DocumentId,
    title: String,
    source_kind: DataSourceKind,
    data_grid: Entity<DataGridPanel>,
    focus_handle: FocusHandle,
    _subscription: Subscription,
}

/// Events emitted by DataDocument.
#[derive(Clone, Debug)]
pub enum DataDocumentEvent {
    MetaChanged,
    PromoteResult {
        result: Arc<QueryResult>,
        query: String,
    },
}

impl DataDocument {
    /// Create a document for browsing a table.
    pub fn new_for_table(
        profile_id: Uuid,
        table: TableRef,
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let title = table.qualified_name();
        let data_grid =
            cx.new(|cx| DataGridPanel::new_for_table(profile_id, table, app_state, window, cx));

        let subscription =
            cx.subscribe(
                &data_grid,
                |this, _grid, event: &DataGridEvent, cx| match event {
                    DataGridEvent::PromoteResult { result, query } => {
                        cx.emit(DataDocumentEvent::PromoteResult {
                            result: result.clone(),
                            query: query.clone(),
                        });
                    }
                },
            );

        Self {
            id: DocumentId::new(),
            title,
            source_kind: DataSourceKind::Table,
            data_grid,
            focus_handle: cx.focus_handle(),
            _subscription: subscription,
        }
    }

    /// Create a document for a promoted query result.
    pub fn new_for_result(
        result: Arc<QueryResult>,
        query: String,
        title: String,
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let data_grid =
            cx.new(|cx| DataGridPanel::new_for_result(result, query, app_state, window, cx));

        let subscription = cx.subscribe(&data_grid, |_this, _grid, _event: &DataGridEvent, _cx| {
            // Promoted results don't need to handle PromoteResult again
        });

        Self {
            id: DocumentId::new(),
            title,
            source_kind: DataSourceKind::QueryResult,
            data_grid,
            focus_handle: cx.focus_handle(),
            _subscription: subscription,
        }
    }

    // === Accessors ===

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        self.title.clone()
    }

    pub fn state(&self) -> DocumentState {
        DocumentState::Clean
    }

    pub fn source_kind(&self) -> DataSourceKind {
        self.source_kind
    }

    pub fn can_close(&self) -> bool {
        true
    }

    pub fn focus(&mut self, window: &mut Window, _cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
    }

    pub fn connection_id(&self, cx: &App) -> Option<Uuid> {
        match self.data_grid.read(cx).source() {
            DataSource::Table { profile_id, .. } => Some(*profile_id),
            DataSource::QueryResult { .. } => None,
        }
    }

    /// Returns the active context for keyboard handling.
    /// DataDocument is always in Results context since it's a pure data grid.
    pub fn active_context(&self) -> ContextId {
        ContextId::Results
    }

    // === Command Dispatch ===

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match cmd {
            Command::RefreshSchema => {
                self.data_grid.update(cx, |grid, cx| {
                    grid.refresh(window, cx);
                });
                true
            }
            Command::SelectNext | Command::FocusDown => {
                self.data_grid.update(cx, |grid, cx| {
                    grid.select_next(cx);
                });
                true
            }
            Command::SelectPrev | Command::FocusUp => {
                self.data_grid.update(cx, |grid, cx| {
                    grid.select_prev(cx);
                });
                true
            }
            Command::SelectFirst => {
                self.data_grid.update(cx, |grid, cx| {
                    grid.select_first(cx);
                });
                true
            }
            Command::SelectLast => {
                self.data_grid.update(cx, |grid, cx| {
                    grid.select_last(cx);
                });
                true
            }
            Command::ColumnLeft | Command::FocusLeft => {
                self.data_grid.update(cx, |grid, cx| {
                    grid.column_left(cx);
                });
                true
            }
            Command::ColumnRight | Command::FocusRight => {
                self.data_grid.update(cx, |grid, cx| {
                    grid.column_right(cx);
                });
                true
            }
            Command::ResultsNextPage | Command::PageDown => {
                self.data_grid.update(cx, |grid, cx| {
                    grid.go_to_next_page(window, cx);
                });
                true
            }
            Command::ResultsPrevPage | Command::PageUp => {
                self.data_grid.update(cx, |grid, cx| {
                    grid.go_to_prev_page(window, cx);
                });
                true
            }
            Command::FocusToolbar => {
                self.data_grid.update(cx, |grid, cx| {
                    grid.focus_toolbar(cx);
                });
                true
            }
            Command::ExportResults => {
                self.data_grid.update(cx, |grid, cx| {
                    grid.export_results(window, cx);
                });
                true
            }
            _ => false,
        }
    }
}

impl EventEmitter<DataDocumentEvent> for DataDocument {}

impl Render for DataDocument {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .track_focus(&self.focus_handle)
            .child(self.data_grid.clone())
    }
}
