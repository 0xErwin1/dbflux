use dbflux_components::SqlPreviewContext;

/// Events that a document can emit.
#[derive(Clone, Debug)]
pub enum DocumentEvent {
    /// Title, state, etc. changed.
    MetaChanged,
    ExecutionStarted,
    ExecutionFinished,
    /// The document wants to close itself.
    RequestClose,
    /// The document area was clicked and wants focus.
    RequestFocus,
    /// Request to show SQL preview modal (from DataGridPanel).
    RequestSqlPreview {
        context: Box<SqlPreviewContext>,
        generation_type: dbflux_components::SqlGenerationType,
    },
    /// Request to mount content into the workspace-level inspector rail.
    OpenInspector {
        title: gpui::SharedString,
        content: gpui::AnyView,
    },
    /// Request to hide the workspace inspector rail without losing the
    /// document's cached inspector state (e.g. when switching away from a
    /// tab whose inspector should reappear on return).
    CloseInspector,
    /// User requested "Chart this query" from a data document's context menu.
    ChartThisQuery {
        query: String,
        connection_id: Option<uuid::Uuid>,
    },
    /// The chart document's active data source was replaced via `set_data_source`.
    ///
    /// Subscribers (e.g. the tab bar title chip) use this to refresh the
    /// displayed title without polling on every render.
    DataSourceChanged,
}
