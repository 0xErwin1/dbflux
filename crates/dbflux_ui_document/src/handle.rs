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
    /// User requested "Chart this query" from a data document's context menu.
    ChartThisQuery {
        query: String,
        connection_id: Option<uuid::Uuid>,
    },
}
