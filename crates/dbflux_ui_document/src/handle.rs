use dbflux_components::SqlPreviewContext;

/// Events that a document can emit.
#[derive(Clone, Debug)]
pub enum DocumentEvent {
    /// Title, state, etc. changed.
    MetaChanged,
    ExecutionStarted,
    ExecutionFinished,
    /// A save attempt finished.
    ///
    /// `succeeded: false` covers a dismissed Save As picker, a failed write,
    /// and a write that landed while the user kept typing — in every case the
    /// document still has pending changes. `true` means the buffer is clean
    /// against what was written. Saving is asynchronous, so the workspace uses
    /// this to give the keyboard back when the close it was waiting on did not
    /// happen.
    SaveFinished {
        succeeded: bool,
    },
    /// A mutation the document ran finished, and whether its edits landed.
    ///
    /// `landed: false` covers a failed run, a cancelled run, and a statement the
    /// database matched no rows for. A close waiting on this apply must not take
    /// it as permission to close, and the workspace uses it to give the keyboard
    /// back when the close it was waiting on did not happen.
    MutationFinished {
        landed: bool,
    },
    /// The document wants to close itself.
    ///
    /// Emitted only for a save the unsaved-changes dialog interrupted that
    /// actually landed, and only by documents that can save: it is what lets a
    /// tab close after its asynchronous write instead of over it.
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
    /// Dashboard document requests the workspace to open the "Add Panel" picker.
    RequestAddPanel {
        dashboard_id: uuid::Uuid,
    },
    /// Read-only dashboard requests the workspace to save a copy as a new
    /// editable dashboard for the same profile, then open it.
    RequestSaveAsEditable {
        source_title: String,
        profile_id: uuid::Uuid,
    },
    /// The query builder's "Open in Editor" was pressed.
    ///
    /// Carries the target connection profile and the fully materialized SQL
    /// (parameter literals inlined, no placeholders).
    OpenEditorWithContent {
        profile_id: uuid::Uuid,
        sql: String,
    },
}
