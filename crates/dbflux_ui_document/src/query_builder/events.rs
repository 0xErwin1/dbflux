use dbflux_core::VisualQuerySpec;

/// Events emitted by `QueryBuilderPanel` to signal state changes and user actions.
///
/// Callers subscribe to these events to react to builder-driven spec mutations
/// and user intent (run, save, open in editor, reset, import).
#[derive(Clone, Debug)]
pub enum BuilderEvent {
    /// The user modified the spec (column added, filter changed, etc.).
    /// Carries the updated spec.
    SpecChanged(Box<VisualQuerySpec>),

    /// The user pressed Run or Cmd+Enter.
    RunRequested,

    /// The user pressed Open in Editor or Cmd+E.
    OpenInEditorRequested,

    /// The user pressed Save or Cmd+S.
    SaveRequested { name: String },

    /// The user pressed Save As or Cmd+Shift+S.
    SaveAsRequested { name: String },

    /// The user pressed Reset or Cmd+Backspace.
    ResetRequested,

    /// The user chose to import a saved query to the current profile.
    ImportRequested { source_id: String },
}
