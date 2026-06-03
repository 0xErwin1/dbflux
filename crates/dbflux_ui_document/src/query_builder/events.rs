use dbflux_core::{VisualMutationSpec, VisualQuerySpec};

use crate::data_grid_panel::mutation_executor::MutationExecOptions;

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

    /// The user pressed Run from UPDATE or DELETE mode.
    ///
    /// Carries the fully-built mutation spec, the execution options chosen in
    /// the Execution section, and the current row count estimate (if available).
    /// `DataGridPanel` uses `est_rows` to select the Light vs Hard confirmation
    /// modal (spec DR-9.1). `None` means the count is unknown or still in flight.
    MutationRunRequested {
        spec: Box<VisualMutationSpec>,
        opts: Box<MutationExecOptions>,
        /// Row count estimate at run-press time. `None` if counting or unavailable.
        est_rows: Option<u64>,
    },

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

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{MutationKind, TableRef};

    use crate::data_grid_panel::mutation_executor::ExecutionMode;

    #[test]
    fn mutation_run_requested_variant_exists_and_fields_accessible() {
        let spec = VisualMutationSpec {
            from: TableRef {
                schema: None,
                name: "orders".to_string(),
            },
            filter: None,
            kind: MutationKind::Delete,
        };
        let opts = MutationExecOptions::single_transaction();

        let event = BuilderEvent::MutationRunRequested {
            spec: Box::new(spec.clone()),
            opts: Box::new(opts),
            est_rows: Some(42),
        };

        match event {
            BuilderEvent::MutationRunRequested {
                spec: s,
                opts: o,
                est_rows,
            } => {
                assert_eq!(s.from.name, "orders");
                assert_eq!(o.mode, ExecutionMode::SingleTransaction);
                assert_eq!(est_rows, Some(42));
            }
            _ => panic!("wrong variant"),
        }
    }
}
