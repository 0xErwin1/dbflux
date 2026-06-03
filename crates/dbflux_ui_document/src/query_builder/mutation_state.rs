use dbflux_core::{Assignment, VisualMutationSpec};

use crate::data_grid_panel::mutation_executor::{CountState, ExecutionMode, MutationExecOptions};

/// The builder mode the panel is currently in.
///
/// `Select` is the default mode; `Update` and `Delete` activate the mutation
/// sections. The panel always opens in `Select` mode (DR-1.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuilderMode {
    Select,
    Update,
    Delete,
}

impl BuilderMode {
    /// Returns `true` when the mode produces a mutation spec (not a SELECT).
    pub fn is_mutation(self) -> bool {
        matches!(self, BuilderMode::Update | BuilderMode::Delete)
    }
}

/// Per-assignment inline-editor text buffer.
///
/// The panel tracks a mutable text value for each assignment row so that partial
/// user input (e.g. a partially typed literal) does not immediately corrupt the
/// `AssignmentValue` until the user commits.
#[derive(Debug, Clone)]
pub struct AssignmentRow {
    pub assignment: Assignment,
    /// Raw text in the value input widget for this row.
    pub raw_text: String,
}

/// State owned by `QueryBuilderPanel` while it is in `Update` or `Delete` mode.
///
/// Created when the mode switches away from `Select`, dropped when it switches back.
#[derive(Debug, Clone)]
pub struct MutationBuilderState {
    pub mode: BuilderMode,

    /// Assignment rows (only meaningful in `Update` mode).
    pub assignments: Vec<AssignmentRow>,

    /// Execution options for this run.
    pub exec_options: MutationExecOptions,

    /// Result of the pre-execution COUNT query, used to populate the UI label
    /// and to drive `auto_suggest_mode`.
    pub count_state: CountState,

    /// The last spec that was built from this state.
    ///
    /// Kept so the panel can check whether a re-build is needed (spec changes
    /// when assignments or mode-specific config changes).
    pub last_built_spec: Option<VisualMutationSpec>,
}

impl MutationBuilderState {
    /// Creates a fresh state for the given mode with default options.
    pub fn new(mode: BuilderMode) -> Self {
        Self {
            mode,
            assignments: Vec::new(),
            exec_options: MutationExecOptions::single_transaction(),
            count_state: CountState::Counting,
            last_built_spec: None,
        }
    }

    /// Returns `true` when there are no assignments and the mode is `Update`.
    ///
    /// The Run button should be disabled when this is the case (DR-5.6).
    pub fn is_update_with_no_assignments(&self) -> bool {
        self.mode == BuilderMode::Update && self.assignments.is_empty()
    }
}
