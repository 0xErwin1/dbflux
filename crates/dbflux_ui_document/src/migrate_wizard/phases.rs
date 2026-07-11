//! Pure phase state machine for the migration wizard: the fixed rail
//! ordering, the guards that gate each forward transition, the FK-cycle
//! reorder interrupt overlay, and run-state tracking. No GPUI — unit
//! testable without a wizard entity. Rendering (`render_phase_rail`) and the
//! metadata-dependent transitions (`Options` → `Confirm`, which needs a real
//! `topological_order` result) are built in later slices; this module only
//! owns the state shapes and the guards that do not require live metadata.

use dbflux_core::TableRef;

use crate::migrate_wizard::tree_model::NodeLoad;

/// The five fixed rail entries. Declaration order doubles as the `Ord`
/// used by the rail to decide which entries are already completed
/// (`entry < current_phase`) — see design ADR #1. A cyclic FK graph is a
/// conditional interrupt surfaced inside `Confirm` (see [`ReorderState`]),
/// never a sixth listed phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WizardPhase {
    SourceTarget,
    TablesMapping,
    Options,
    Confirm,
    Run,
}

/// All rail entries in display order, for `render_phase_rail` to iterate.
pub const RAIL_PHASES: [WizardPhase; 5] = [
    WizardPhase::SourceTarget,
    WizardPhase::TablesMapping,
    WizardPhase::Options,
    WizardPhase::Confirm,
    WizardPhase::Run,
];

/// Whether `entry` should render a checkmark given the wizard is currently
/// on `current` — an already-passed rail entry.
pub fn is_completed(entry: WizardPhase, current: WizardPhase) -> bool {
    entry < current
}

/// Guard for `SourceTarget` → `TablesMapping`: at least one source table is
/// checked, a target container has been chosen, and the source/target
/// drivers are transfer-compatible.
pub fn can_advance_from_source_target(
    checked_table_count: usize,
    target_container_chosen: bool,
    transfer_compatible: bool,
) -> bool {
    checked_table_count > 0 && target_container_chosen && transfer_compatible
}

/// One mapping-grid row's readiness to advance past `TablesMapping`,
/// decoupled from the grid's full row type (built in a later slice).
pub struct MappingRowReadiness<'a> {
    pub target_name: &'a str,
    pub target_lookup: NodeLoad,
}

/// Guard for `TablesMapping` → `Options`: every row has a non-empty target
/// table name and its target-existence lookup has finished (neither
/// `Loading` nor `Failed`).
pub fn can_advance_from_tables_mapping(rows: &[MappingRowReadiness]) -> bool {
    rows.iter().all(|row| {
        !row.target_name.trim().is_empty()
            && !matches!(row.target_lookup, NodeLoad::Loading | NodeLoad::Failed(_))
    })
}

/// The FK-cycle reorder interrupt shown inside `Confirm` when
/// `topological_order` reports a cycle among the selected tables — not a
/// listed rail phase (design ADR #1). `prefix` is the fixed, already-ordered
/// portion; `list` is the cyclic remainder the user reorders with Up/Down.
pub struct ReorderState {
    pub prefix: Vec<TableRef>,
    pub list: Vec<TableRef>,
}

impl ReorderState {
    pub fn new(prefix: Vec<TableRef>, list: Vec<TableRef>) -> Self {
        Self { prefix, list }
    }

    /// Swaps `index` with `index + delta`, ignoring the move if it would
    /// fall outside `list`'s bounds (the reorderable cyclic subset).
    pub fn move_row(&mut self, index: usize, delta: isize) {
        let Some(new_index) = index.checked_add_signed(delta) else {
            return;
        };
        if index >= self.list.len() || new_index >= self.list.len() {
            return;
        }
        self.list.swap(index, new_index);
    }

    /// The final load order once the user accepts the current arrangement:
    /// the fixed prefix followed by the user-ordered cyclic remainder.
    pub fn resolved_order(&self) -> Vec<TableRef> {
        let mut order = self.prefix.clone();
        order.extend(self.list.clone());
        order
    }
}

/// Tracks the migration run itself, separate from `WizardPhase` so `Run`
/// can stay a single rail entry while progress/completion vary underneath.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RunState {
    #[default]
    Idle,
    Running,
    Done,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_ordering_matches_rail_declaration_order() {
        assert!(WizardPhase::SourceTarget < WizardPhase::TablesMapping);
        assert!(WizardPhase::TablesMapping < WizardPhase::Options);
        assert!(WizardPhase::Options < WizardPhase::Confirm);
        assert!(WizardPhase::Confirm < WizardPhase::Run);
        assert!(WizardPhase::Run >= WizardPhase::SourceTarget);
    }

    #[test]
    fn is_completed_is_true_only_for_entries_before_current() {
        assert!(is_completed(
            WizardPhase::SourceTarget,
            WizardPhase::TablesMapping
        ));
        assert!(!is_completed(
            WizardPhase::TablesMapping,
            WizardPhase::TablesMapping
        ));
        assert!(!is_completed(
            WizardPhase::Confirm,
            WizardPhase::TablesMapping
        ));
    }

    #[test]
    fn can_advance_from_source_target_requires_checked_table_target_and_compatibility() {
        assert!(!can_advance_from_source_target(0, true, true));
        assert!(!can_advance_from_source_target(1, false, true));
        assert!(!can_advance_from_source_target(1, true, false));
        assert!(can_advance_from_source_target(1, true, true));
    }

    #[test]
    fn can_advance_from_tables_mapping_requires_every_row_named_and_lookup_resolved() {
        let ready = vec![
            MappingRowReadiness {
                target_name: "users",
                target_lookup: NodeLoad::Loaded,
            },
            MappingRowReadiness {
                target_name: "orders",
                target_lookup: NodeLoad::NotLoaded,
            },
        ];
        assert!(can_advance_from_tables_mapping(&ready));

        let empty_name = vec![MappingRowReadiness {
            target_name: "",
            target_lookup: NodeLoad::Loaded,
        }];
        assert!(!can_advance_from_tables_mapping(&empty_name));

        let blank_name = vec![MappingRowReadiness {
            target_name: "   ",
            target_lookup: NodeLoad::Loaded,
        }];
        assert!(!can_advance_from_tables_mapping(&blank_name));

        let still_loading = vec![MappingRowReadiness {
            target_name: "users",
            target_lookup: NodeLoad::Loading,
        }];
        assert!(!can_advance_from_tables_mapping(&still_loading));

        let lookup_failed = vec![MappingRowReadiness {
            target_name: "users",
            target_lookup: NodeLoad::Failed("boom".to_string()),
        }];
        assert!(!can_advance_from_tables_mapping(&lookup_failed));
    }

    #[test]
    fn reorder_state_move_row_swaps_within_bounds_and_ignores_out_of_range_moves() {
        let mut reorder = ReorderState::new(
            vec![TableRef::new("a")],
            vec![TableRef::new("b"), TableRef::new("c")],
        );

        reorder.move_row(0, 1);
        assert_eq!(reorder.list[0].name, "c");
        assert_eq!(reorder.list[1].name, "b");

        reorder.move_row(0, -1);
        assert_eq!(reorder.list[0].name, "c");

        reorder.move_row(1, 1);
        assert_eq!(reorder.list[1].name, "b");
    }

    #[test]
    fn reorder_state_resolved_order_is_prefix_then_reordered_list() {
        let reorder = ReorderState::new(
            vec![TableRef::new("a")],
            vec![TableRef::new("c"), TableRef::new("b")],
        );

        let names: Vec<String> = reorder
            .resolved_order()
            .into_iter()
            .map(|t| t.name)
            .collect();
        assert_eq!(names, vec!["a", "c", "b"]);
    }

    #[test]
    fn run_state_defaults_to_idle() {
        assert_eq!(RunState::default(), RunState::Idle);
    }
}
