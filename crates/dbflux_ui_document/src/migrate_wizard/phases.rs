//! Pure phase state machine for the migration wizard: the fixed rail
//! ordering, the guards that gate each forward transition, the FK-cycle
//! reorder interrupt overlay, and run-state tracking. No GPUI — unit
//! testable without a wizard entity. Rendering (`render_phase_rail`) and the
//! metadata-dependent transitions (`Options` → `Confirm`, which needs a real
//! `topological_order` result) live in `mod.rs`; this module owns only the
//! state shapes and the guards that do not require live metadata.

use std::collections::{BTreeMap, BTreeSet, HashSet};

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

impl WizardPhase {
    /// The rail's display label for this phase.
    pub fn label(self) -> &'static str {
        match self {
            WizardPhase::SourceTarget => "Source & Target",
            WizardPhase::TablesMapping => "Tables Mapping",
            WizardPhase::Options => "Options",
            WizardPhase::Confirm => "Confirm",
            WizardPhase::Run => "Run",
        }
    }
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

/// One rail row's presentation state: a completed entry shows a checkmark and
/// is clickable for back-navigation; the current entry is highlighted. Derived
/// purely from the linear phase ordering so `render_phase_rail` stays a thin
/// view over this model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RailEntry {
    pub phase: WizardPhase,
    pub completed: bool,
    pub current: bool,
}

/// The rail's five entries with their completion/current flags resolved
/// against `current` — the single source of truth the renderer iterates.
pub fn rail_entries(current: WizardPhase) -> Vec<RailEntry> {
    RAIL_PHASES
        .iter()
        .map(|&phase| RailEntry {
            phase,
            completed: is_completed(phase, current),
            current: phase == current,
        })
        .collect()
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
/// decoupled from the grid's full row type in `mapping.rs`.
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

/// Cross-row view of one mapping row, for the collision checks that
/// [`MappingRowReadiness`] cannot express (they compare rows against each
/// other and against the shared source container).
pub struct MappingRowPlan<'a> {
    pub source_schema: Option<&'a str>,
    pub source_table: &'a str,
    pub target_schema: Option<&'a str>,
    pub target_table: &'a str,
    pub destructive: bool,
}

fn target_label(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(schema) if !schema.is_empty() => format!("{schema}.{table}"),
        _ => table.to_string(),
    }
}

/// Normalizes an optional schema so an empty schema string is treated as
/// "unqualified" (`None`), matching how [`target_label`] renders it — a bare
/// `Some("")` must never be a distinct identity from `None`.
fn normalized_schema(schema: Option<&str>) -> Option<&str> {
    schema.filter(|s| !s.is_empty())
}

/// Whether a destructive row's target refers to the same relation as a source
/// table in the same container. Table names must match; schemas are compared
/// only when the target schema is known — an unqualified target (`None`) is a
/// potential collision with any same-named source table, since the server
/// resolves it against the active schema at run time (the safe direction for a
/// destructive self-target). A qualified target with a *different* schema (e.g.
/// `archive.orders` vs source `public.orders`) is therefore not a collision.
fn is_same_relation(
    source_schema: Option<&str>,
    source_table: &str,
    target_schema: Option<&str>,
    target_table: &str,
) -> bool {
    if source_table != target_table {
        return false;
    }
    match normalized_schema(target_schema) {
        Some(target_schema) => normalized_schema(source_schema) == Some(target_schema),
        None => true,
    }
}

/// Blocking, cross-row validations for `TablesMapping` → `Options` beyond
/// per-row readiness:
/// - **Duplicate targets**: two rows writing the same `(schema, table)` would
///   silently have the second clobber the first.
/// - **Source-as-target destructive collision**: when source and target are the
///   same container, a destructive row (Recreate/Truncate) whose target is one
///   of the source tables would drop or empty that table before it is read.
///
/// Each returned string is a user-facing error; a non-empty result must block
/// advancing. Non-destructive same-container collisions are intentionally not
/// blocked here — they surface as a warning on the Confirm screen instead.
pub fn tables_mapping_blocking_errors(
    rows: &[MappingRowPlan],
    same_container: bool,
) -> Vec<String> {
    let mut errors = Vec::new();

    let mut seen: HashSet<(Option<&str>, &str)> = HashSet::new();
    let mut reported: HashSet<(Option<&str>, &str)> = HashSet::new();
    for row in rows {
        let key = (row.target_schema, row.target_table);
        if !seen.insert(key) && reported.insert(key) {
            errors.push(format!(
                "Two source tables map to the same target '{}'. Give each a unique target name.",
                target_label(row.target_schema, row.target_table)
            ));
        }
    }

    if same_container {
        let mut collided: HashSet<(Option<&str>, &str)> = HashSet::new();
        for row in rows {
            if !row.destructive {
                continue;
            }

            let collides = rows.iter().any(|source| {
                is_same_relation(
                    source.source_schema,
                    source.source_table,
                    row.target_schema,
                    row.target_table,
                )
            });
            let target_key = (normalized_schema(row.target_schema), row.target_table);

            if collides && collided.insert(target_key) {
                errors.push(format!(
                    "'{}' uses a destructive mode but its target is source table '{}' in the \
                     same connection — it would be dropped or emptied before it is read.",
                    row.source_table,
                    target_label(row.target_schema, row.target_table)
                ));
            }
        }
    }

    errors
}

/// Non-blocking, cross-row warnings surfaced on the Confirm screen:
/// - **Case-only target collisions**: two rows whose targets differ only in
///   letter case (e.g. `Users` and `users`). On a case-sensitive database they
///   are distinct tables (so this is not the blocking exact-duplicate error),
///   but on a case-insensitive collation they resolve to one table — worth a
///   warning, not a hard block. Independent of the source/target container.
/// - **Same-container append**: a non-destructive row whose target is one of the
///   source tables in the same container appends rows into a table that is also
///   being read. Destructive same-container collisions are blocked earlier by
///   [`tables_mapping_blocking_errors`].
pub fn tables_mapping_confirm_warnings(
    rows: &[MappingRowPlan],
    same_container: bool,
) -> Vec<String> {
    let mut warnings = case_insensitive_duplicate_target_warnings(rows);

    if same_container {
        warnings.extend(same_container_append_warnings(rows));
    }

    warnings
}

/// Warns when two target rows share the same identifier under a case-insensitive
/// comparison but differ in exact spelling. Exact duplicates are the blocking
/// error in [`tables_mapping_blocking_errors`], so a group that is a single
/// exact spelling is left alone here.
fn case_insensitive_duplicate_target_warnings(rows: &[MappingRowPlan]) -> Vec<String> {
    let mut folded: BTreeMap<(Option<String>, String), BTreeSet<String>> = BTreeMap::new();

    for row in rows {
        let key = (
            normalized_schema(row.target_schema).map(str::to_lowercase),
            row.target_table.to_lowercase(),
        );
        folded
            .entry(key)
            .or_default()
            .insert(target_label(row.target_schema, row.target_table));
    }

    folded
        .into_values()
        .filter(|spellings| spellings.len() > 1)
        .map(|spellings| {
            let labels: Vec<String> = spellings.into_iter().collect();
            format!(
                "Targets {} differ only in letter case; on a case-insensitive database they \
                 resolve to the same table.",
                labels.join(", ")
            )
        })
        .collect()
}

/// The same-container append warnings: a non-destructive row writing into one of
/// the source tables in the same container.
fn same_container_append_warnings(rows: &[MappingRowPlan]) -> Vec<String> {
    let mut warned: HashSet<(Option<&str>, &str)> = HashSet::new();

    rows.iter()
        .filter(|row| {
            if row.destructive {
                return false;
            }

            let collides = rows.iter().any(|source| {
                is_same_relation(
                    source.source_schema,
                    source.source_table,
                    row.target_schema,
                    row.target_table,
                )
            });

            collides && warned.insert((normalized_schema(row.target_schema), row.target_table))
        })
        .map(|row| {
            format!(
                "'{}' writes into source table '{}' in the same connection; rows are appended to \
                 a table you are also reading.",
                row.source_table,
                target_label(row.target_schema, row.target_table)
            )
        })
        .collect()
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

    fn plan<'a>(source: &'a str, target: &'a str, destructive: bool) -> MappingRowPlan<'a> {
        MappingRowPlan {
            source_schema: None,
            source_table: source,
            target_schema: None,
            target_table: target,
            destructive,
        }
    }

    fn qualified_plan<'a>(
        source_schema: Option<&'a str>,
        source: &'a str,
        target_schema: Option<&'a str>,
        target: &'a str,
        destructive: bool,
    ) -> MappingRowPlan<'a> {
        MappingRowPlan {
            source_schema,
            source_table: source,
            target_schema,
            target_table: target,
            destructive,
        }
    }

    #[test]
    fn tables_mapping_blocking_errors_flags_duplicate_targets() {
        let rows = vec![
            plan("users", "accounts", false),
            plan("members", "accounts", false),
        ];
        let errors = tables_mapping_blocking_errors(&rows, false);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("accounts"));
    }

    #[test]
    fn tables_mapping_blocking_errors_flags_destructive_source_as_target_only_in_same_container() {
        let rows = vec![plan("orders", "orders", true)];

        assert!(tables_mapping_blocking_errors(&rows, false).is_empty());

        let errors = tables_mapping_blocking_errors(&rows, true);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("orders"));
    }

    #[test]
    fn tables_mapping_blocking_errors_ignores_non_destructive_source_as_target() {
        let rows = vec![plan("orders", "orders", false)];
        assert!(tables_mapping_blocking_errors(&rows, true).is_empty());
    }

    #[test]
    fn tables_mapping_blocking_errors_compares_qualified_identity_for_source_as_target() {
        // A destructive row targeting `archive.orders` must NOT be blocked by a
        // source `public.orders` in the same connection — different schemas are
        // different relations.
        let different_schema = vec![qualified_plan(
            Some("public"),
            "orders",
            Some("archive"),
            "orders",
            true,
        )];
        assert!(tables_mapping_blocking_errors(&different_schema, true).is_empty());

        // A genuine same-(schema, table) destructive self-target still blocks.
        let same_schema = vec![qualified_plan(
            Some("public"),
            "orders",
            Some("public"),
            "orders",
            true,
        )];
        let errors = tables_mapping_blocking_errors(&same_schema, true);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("orders"));
    }

    #[test]
    fn tables_mapping_confirm_warnings_flags_non_destructive_same_container_collision() {
        let rows = vec![plan("orders", "orders", false)];

        assert!(tables_mapping_confirm_warnings(&rows, false).is_empty());

        let warnings = tables_mapping_confirm_warnings(&rows, true);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("orders"));
    }

    #[test]
    fn tables_mapping_confirm_warnings_flags_case_only_target_collisions() {
        // `Users` and `users` are distinct on a case-sensitive database (so no
        // blocking exact-duplicate error) but collide on a case-insensitive
        // collation — a non-blocking warning, independent of the container.
        let rows = vec![plan("a", "Users", false), plan("b", "users", false)];

        assert!(tables_mapping_blocking_errors(&rows, false).is_empty());

        let warnings = tables_mapping_confirm_warnings(&rows, false);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("Users"));
        assert!(warnings[0].contains("users"));
    }

    #[test]
    fn tables_mapping_confirm_warnings_ignores_exact_duplicate_targets() {
        // Exact duplicates are the blocking error, not a case-only warning.
        let rows = vec![plan("a", "users", false), plan("b", "users", false)];
        assert!(tables_mapping_confirm_warnings(&rows, false).is_empty());
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

    #[test]
    fn phase_labels_cover_every_rail_entry() {
        assert_eq!(WizardPhase::SourceTarget.label(), "Source & Target");
        assert_eq!(WizardPhase::TablesMapping.label(), "Tables Mapping");
        assert_eq!(WizardPhase::Options.label(), "Options");
        assert_eq!(WizardPhase::Confirm.label(), "Confirm");
        assert_eq!(WizardPhase::Run.label(), "Run");
    }

    #[test]
    fn rail_entries_mark_passed_phases_completed_and_only_current_as_current() {
        let entries = rail_entries(WizardPhase::Options);

        assert_eq!(entries.len(), RAIL_PHASES.len());

        let completed: Vec<WizardPhase> = entries
            .iter()
            .filter(|entry| entry.completed)
            .map(|entry| entry.phase)
            .collect();
        assert_eq!(
            completed,
            vec![WizardPhase::SourceTarget, WizardPhase::TablesMapping]
        );

        let current: Vec<WizardPhase> = entries
            .iter()
            .filter(|entry| entry.current)
            .map(|entry| entry.phase)
            .collect();
        assert_eq!(current, vec![WizardPhase::Options]);

        assert!(
            entries
                .iter()
                .all(|entry| !(entry.completed && entry.current))
        );
    }

    #[test]
    fn rail_entries_on_first_phase_have_no_completed_entries() {
        let entries = rail_entries(WizardPhase::SourceTarget);
        assert!(entries.iter().all(|entry| !entry.completed));
        assert!(entries[0].current);
    }
}
