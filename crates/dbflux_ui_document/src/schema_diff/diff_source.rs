//! Driver-agnostic diff-source model and pure classification helpers for the
//! schema-diff document.
//!
//! Everything here is free of GPUI so it can be unit-tested directly: the
//! `DiffSource`/`DiffMode` picker model, the risk-to-badge mapping, and the
//! partition that separates changes the driver can apply from the ones it must
//! surface as unsupported.

use dbflux_core::{
    CodeGenerator, ExecutionClassification, RiskedChange, SchemaChange, TableInfo, TableRef,
};
use uuid::Uuid;

use super::apply::build_statements_for_change;

/// One side of a schema comparison.
///
/// `Live` resolves through the driver (shallow tree plus lazy `table_details`);
/// `Snapshot` resolves from the persisted `SchemaSnapshotRepo`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiffSource {
    Live {
        profile_id: Uuid,
        database: Option<String>,
    },
    Snapshot {
        snapshot_id: Uuid,
    },
}

/// Which pair of sources the picker compares. The default — and the primary
/// workflow — is two live connections; snapshot-to-live is the secondary mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum DiffMode {
    #[default]
    LiveVsLive,
    SnapshotVsLive,
}

/// Selection state for the source picker. `before` is the reference side and
/// `after` is the target the DDL would bring in line with `before` (for
/// snapshot-to-live the snapshot is the reference `before`).
#[derive(Clone, Debug, Default)]
pub struct SourcePicker {
    pub mode: DiffMode,
    /// Snapshot chosen as the reference side in `SnapshotVsLive` mode.
    pub selected_snapshot: Option<Uuid>,
}

impl SourcePicker {
    /// Returns `true` when the current selection is complete enough to run a
    /// diff. Live-to-live is always ready (both live schemas are known from the
    /// open connection); snapshot-to-live needs a chosen snapshot first.
    pub fn is_ready(&self) -> bool {
        match self.mode {
            DiffMode::LiveVsLive => true,
            DiffMode::SnapshotVsLive => self.selected_snapshot.is_some(),
        }
    }
}

/// Three-level risk badge shown per change, derived from the shared governance
/// classification so the schema-diff surface stays consistent with MCP.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RiskBadge {
    Safe,
    Warning,
    Destructive,
}

impl RiskBadge {
    /// Collapses the seven-level `ExecutionClassification` ladder onto the three
    /// badge levels the diff list renders. Destructive/admin-destructive are red;
    /// write/admin (risky-but-recoverable DDL) are amber; everything safe is green.
    pub fn from_classification(classification: ExecutionClassification) -> Self {
        match classification {
            ExecutionClassification::Metadata
            | ExecutionClassification::Read
            | ExecutionClassification::AdminSafe => RiskBadge::Safe,
            ExecutionClassification::Write | ExecutionClassification::Admin => RiskBadge::Warning,
            ExecutionClassification::Destructive | ExecutionClassification::AdminDestructive => {
                RiskBadge::Destructive
            }
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            RiskBadge::Safe => "Safe",
            RiskBadge::Warning => "Warning",
            RiskBadge::Destructive => "Destructive",
        }
    }
}

/// A change excluded from apply because the driver cannot express it, carrying
/// the reason (and optional follow-up reference, e.g. DBF-158) so the diff list
/// can show it explicitly rather than dropping it silently.
#[derive(Clone, Debug)]
pub struct UnsupportedChange {
    pub change: SchemaChange,
    pub risk: ExecutionClassification,
    pub reason: String,
    pub followup: Option<String>,
}

/// The result of splitting a table's changes into what the executor can apply
/// and what must be surfaced as unsupported.
#[derive(Clone, Debug, Default)]
pub struct PartitionedChanges {
    pub appliable: Vec<RiskedChange>,
    pub unsupported: Vec<UnsupportedChange>,
}

impl PartitionedChanges {
    pub fn is_empty(&self) -> bool {
        self.appliable.is_empty() && self.unsupported.is_empty()
    }
}

/// Splits `changes` for one table into the set the driver can generate DDL for
/// and the set it rejects, by probing each change through the same
/// `CodeGenerator` mapping the apply path uses.
///
/// This keeps the executor's all-or-nothing contract intact: only the
/// `appliable` half is ever handed to `DdlApplyExecutor`, and every rejection
/// (constraint changes, SQLite rebuild-only column changes, index ops a driver
/// cannot express) lands in `unsupported` with its reason preserved.
pub fn partition_table_changes(
    table: &TableRef,
    changes: &[RiskedChange],
    code_generator: &dyn CodeGenerator,
) -> PartitionedChanges {
    let mut partitioned = PartitionedChanges::default();

    for risked in changes {
        match build_statements_for_change(table, &risked.change, code_generator) {
            Ok(_) => partitioned.appliable.push(risked.clone()),
            Err(rejection) => partitioned.unsupported.push(UnsupportedChange {
                change: risked.change.clone(),
                risk: risked.risk,
                reason: rejection.reason,
                followup: rejection.followup.map(|s| s.to_string()),
            }),
        }
    }

    partitioned
}

/// Resolves a persisted snapshot's stored tables into the `Vec<TableInfo>` the
/// diff engine consumes. Kept as a named seam so the Snapshot resolution path
/// is testable without a live connection.
pub fn tables_from_snapshot(record: &dbflux_core::SchemaSnapshotRecord) -> Vec<TableInfo> {
    record.tables.clone()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{
        AddColumnRequest, AlterColumnRequest, CodeGenCapabilities, ColumnSnapshot, DdlRejection,
        DropColumnRequest,
    };

    fn table() -> TableRef {
        TableRef {
            schema: Some("public".to_string()),
            name: "users".to_string(),
        }
    }

    fn column(name: &str) -> ColumnSnapshot {
        ColumnSnapshot {
            name: name.to_string(),
            type_name: "text".to_string(),
            nullable: true,
            is_primary_key: false,
            default_value: None,
        }
    }

    fn risked(change: SchemaChange, risk: ExecutionClassification) -> RiskedChange {
        RiskedChange { change, risk }
    }

    /// Generates column DDL but rejects any ALTER COLUMN with a named reason —
    /// stands in for the SQLite rebuild rejection.
    struct RebuildRejectingGenerator;

    impl CodeGenerator for RebuildRejectingGenerator {
        fn capabilities(&self) -> CodeGenCapabilities {
            CodeGenCapabilities::ADD_COLUMN | CodeGenCapabilities::DROP_COLUMN
        }

        fn generate_add_column(
            &self,
            request: &AddColumnRequest,
        ) -> Result<Vec<String>, DdlRejection> {
            Ok(vec![format!(
                "ALTER TABLE {} ADD COLUMN {}",
                request.table_name, request.column_name
            )])
        }

        fn generate_drop_column(
            &self,
            request: &DropColumnRequest,
        ) -> Result<Vec<String>, DdlRejection> {
            Ok(vec![format!(
                "ALTER TABLE {} DROP COLUMN {}",
                request.table_name, request.column_name
            )])
        }

        fn generate_alter_column(
            &self,
            _request: &AlterColumnRequest,
        ) -> Result<Vec<String>, DdlRejection> {
            Err(DdlRejection {
                reason: "SQLite requires a table rebuild".to_string(),
                followup: Some("DBF-158"),
            })
        }
    }

    // -- RiskBadge mapping -----------------------------------------------------

    #[test]
    fn admin_safe_maps_to_safe_badge() {
        assert_eq!(
            RiskBadge::from_classification(ExecutionClassification::AdminSafe),
            RiskBadge::Safe
        );
    }

    #[test]
    fn admin_maps_to_warning_badge() {
        assert_eq!(
            RiskBadge::from_classification(ExecutionClassification::Admin),
            RiskBadge::Warning
        );
    }

    #[test]
    fn admin_destructive_maps_to_destructive_badge() {
        assert_eq!(
            RiskBadge::from_classification(ExecutionClassification::AdminDestructive),
            RiskBadge::Destructive
        );
        assert_eq!(
            RiskBadge::from_classification(ExecutionClassification::Destructive),
            RiskBadge::Destructive
        );
    }

    #[test]
    fn every_classification_maps_to_a_badge() {
        for classification in [
            ExecutionClassification::Metadata,
            ExecutionClassification::Read,
            ExecutionClassification::Write,
            ExecutionClassification::Destructive,
            ExecutionClassification::AdminSafe,
            ExecutionClassification::Admin,
            ExecutionClassification::AdminDestructive,
        ] {
            let badge = RiskBadge::from_classification(classification);
            assert!(!badge.label().is_empty());
        }
    }

    // -- Source picker state ---------------------------------------------------

    #[test]
    fn default_mode_is_live_vs_live_and_ready() {
        let picker = SourcePicker::default();
        assert_eq!(picker.mode, DiffMode::LiveVsLive);
        assert!(picker.is_ready());
    }

    #[test]
    fn snapshot_mode_is_not_ready_until_a_snapshot_is_selected() {
        let mut picker = SourcePicker {
            mode: DiffMode::SnapshotVsLive,
            selected_snapshot: None,
        };
        assert!(!picker.is_ready());

        picker.selected_snapshot = Some(Uuid::now_v7());
        assert!(picker.is_ready());
    }

    // -- Partitioning ----------------------------------------------------------

    #[test]
    fn appliable_column_changes_are_kept_appliable() {
        let changes = vec![
            risked(
                SchemaChange::ColumnAdded(column("email")),
                ExecutionClassification::AdminSafe,
            ),
            risked(
                SchemaChange::ColumnRemoved(column("legacy")),
                ExecutionClassification::AdminDestructive,
            ),
        ];

        let partitioned = partition_table_changes(&table(), &changes, &RebuildRejectingGenerator);

        assert_eq!(partitioned.appliable.len(), 2);
        assert!(partitioned.unsupported.is_empty());
    }

    #[test]
    fn rebuild_rejected_change_lands_in_unsupported_with_followup() {
        let changes = vec![
            risked(
                SchemaChange::ColumnAdded(column("email")),
                ExecutionClassification::AdminSafe,
            ),
            risked(
                SchemaChange::ColumnTypeChanged {
                    before: column("id"),
                    after: ColumnSnapshot {
                        type_name: "bigint".to_string(),
                        ..column("id")
                    },
                },
                ExecutionClassification::Admin,
            ),
        ];

        let partitioned = partition_table_changes(&table(), &changes, &RebuildRejectingGenerator);

        assert_eq!(partitioned.appliable.len(), 1);
        assert_eq!(partitioned.unsupported.len(), 1);

        let unsupported = &partitioned.unsupported[0];
        assert!(unsupported.reason.contains("rebuild"));
        assert_eq!(unsupported.followup.as_deref(), Some("DBF-158"));
    }

    #[test]
    fn constraint_changes_are_always_unsupported() {
        let changes = vec![risked(
            SchemaChange::ForeignKeyChanged,
            ExecutionClassification::Admin,
        )];

        let partitioned = partition_table_changes(&table(), &changes, &RebuildRejectingGenerator);

        assert!(partitioned.appliable.is_empty());
        assert_eq!(partitioned.unsupported.len(), 1);
    }
}
