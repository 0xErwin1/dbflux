//! Driver-agnostic diff-source model and pure classification helpers for the
//! schema-diff document.
//!
//! Everything here is free of GPUI so it can be unit-tested directly: the
//! `SourcePicker`/`DiffMode` picker model, the risk-to-badge mapping, and the
//! partition that separates changes the driver can apply from the ones it must
//! surface as unsupported.

use std::collections::HashMap;

use dbflux_core::{
    CodeGenerator, DbSchemaInfo, DdlRejection, ExecutionClassification, RiskedChange, SchemaChange,
    TableInfo, TableRef, classify_table_added, classify_table_removed,
};
use uuid::Uuid;

use super::apply::{TableLevelAction, build_statements_for_change};

/// Which pair of sources the picker compares. The default — and the primary
/// workflow — is two live connections; snapshot-to-live is the secondary mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum DiffMode {
    #[default]
    LiveVsLive,
    SnapshotVsLive,
}

/// Selection state for the source picker. `before` is the live target the DDL
/// applies to and `after` is the reference schema `before` is brought in line
/// with (for snapshot-to-live the snapshot is the reference `after`).
#[derive(Clone, Debug, Default)]
pub struct SourcePicker {
    pub mode: DiffMode,
    /// Snapshot chosen as the reference side in `SnapshotVsLive` mode.
    pub selected_snapshot: Option<Uuid>,
}

/// The reference side chosen in `LiveVsLive` mode. The reference is what the
/// live target is compared against and brought in line with. It can be either a
/// different database on the SAME connection as the target — the common case,
/// e.g. `atlas_dev` vs `atlas_test` under one server — or a different open
/// relational connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReferenceTarget {
    /// A different database on the same connection as the diff target.
    SameConnectionDatabase(String),
    /// A different open relational connection. `database` pins a specific
    /// database on that connection when set; otherwise its active database is
    /// used.
    OtherConnection {
        profile_id: Uuid,
        database: Option<String>,
    },
}

/// Live-vs-live readiness: a diff can only run once a reference has been chosen.
pub fn live_reference_ready(reference: &Option<ReferenceTarget>) -> bool {
    reference.is_some()
}

/// The other databases on the target's own connection that can serve as a
/// same-connection reference: every known database name except the target's
/// own. Pure so the picker's readiness can be unit-tested without GPUI.
pub fn same_connection_reference_databases(
    available: &[String],
    own_database: Option<&str>,
) -> Vec<String> {
    available
        .iter()
        .filter(|db| Some(db.as_str()) != own_database)
        .cloned()
        .collect()
}

/// Resolves the shallow table list for a same-connection reference database
/// from the connection's per-database schema cache.
///
/// A database whose schema has not been loaded yet is reported as an error
/// rather than silently treated as empty: an empty reference would make the
/// diff engine see every table as a drop, producing a bogus destructive plan.
/// This upholds the "no silently-incomplete metadata" invariant the deep
/// resolver already enforces for column detail.
pub fn resolve_same_connection_shallow(
    database_schemas: &HashMap<String, DbSchemaInfo>,
    database: &str,
) -> Result<Vec<TableInfo>, String> {
    match database_schemas.get(database) {
        Some(schema) => Ok(schema.tables.clone()),
        None => Err(format!(
            "The schema for \"{database}\" is not loaded yet. Expand that database \
             in the sidebar first, then run Compute Diff again."
        )),
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
    pub applicable: Vec<RiskedChange>,
    pub unsupported: Vec<UnsupportedChange>,
}

impl PartitionedChanges {
    pub fn is_empty(&self) -> bool {
        self.applicable.is_empty() && self.unsupported.is_empty()
    }
}

/// Splits `changes` for one table into the set the driver can generate DDL for
/// and the set it rejects, by probing each change through the same
/// `CodeGenerator` mapping the apply path uses.
///
/// This keeps the executor's all-or-nothing contract intact: only the
/// `applicable` half is ever handed to `DdlApplyExecutor`, and every rejection
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
            Ok(_) => partitioned.applicable.push(risked.clone()),
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

/// The result of classifying a whole-table add/remove: its governance risk,
/// and whether the driver's `generate_code` seam (`"create_table"`/
/// `"drop_table"`) can express it. Table-level counterpart to
/// `PartitionedChanges` — a single item rather than a list, since
/// `TableChange::TableAdded`/`TableRemoved` each carry exactly one action.
#[derive(Clone, Debug)]
pub enum TableActionOutcome {
    Applicable {
        action: TableLevelAction,
        risk: ExecutionClassification,
    },
    Unsupported {
        is_create: bool,
        risk: ExecutionClassification,
        reason: String,
        followup: Option<String>,
    },
}

/// Risk-classifies a whole-table add/remove and folds in the outcome of
/// probing `Connection::generate_code` (via `build_statements_for_table_action`).
/// The probe result is passed in rather than a live `Connection` so this stays
/// a pure, easily unit-tested classification step, mirroring
/// `partition_table_changes` for column/index changes.
pub fn classify_table_action(
    action: TableLevelAction,
    probe: Result<Vec<String>, DdlRejection>,
) -> TableActionOutcome {
    let is_create = matches!(action, TableLevelAction::Create(_));
    let risk = if is_create {
        classify_table_added()
    } else {
        classify_table_removed()
    };

    match probe {
        Ok(_) => TableActionOutcome::Applicable { action, risk },
        Err(rejection) => TableActionOutcome::Unsupported {
            is_create,
            risk,
            reason: rejection.reason,
            followup: rejection.followup.map(|s| s.to_string()),
        },
    }
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

    // -- Partitioning ----------------------------------------------------------

    #[test]
    fn applicable_column_changes_are_kept_applicable() {
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

        assert_eq!(partitioned.applicable.len(), 2);
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

        assert_eq!(partitioned.applicable.len(), 1);
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

        assert!(partitioned.applicable.is_empty());
        assert_eq!(partitioned.unsupported.len(), 1);
    }

    // -- Table-level action classification --------------------------------

    fn table_info() -> TableInfo {
        TableInfo {
            name: "orders".to_string(),
            schema: Some("public".to_string()),
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
        }
    }

    #[test]
    fn table_added_probe_ok_is_applicable_with_admin_safe_risk() {
        let action = TableLevelAction::Create(table_info());

        let outcome = classify_table_action(action, Ok(vec!["CREATE TABLE orders ()".to_string()]));

        match outcome {
            TableActionOutcome::Applicable { risk, .. } => {
                assert_eq!(risk, ExecutionClassification::AdminSafe);
            }
            other => panic!("expected Applicable, got {other:?}"),
        }
    }

    #[test]
    fn table_removed_probe_ok_is_applicable_with_admin_destructive_risk() {
        let action = TableLevelAction::Drop(table());

        let outcome = classify_table_action(action, Ok(vec!["DROP TABLE users".to_string()]));

        match outcome {
            TableActionOutcome::Applicable { risk, .. } => {
                assert_eq!(risk, ExecutionClassification::AdminDestructive);
            }
            other => panic!("expected Applicable, got {other:?}"),
        }
    }

    #[test]
    fn table_added_probe_err_is_unsupported_with_reason_and_followup() {
        let action = TableLevelAction::Create(table_info());
        let probe = Err(DdlRejection {
            reason: "Code generator 'create_table' not supported".to_string(),
            followup: Some("DBF-999"),
        });

        let outcome = classify_table_action(action, probe);

        match outcome {
            TableActionOutcome::Unsupported {
                is_create,
                risk,
                reason,
                followup,
            } => {
                assert!(is_create);
                assert_eq!(risk, ExecutionClassification::AdminSafe);
                assert!(reason.contains("create_table"));
                assert_eq!(followup.as_deref(), Some("DBF-999"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    // -- Reference target model (same-connection vs other connection) ------

    fn db_schema_with_tables(name: &str, table_names: &[&str]) -> dbflux_core::DbSchemaInfo {
        dbflux_core::DbSchemaInfo {
            name: name.to_string(),
            tables: table_names
                .iter()
                .map(|t| TableInfo {
                    name: t.to_string(),
                    schema: Some("public".to_string()),
                    columns: None,
                    indexes: None,
                    foreign_keys: None,
                    constraints: None,
                    sample_fields: None,
                    presentation: Default::default(),
                    child_items: None,
                })
                .collect(),
            views: Vec::new(),
            custom_types: None,
        }
    }

    #[test]
    fn same_connection_databases_excludes_the_targets_own_database() {
        let available = vec![
            "atlas_dev".to_string(),
            "atlas_test".to_string(),
            "postgres".to_string(),
        ];

        let out = same_connection_reference_databases(&available, Some("atlas_dev"));

        assert_eq!(out, vec!["atlas_test".to_string(), "postgres".to_string()]);
    }

    #[test]
    fn same_connection_databases_keeps_all_when_target_database_is_unknown() {
        let available = vec!["atlas_dev".to_string(), "atlas_test".to_string()];

        let out = same_connection_reference_databases(&available, None);

        assert_eq!(out, available);
    }

    #[test]
    fn resolve_same_connection_shallow_returns_the_loaded_tables() {
        let mut schemas = std::collections::HashMap::new();
        schemas.insert(
            "atlas_test".to_string(),
            db_schema_with_tables("atlas_test", &["users", "orders"]),
        );

        let resolved = resolve_same_connection_shallow(&schemas, "atlas_test")
            .expect("a loaded database resolves to its cached tables");

        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].name, "users");
        assert_eq!(resolved[1].name, "orders");
    }

    #[test]
    fn resolve_same_connection_shallow_errors_when_the_database_is_not_loaded() {
        let schemas: std::collections::HashMap<String, dbflux_core::DbSchemaInfo> =
            std::collections::HashMap::new();

        let err = resolve_same_connection_shallow(&schemas, "atlas_test")
            .expect_err("an unloaded reference database must not silently resolve to empty");

        assert!(err.contains("atlas_test"));
        assert!(err.to_lowercase().contains("sidebar"));
    }

    #[test]
    fn live_reference_ready_only_when_a_reference_is_chosen() {
        assert!(!live_reference_ready(&None));
        assert!(live_reference_ready(&Some(
            ReferenceTarget::SameConnectionDatabase("atlas_test".to_string())
        )));
        assert!(live_reference_ready(&Some(
            ReferenceTarget::OtherConnection {
                profile_id: Uuid::nil(),
                database: None,
            }
        )));
    }

    #[test]
    fn table_removed_probe_err_is_unsupported_with_is_create_false() {
        let action = TableLevelAction::Drop(table());
        let probe = Err(DdlRejection {
            reason: "Code generator 'drop_table' not supported".to_string(),
            followup: None,
        });

        let outcome = classify_table_action(action, probe);

        match outcome {
            TableActionOutcome::Unsupported {
                is_create, risk, ..
            } => {
                assert!(!is_create);
                assert_eq!(risk, ExecutionClassification::AdminDestructive);
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }
}
