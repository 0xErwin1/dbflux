use crate::schema::query_parser::QueryTableRef;
use crate::{
    ForeignKeyInfo, IndexData, IndexInfo, OwnedDefaultSpec, TableAlterExpectedColumn,
    TableAlterOperation, TableAlterRequest, TableInfo, TableRef,
};

/// Snapshot of a column's schema-relevant properties.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSnapshot {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub default_value: Option<String>,
}

/// A diff between two column snapshots.
#[derive(Debug, Clone)]
pub struct ColumnDiff {
    pub column: String,
    pub before: ColumnSnapshot,
    pub after: ColumnSnapshot,
}

/// Snapshot of an index's schema-relevant properties, used to identify an
/// index across two `TableInfo` snapshots by name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSnapshot {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
}

impl From<&IndexInfo> for IndexSnapshot {
    fn from(info: &IndexInfo) -> Self {
        Self {
            name: info.name.clone(),
            columns: info.columns.clone(),
            is_unique: info.is_unique,
        }
    }
}

/// An individual schema change detected between two `TableInfo` snapshots.
#[derive(Debug, Clone)]
pub enum SchemaChange {
    ColumnAdded(ColumnSnapshot),
    ColumnRemoved(ColumnSnapshot),
    ColumnTypeChanged {
        before: ColumnSnapshot,
        after: ColumnSnapshot,
    },
    NullabilityChanged {
        column: String,
        before: bool,
        after: bool,
    },
    DefaultChanged {
        column: String,
        before: Option<String>,
        after: Option<String>,
    },
    PrimaryKeyChanged {
        before: Vec<String>,
        after: Vec<String>,
    },
    /// Simplified FK change — reports any FK set mutation without per-key detail.
    ForeignKeyChanged,
    /// Non-primary-key index added. Primary key indexes are tracked separately
    /// via `PrimaryKeyChanged`.
    IndexAdded(IndexSnapshot),
    /// Non-primary-key index removed. See `IndexAdded`.
    IndexRemoved(IndexSnapshot),
}

/// A schema change annotated with its governance risk level.
#[derive(Debug, Clone)]
pub struct RiskedChange {
    pub change: SchemaChange,
    pub risk: dbflux_policy::ExecutionClassification,
}

/// A change detected at the table level between two schema snapshots.
#[derive(Debug, Clone)]
pub enum TableChange {
    TableAdded(TableInfo),
    TableRemoved(TableRef),
    TableModified {
        table: TableRef,
        changes: Vec<RiskedChange>,
    },
}

/// All changes detected for a specific table.
#[derive(Debug, Clone)]
pub struct SchemaDiff {
    pub table: QueryTableRef,
    pub changes: Vec<SchemaChange>,
    /// The fresh `TableInfo` fetched from the driver. On "Refresh & re-run"
    /// the caller writes this back into `ConnectedProfile::table_details`.
    pub fresh: crate::TableInfo,
}

/// Payload emitted when at least one referenced table has drifted.
#[derive(Debug, Clone)]
pub struct SchemaDriftDetected {
    pub diffs: Vec<SchemaDiff>,
    /// Unchanged tables whose fresh `TableInfo` should be written into the
    /// cache transparently, keyed by `(database, schema, table)`. Provided so
    /// that the "Refresh & re-run" handler can update all tables in one pass.
    pub refreshes: Vec<(crate::schema::TableKey, crate::TableInfo)>,
}

/// A fail-closed rejection raised while normalizing selected table deltas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableAlterNormalizationError {
    pub reason: String,
}

impl TableAlterNormalizationError {
    fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

#[derive(Debug)]
struct SelectedColumnAlter {
    name: String,
    dropped: bool,
    new_type: Option<String>,
    nullable: Option<bool>,
    default: Option<OwnedDefaultSpec>,
    default_selected: bool,
    expected_type: Option<String>,
    expected_nullable: Option<bool>,
    expected_default: Option<Option<String>>,
}

impl SelectedColumnAlter {
    fn new(name: String) -> Self {
        Self {
            name,
            dropped: false,
            new_type: None,
            nullable: None,
            default: None,
            default_selected: false,
            expected_type: None,
            expected_nullable: None,
            expected_default: None,
        }
    }

    fn reject_if_dropped(&self) -> Result<(), TableAlterNormalizationError> {
        if self.dropped {
            return Err(TableAlterNormalizationError::new(format!(
                "column {} is selected for removal and alteration",
                self.name
            )));
        }
        Ok(())
    }
}

/// Normalize selected per-column differences into one complete table request.
///
/// The output only contains selected fields. Any duplicate, conflicting, missing,
/// or unsupported selected change rejects the entire request so callers cannot
/// execute a supported prefix of an incoherent table alteration.
pub fn normalize_selected_table_alter(
    table: TableRef,
    selected: &[SchemaChange],
) -> Result<TableAlterRequest, TableAlterNormalizationError> {
    validate_identifier(&table.name, "table")?;
    if selected.is_empty() {
        return Err(TableAlterNormalizationError::new(
            "at least one selected table alteration is required",
        ));
    }

    let mut columns = Vec::<SelectedColumnAlter>::new();
    for change in selected {
        match change {
            SchemaChange::ColumnAdded(column) => {
                return unsupported_selected_change(format!(
                    "adding column {} cannot be mixed into a table alteration request",
                    column.name
                ));
            }
            SchemaChange::PrimaryKeyChanged { .. }
            | SchemaChange::ForeignKeyChanged
            | SchemaChange::IndexAdded(_)
            | SchemaChange::IndexRemoved(_) => {
                return unsupported_selected_change(
                    "selected index or constraint changes cannot be mixed into a table alteration request",
                );
            }
            SchemaChange::ColumnRemoved(before) => {
                validate_identifier(&before.name, "column")?;
                let column = selected_column(&mut columns, &before.name)?;
                if column.dropped
                    || column.new_type.is_some()
                    || column.nullable.is_some()
                    || column.default_selected
                {
                    return Err(TableAlterNormalizationError::new(format!(
                        "column {} has duplicate or conflicting selected changes",
                        before.name
                    )));
                }
                column.dropped = true;
                column.expected_type = Some(before.type_name.clone());
                column.expected_nullable = Some(before.nullable);
                column.expected_default = Some(before.default_value.clone());
            }
            SchemaChange::ColumnTypeChanged { before, after } => {
                validate_matching_column_names(before, after)?;
                let column = selected_column(&mut columns, &before.name)?;
                column.reject_if_dropped()?;
                if column.new_type.is_some() {
                    return Err(TableAlterNormalizationError::new(format!(
                        "column {} has duplicate selected type changes",
                        before.name
                    )));
                }
                column.new_type = Some(after.type_name.clone());
                column.expected_type = Some(before.type_name.clone());
            }
            SchemaChange::NullabilityChanged {
                column: name,
                before,
                after,
            } => {
                validate_identifier(name, "column")?;
                let column = selected_column(&mut columns, name)?;
                column.reject_if_dropped()?;
                if column.nullable.is_some() {
                    return Err(TableAlterNormalizationError::new(format!(
                        "column {name} has duplicate selected nullability changes"
                    )));
                }
                column.nullable = Some(*after);
                column.expected_nullable = Some(*before);
            }
            SchemaChange::DefaultChanged {
                column: name,
                before,
                after,
            } => {
                validate_identifier(name, "column")?;
                let column = selected_column(&mut columns, name)?;
                column.reject_if_dropped()?;
                if column.default_selected {
                    return Err(TableAlterNormalizationError::new(format!(
                        "column {name} has duplicate selected default changes"
                    )));
                }
                column.default = Some(match after {
                    Some(value) => OwnedDefaultSpec::Set(value.clone()),
                    None => OwnedDefaultSpec::Drop,
                });
                column.default_selected = true;
                column.expected_default = Some(before.clone());
            }
        }
    }

    let mut operations = Vec::with_capacity(columns.len());
    let mut expected_before = Vec::with_capacity(columns.len());
    for column in columns {
        let name = column.name.clone();
        if column.dropped {
            operations.push(TableAlterOperation::DropColumn { name });
        } else {
            operations.push(TableAlterOperation::AlterColumn {
                name,
                new_type: column.new_type,
                nullable: column.nullable,
                default: column.default,
            });
        }
        expected_before.push(TableAlterExpectedColumn {
            name: column.name,
            type_name: column.expected_type,
            nullable: column.expected_nullable,
            default: column.expected_default,
        });
    }

    Ok(TableAlterRequest {
        table,
        operations,
        expected_before,
    })
}

fn unsupported_selected_change<T>(
    reason: impl Into<String>,
) -> Result<T, TableAlterNormalizationError> {
    Err(TableAlterNormalizationError::new(reason))
}

fn selected_column<'a>(
    columns: &'a mut Vec<SelectedColumnAlter>,
    name: &str,
) -> Result<&'a mut SelectedColumnAlter, TableAlterNormalizationError> {
    if let Some(index) = columns.iter().position(|column| column.name == name) {
        return columns.get_mut(index).ok_or_else(|| {
            TableAlterNormalizationError::new("selected column was unavailable by index")
        });
    }

    columns.push(SelectedColumnAlter::new(name.to_string()));
    columns.last_mut().ok_or_else(|| {
        TableAlterNormalizationError::new("selected column was unavailable after insertion")
    })
}

fn validate_matching_column_names(
    before: &ColumnSnapshot,
    after: &ColumnSnapshot,
) -> Result<(), TableAlterNormalizationError> {
    validate_identifier(&before.name, "column")?;
    validate_identifier(&after.name, "column")?;
    if before.name != after.name {
        return Err(TableAlterNormalizationError::new(format!(
            "selected type change has mismatched columns {} and {}",
            before.name, after.name
        )));
    }
    Ok(())
}

fn validate_identifier(value: &str, kind: &str) -> Result<(), TableAlterNormalizationError> {
    if value.trim().is_empty() {
        return Err(TableAlterNormalizationError::new(format!(
            "selected {kind} identifier must not be empty"
        )));
    }
    Ok(())
}

/// Compute the list of schema changes between `before` and `after` for one table.
///
/// Both arguments are expected to have their `columns` and `foreign_keys` fields
/// populated (`Some`). If either is `None`, that aspect is treated as an empty
/// set and any difference relative to the other snapshot is reported accordingly.
pub fn diff_table_info(before: &TableInfo, after: &TableInfo) -> Vec<SchemaChange> {
    let mut changes = Vec::new();

    let before_cols: Vec<ColumnSnapshot> = before
        .columns
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|c| ColumnSnapshot {
            name: c.name.clone(),
            type_name: c.type_name.clone(),
            nullable: c.nullable,
            is_primary_key: c.is_primary_key,
            default_value: c.default_value.clone(),
        })
        .collect();

    let after_cols: Vec<ColumnSnapshot> = after
        .columns
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|c| ColumnSnapshot {
            name: c.name.clone(),
            type_name: c.type_name.clone(),
            nullable: c.nullable,
            is_primary_key: c.is_primary_key,
            default_value: c.default_value.clone(),
        })
        .collect();

    // Detect removed and changed columns (iterate before).
    for bc in &before_cols {
        match after_cols.iter().find(|ac| ac.name == bc.name) {
            None => changes.push(SchemaChange::ColumnRemoved(bc.clone())),
            Some(ac) => {
                if ac.type_name != bc.type_name {
                    changes.push(SchemaChange::ColumnTypeChanged {
                        before: bc.clone(),
                        after: ac.clone(),
                    });
                }

                // A column can change both its type and its nullability at
                // once (e.g. `text NULL` -> `varchar NOT NULL`). These are
                // independent deltas: coupling them with `else if` would drop
                // the NOT NULL change whenever the type also changed, so the
                // apply plan would silently keep the column nullable.
                if ac.nullable != bc.nullable {
                    changes.push(SchemaChange::NullabilityChanged {
                        column: bc.name.clone(),
                        before: bc.nullable,
                        after: ac.nullable,
                    });
                }

                if ac.default_value != bc.default_value {
                    changes.push(SchemaChange::DefaultChanged {
                        column: bc.name.clone(),
                        before: bc.default_value.clone(),
                        after: ac.default_value.clone(),
                    });
                }
            }
        }
    }

    // Detect added columns (in after but not in before).
    for ac in &after_cols {
        if !before_cols.iter().any(|bc| bc.name == ac.name) {
            changes.push(SchemaChange::ColumnAdded(ac.clone()));
        }
    }

    // Detect primary key set changes.
    let before_pks: Vec<String> = before_cols
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.name.clone())
        .collect();

    let after_pks: Vec<String> = after_cols
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.name.clone())
        .collect();

    if before_pks != after_pks {
        changes.push(SchemaChange::PrimaryKeyChanged {
            before: before_pks,
            after: after_pks,
        });
    }

    // Detect foreign key set changes (simplified: any difference triggers the
    // variant). Compared as a SET rather than positionally: drivers are not
    // guaranteed to return foreign keys in a stable order, so a positional
    // zip would report a false change whenever the same FK set comes back in
    // a different order.
    let before_fks = before.foreign_keys.as_deref().unwrap_or(&[]);
    let after_fks = after.foreign_keys.as_deref().unwrap_or(&[]);

    let mut before_fk_signatures: Vec<_> = before_fks.iter().map(foreign_key_signature).collect();
    let mut after_fk_signatures: Vec<_> = after_fks.iter().map(foreign_key_signature).collect();
    before_fk_signatures.sort();
    after_fk_signatures.sort();

    if before_fk_signatures != after_fk_signatures {
        changes.push(SchemaChange::ForeignKeyChanged);
    }

    // Detect non-primary-key index add/remove by name identity. Primary key
    // indexes are excluded since `PrimaryKeyChanged` already covers them.
    let before_indexes = relational_indexes(before);
    let after_indexes = relational_indexes(after);

    for bi in &before_indexes {
        if !after_indexes.iter().any(|ai| ai.name == bi.name) {
            changes.push(SchemaChange::IndexRemoved(IndexSnapshot::from(*bi)));
        }
    }

    for ai in &after_indexes {
        if !before_indexes.iter().any(|bi| bi.name == ai.name) {
            changes.push(SchemaChange::IndexAdded(IndexSnapshot::from(*ai)));
        }
    }

    changes
}

/// Comparable, order-independent identity for a foreign key: the fields that
/// determine whether two FKs describe the same relationship. `name`,
/// `on_delete`, and `on_update` are intentionally excluded, matching the
/// fields the previous positional comparison inspected.
fn foreign_key_signature(
    fk: &ForeignKeyInfo,
) -> (Vec<String>, String, Option<String>, Vec<String>) {
    (
        fk.columns.clone(),
        fk.referenced_table.clone(),
        fk.referenced_schema.clone(),
        fk.referenced_columns.clone(),
    )
}

/// Non-primary-key indexes declared on `table`, treating a missing/lazy or
/// document-shaped `indexes` field as an empty set (mirrors the columns and
/// foreign-key handling above).
fn relational_indexes(table: &TableInfo) -> Vec<&IndexInfo> {
    match table.indexes.as_ref() {
        Some(IndexData::Relational(indexes)) => {
            indexes.iter().filter(|idx| !idx.is_primary).collect()
        }
        _ => Vec::new(),
    }
}

/// Map a single detected `SchemaChange` onto the shared governance ladder in
/// `dbflux_policy`. `PrimaryKeyChanged` and `ForeignKeyChanged` are constraint
/// mutations under the hood (DROP/ADD CONSTRAINT), so both classify as
/// `AddConstraint`.
fn classify_schema_change(change: &SchemaChange) -> dbflux_policy::ExecutionClassification {
    use dbflux_policy::{SchemaAlterKind, classify_schema_alter};

    let kind = match change {
        SchemaChange::ColumnAdded(column) => SchemaAlterKind::AddColumn {
            safe: column.nullable || column.default_value.is_some(),
        },
        SchemaChange::ColumnRemoved(_) => SchemaAlterKind::DropColumn,
        SchemaChange::ColumnTypeChanged { .. } => SchemaAlterKind::AlterColumn,
        SchemaChange::NullabilityChanged { .. } => SchemaAlterKind::AlterColumn,
        SchemaChange::DefaultChanged { .. } => SchemaAlterKind::AlterColumn,
        SchemaChange::PrimaryKeyChanged { .. } => SchemaAlterKind::AddConstraint,
        SchemaChange::ForeignKeyChanged => SchemaAlterKind::AddConstraint,
        SchemaChange::IndexAdded(_) => SchemaAlterKind::AddIndex,
        SchemaChange::IndexRemoved(_) => SchemaAlterKind::DropIndex,
    };

    classify_schema_alter(kind)
}

/// Risk classification for a whole-table `CREATE`, using the same
/// `dbflux_policy` governance ladder as `classify_schema_change` for
/// column/index changes. Exposed publicly (unlike `classify_schema_change`)
/// so callers outside `dbflux_core` — which may not depend on `dbflux_policy`
/// directly, e.g. non-`mcp` builds of `dbflux_ui_document` — can classify
/// `TableChange::TableAdded`/`TableRemoved` without a direct `dbflux_policy`
/// dependency.
pub fn classify_table_added() -> dbflux_policy::ExecutionClassification {
    use dbflux_policy::{SchemaAlterKind, classify_schema_alter};

    classify_schema_alter(SchemaAlterKind::AddTable)
}

/// Risk classification for a whole-table `DROP`. See `classify_table_added`.
pub fn classify_table_removed() -> dbflux_policy::ExecutionClassification {
    use dbflux_policy::{SchemaAlterKind, classify_schema_alter};

    classify_schema_alter(SchemaAlterKind::DropTable)
}

fn table_identity(table: &TableInfo) -> (Option<&str>, &str) {
    (table.schema.as_deref(), table.name.as_str())
}

/// Compute the list of table-level changes between two full schema snapshots.
///
/// Tables are matched by `(schema, name)` identity. A table present in `after`
/// but not `before` is `TableAdded`; the reverse is `TableRemoved`. A rename
/// is therefore reported as a removal of the old name plus an addition of the
/// new one, matching `diff_table_info`'s column-rename identity rule. Matched
/// tables are diffed via `diff_table_info`, with each resulting `SchemaChange`
/// classified into a `RiskedChange` via `classify_schema_change`. Identical
/// schemas yield an empty diff.
pub fn diff_schema(before: &[TableInfo], after: &[TableInfo]) -> Vec<TableChange> {
    let mut table_changes = Vec::new();

    for before_table in before {
        let before_key = table_identity(before_table);
        match after
            .iter()
            .find(|after_table| table_identity(after_table) == before_key)
        {
            None => {
                table_changes.push(TableChange::TableRemoved(TableRef {
                    schema: before_table.schema.clone(),
                    name: before_table.name.clone(),
                }));
            }
            Some(after_table) => {
                let raw_changes = diff_table_info(before_table, after_table);
                if raw_changes.is_empty() {
                    continue;
                }

                let risked_changes = raw_changes
                    .into_iter()
                    .map(|change| RiskedChange {
                        risk: classify_schema_change(&change),
                        change,
                    })
                    .collect();

                table_changes.push(TableChange::TableModified {
                    table: TableRef {
                        schema: before_table.schema.clone(),
                        name: before_table.name.clone(),
                    },
                    changes: risked_changes,
                });
            }
        }
    }

    for after_table in after {
        let after_key = table_identity(after_table);
        let existed_before = before
            .iter()
            .any(|before_table| table_identity(before_table) == after_key);

        if !existed_before {
            table_changes.push(TableChange::TableAdded(after_table.clone()));
        }
    }

    table_changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ColumnInfo, ForeignKeyInfo, IndexData, IndexInfo, OwnedDefaultSpec, TableAlterOperation,
        TableInfo,
    };

    fn make_table(columns: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            name: "users".to_string(),
            schema: Some("public".to_string()),
            columns: Some(columns),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
            storage_hints: None,
        }
    }

    fn make_table_named(name: &str, schema: Option<&str>, columns: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            schema: schema.map(str::to_string),
            columns: Some(columns),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
            storage_hints: None,
        }
    }

    fn make_table_with_indexes(columns: Vec<ColumnInfo>, indexes: Vec<IndexInfo>) -> TableInfo {
        TableInfo {
            indexes: Some(IndexData::Relational(indexes)),
            ..make_table(columns)
        }
    }

    fn col(name: &str, type_name: &str, nullable: bool, is_pk: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            type_name: type_name.to_string(),
            nullable,
            is_primary_key: is_pk,
            default_value: None,
            enum_values: None,
        }
    }

    fn col_with_default(
        name: &str,
        type_name: &str,
        nullable: bool,
        is_pk: bool,
        default_value: Option<&str>,
    ) -> ColumnInfo {
        ColumnInfo {
            default_value: default_value.map(str::to_string),
            ..col(name, type_name, nullable, is_pk)
        }
    }

    fn index(name: &str, columns: &[&str], is_unique: bool, is_primary: bool) -> IndexInfo {
        IndexInfo {
            name: name.to_string(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
            is_unique,
            is_primary,
        }
    }

    #[test]
    fn selected_type_delta_ignores_unselected_after_properties() {
        let before = ColumnSnapshot {
            name: "status".to_string(),
            type_name: "TEXT".to_string(),
            nullable: true,
            is_primary_key: false,
            default_value: None,
        };
        let after = ColumnSnapshot {
            name: "status".to_string(),
            type_name: "INTEGER".to_string(),
            nullable: false,
            is_primary_key: true,
            default_value: Some("'not-selected'".to_string()),
        };

        let request = normalize_selected_table_alter(
            TableRef::new("users"),
            &[SchemaChange::ColumnTypeChanged { before, after }],
        )
        .expect("a selected type change should normalize");

        assert_eq!(
            request.operations,
            vec![TableAlterOperation::AlterColumn {
                name: "status".to_string(),
                new_type: Some("INTEGER".to_string()),
                nullable: None,
                default: None,
            }]
        );
        assert_eq!(
            request.expected_before,
            vec![TableAlterExpectedColumn {
                name: "status".to_string(),
                type_name: Some("TEXT".to_string()),
                nullable: None,
                default: None,
            }]
        );
    }

    #[test]
    fn selected_deltas_coalesce_only_selected_column_properties() {
        let type_before = ColumnSnapshot {
            name: "status".to_string(),
            type_name: "TEXT".to_string(),
            nullable: true,
            is_primary_key: false,
            default_value: None,
        };
        let type_after = ColumnSnapshot {
            type_name: "INTEGER".to_string(),
            ..type_before.clone()
        };
        let request = normalize_selected_table_alter(
            TableRef::with_schema("main", "users"),
            &[
                SchemaChange::ColumnTypeChanged {
                    before: type_before,
                    after: type_after,
                },
                SchemaChange::NullabilityChanged {
                    column: "status".to_string(),
                    before: true,
                    after: false,
                },
                SchemaChange::DefaultChanged {
                    column: "status".to_string(),
                    before: None,
                    after: Some("NULL".to_string()),
                },
            ],
        )
        .expect("disjoint selected changes should normalize");

        assert_eq!(request.operations.len(), 1);
        assert_eq!(
            request.operations,
            vec![TableAlterOperation::AlterColumn {
                name: "status".to_string(),
                new_type: Some("INTEGER".to_string()),
                nullable: Some(false),
                default: Some(OwnedDefaultSpec::Set("NULL".to_string())),
            }]
        );
        assert_eq!(request.expected_before.len(), 1);
        assert_eq!(
            request.expected_before[0].type_name.as_deref(),
            Some("TEXT")
        );
        assert_eq!(request.expected_before[0].nullable, Some(true));
        assert_eq!(request.expected_before[0].default, Some(None));
    }

    #[test]
    fn selected_delta_normalizer_rejects_duplicate_conflicting_and_unsupported_changes() {
        let column = ColumnSnapshot {
            name: "status".to_string(),
            type_name: "TEXT".to_string(),
            nullable: true,
            is_primary_key: false,
            default_value: None,
        };
        let changed = ColumnSnapshot {
            type_name: "INTEGER".to_string(),
            ..column.clone()
        };
        let type_change = SchemaChange::ColumnTypeChanged {
            before: column.clone(),
            after: changed,
        };
        let nullable_change = SchemaChange::NullabilityChanged {
            column: "status".to_string(),
            before: true,
            after: false,
        };
        let default_change = SchemaChange::DefaultChanged {
            column: "status".to_string(),
            before: None,
            after: Some("'pending'".to_string()),
        };
        let missing_identifier = SchemaChange::ColumnTypeChanged {
            before: column.clone(),
            after: ColumnSnapshot {
                name: String::new(),
                type_name: "INTEGER".to_string(),
                nullable: true,
                is_primary_key: false,
                default_value: None,
            },
        };
        let index = IndexSnapshot {
            name: "idx_status".to_string(),
            columns: vec!["status".to_string()],
            is_unique: false,
        };

        let invalid_selections = [
            (
                "duplicate type",
                vec![type_change.clone(), type_change.clone()],
            ),
            (
                "duplicate nullability",
                vec![nullable_change.clone(), nullable_change.clone()],
            ),
            (
                "duplicate default",
                vec![default_change.clone(), default_change.clone()],
            ),
            (
                "drop followed by alter",
                vec![
                    SchemaChange::ColumnRemoved(column.clone()),
                    type_change.clone(),
                ],
            ),
            (
                "alter followed by drop",
                vec![type_change, SchemaChange::ColumnRemoved(column.clone())],
            ),
            (
                "mixed add",
                vec![
                    SchemaChange::ColumnRemoved(column.clone()),
                    SchemaChange::ColumnAdded(ColumnSnapshot {
                        name: "new_status".to_string(),
                        ..column.clone()
                    }),
                ],
            ),
            (
                "mixed index",
                vec![
                    SchemaChange::ColumnRemoved(column.clone()),
                    SchemaChange::IndexAdded(index),
                ],
            ),
            (
                "mixed primary key constraint",
                vec![
                    SchemaChange::ColumnRemoved(column.clone()),
                    SchemaChange::PrimaryKeyChanged {
                        before: vec!["id".to_string()],
                        after: vec!["status".to_string()],
                    },
                ],
            ),
            (
                "mixed foreign key constraint",
                vec![
                    SchemaChange::ColumnRemoved(column.clone()),
                    SchemaChange::ForeignKeyChanged,
                ],
            ),
            ("missing column identifier", vec![missing_identifier]),
        ];

        for (description, selected) in invalid_selections {
            assert!(
                normalize_selected_table_alter(TableRef::new("users"), &selected).is_err(),
                "{description} must reject the selected table request: {selected:?}"
            );
        }
        assert!(
            normalize_selected_table_alter(TableRef::new("users"), &[]).is_err(),
            "an empty selected list must reject"
        );
        assert!(
            normalize_selected_table_alter(
                TableRef::new(""),
                &[SchemaChange::ColumnRemoved(column)],
            )
            .is_err(),
            "an empty table identifier must reject"
        );
    }

    #[test]
    fn selected_delta_normalizer_keeps_absent_default_distinct_from_sql_null_and_drop() {
        let absent_default = normalize_selected_table_alter(
            TableRef::new("users"),
            &[SchemaChange::DefaultChanged {
                column: "status".to_string(),
                before: Some("NULL".to_string()),
                after: None,
            }],
        )
        .expect("dropping a default should normalize");
        let explicit_null = normalize_selected_table_alter(
            TableRef::new("users"),
            &[SchemaChange::DefaultChanged {
                column: "status".to_string(),
                before: None,
                after: Some("NULL".to_string()),
            }],
        )
        .expect("an explicit SQL NULL default should normalize");

        assert_eq!(
            absent_default.operations,
            vec![TableAlterOperation::AlterColumn {
                name: "status".to_string(),
                new_type: None,
                nullable: None,
                default: Some(OwnedDefaultSpec::Drop),
            }]
        );
        assert_eq!(
            absent_default.expected_before[0].default,
            Some(Some("NULL".to_string()))
        );
        assert_eq!(
            explicit_null.operations[0],
            TableAlterOperation::AlterColumn {
                name: "status".to_string(),
                new_type: None,
                nullable: None,
                default: Some(OwnedDefaultSpec::Set("NULL".to_string())),
            }
        );
        assert_eq!(explicit_null.expected_before[0].default, Some(None));
    }

    #[test]
    fn no_changes_returns_empty() {
        let table = make_table(vec![col("id", "integer", false, true)]);
        assert!(diff_table_info(&table, &table).is_empty());
    }

    #[test]
    fn column_added_detected() {
        let before = make_table(vec![col("id", "integer", false, true)]);
        let after = make_table(vec![
            col("id", "integer", false, true),
            col("email", "text", false, false),
        ]);
        let changes = diff_table_info(&before, &after);
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, SchemaChange::ColumnAdded(s) if s.name == "email"))
        );
    }

    #[test]
    fn column_removed_detected() {
        let before = make_table(vec![
            col("id", "integer", false, true),
            col("email", "text", false, false),
        ]);
        let after = make_table(vec![col("id", "integer", false, true)]);
        let changes = diff_table_info(&before, &after);
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, SchemaChange::ColumnRemoved(s) if s.name == "email"))
        );
    }

    #[test]
    fn column_type_changed_detected() {
        let before = make_table(vec![col("id", "integer", false, true)]);
        let after = make_table(vec![col("id", "bigint", false, true)]);
        let changes = diff_table_info(&before, &after);
        assert!(changes.iter().any(|c| matches!(
            c,
            SchemaChange::ColumnTypeChanged { before, after }
            if before.type_name == "integer" && after.type_name == "bigint"
        )));
    }

    #[test]
    fn nullability_changed_detected() {
        let before = make_table(vec![col("email", "text", true, false)]);
        let after = make_table(vec![col("email", "text", false, false)]);
        let changes = diff_table_info(&before, &after);
        assert!(changes.iter().any(|c| matches!(
            c,
            SchemaChange::NullabilityChanged { column, before: b, after: a }
            if column == "email" && *b && !*a
        )));
    }

    #[test]
    fn combined_type_and_nullability_change_emits_both() {
        let before = make_table(vec![col("age", "text", true, false)]);
        let after = make_table(vec![col("age", "bigint", false, false)]);
        let changes = diff_table_info(&before, &after);

        assert!(
            changes.iter().any(|c| matches!(
                c,
                SchemaChange::ColumnTypeChanged { before, after }
                if before.type_name == "text" && after.type_name == "bigint"
            )),
            "expected a type change, got {changes:?}"
        );
        assert!(
            changes.iter().any(|c| matches!(
                c,
                SchemaChange::NullabilityChanged { column, before: b, after: a }
                if column == "age" && *b && !*a
            )),
            "expected the NOT NULL delta to survive alongside the type change, got {changes:?}"
        );
    }

    #[test]
    fn combined_type_and_default_change_emits_both() {
        let before = make_table(vec![col_with_default("age", "text", false, false, None)]);
        let after = make_table(vec![col_with_default(
            "age",
            "bigint",
            false,
            false,
            Some("0"),
        )]);
        let changes = diff_table_info(&before, &after);

        assert!(
            changes
                .iter()
                .any(|c| matches!(c, SchemaChange::ColumnTypeChanged { .. }))
        );
        assert!(changes.iter().any(|c| matches!(
            c,
            SchemaChange::DefaultChanged { column, after, .. }
            if column == "age" && after.as_deref() == Some("0")
        )));
    }

    #[test]
    fn combined_type_nullability_and_default_change_emits_all_three() {
        let before = make_table(vec![col_with_default("age", "text", true, false, None)]);
        let after = make_table(vec![col_with_default(
            "age",
            "bigint",
            false,
            false,
            Some("0"),
        )]);
        let changes = diff_table_info(&before, &after);

        assert!(
            changes
                .iter()
                .any(|c| matches!(c, SchemaChange::ColumnTypeChanged { .. })),
            "missing type change: {changes:?}"
        );
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, SchemaChange::NullabilityChanged { .. })),
            "missing nullability change: {changes:?}"
        );
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, SchemaChange::DefaultChanged { .. })),
            "missing default change: {changes:?}"
        );
    }

    #[test]
    fn primary_key_changed_detected() {
        let before = make_table(vec![
            col("id", "integer", false, true),
            col("name", "text", false, false),
        ]);
        let after = make_table(vec![
            col("id", "integer", false, false),
            col("name", "text", false, true),
        ]);
        let changes = diff_table_info(&before, &after);
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, SchemaChange::PrimaryKeyChanged { .. }))
        );
    }

    #[test]
    fn foreign_key_changed_detected() {
        let before = make_table(vec![col("id", "integer", false, true)]);
        let mut after = before.clone();
        after.foreign_keys = Some(vec![ForeignKeyInfo {
            name: "fk_org".to_string(),
            columns: vec!["org_id".to_string()],
            referenced_table: "orgs".to_string(),
            referenced_schema: None,
            referenced_columns: vec!["id".to_string()],
            on_delete: None,
            on_update: None,
        }]);
        let changes = diff_table_info(&before, &after);
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, SchemaChange::ForeignKeyChanged))
        );
    }

    #[test]
    fn foreign_key_set_reordered_is_not_reported_as_changed() {
        let fk_org = ForeignKeyInfo {
            name: "fk_org".to_string(),
            columns: vec!["org_id".to_string()],
            referenced_table: "orgs".to_string(),
            referenced_schema: None,
            referenced_columns: vec!["id".to_string()],
            on_delete: None,
            on_update: None,
        };
        let fk_team = ForeignKeyInfo {
            name: "fk_team".to_string(),
            columns: vec!["team_id".to_string()],
            referenced_table: "teams".to_string(),
            referenced_schema: None,
            referenced_columns: vec!["id".to_string()],
            on_delete: None,
            on_update: None,
        };

        let mut before = make_table(vec![col("id", "integer", false, true)]);
        before.foreign_keys = Some(vec![fk_org.clone(), fk_team.clone()]);

        let mut after = before.clone();
        after.foreign_keys = Some(vec![fk_team, fk_org]);

        let changes = diff_table_info(&before, &after);
        assert!(
            !changes
                .iter()
                .any(|c| matches!(c, SchemaChange::ForeignKeyChanged)),
            "same FK set in a different order must not report ForeignKeyChanged, got {changes:?}"
        );
    }

    #[test]
    fn default_value_changed_detected() {
        let before = make_table(vec![col_with_default("status", "text", false, false, None)]);
        let after = make_table(vec![col_with_default(
            "status",
            "text",
            false,
            false,
            Some("'active'"),
        )]);
        let changes = diff_table_info(&before, &after);
        assert!(changes.iter().any(|c| matches!(
            c,
            SchemaChange::DefaultChanged { column, before: b, after: a }
            if column == "status" && b.is_none() && a.as_deref() == Some("'active'")
        )));
    }

    #[test]
    fn index_added_detected() {
        let before = make_table_with_indexes(vec![col("id", "integer", false, true)], vec![]);
        let after = make_table_with_indexes(
            vec![col("id", "integer", false, true)],
            vec![index("idx_users_email", &["email"], true, false)],
        );
        let changes = diff_table_info(&before, &after);
        assert!(changes.iter().any(|c| matches!(
            c,
            SchemaChange::IndexAdded(snapshot) if snapshot.name == "idx_users_email"
        )));
    }

    #[test]
    fn index_removed_detected() {
        let before = make_table_with_indexes(
            vec![col("id", "integer", false, true)],
            vec![index("idx_users_email", &["email"], true, false)],
        );
        let after = make_table_with_indexes(vec![col("id", "integer", false, true)], vec![]);
        let changes = diff_table_info(&before, &after);
        assert!(changes.iter().any(|c| matches!(
            c,
            SchemaChange::IndexRemoved(snapshot) if snapshot.name == "idx_users_email"
        )));
    }

    #[test]
    fn primary_key_index_excluded_from_index_diff() {
        let before = make_table_with_indexes(vec![col("id", "integer", false, true)], vec![]);
        let after = make_table_with_indexes(
            vec![col("id", "integer", false, true)],
            vec![index("users_pkey", &["id"], true, true)],
        );
        let changes = diff_table_info(&before, &after);
        assert!(
            !changes
                .iter()
                .any(|c| matches!(c, SchemaChange::IndexAdded(_)))
        );
    }

    #[test]
    fn diff_schema_identical_schemas_yield_empty_diff() {
        let before = make_table(vec![col("id", "integer", false, true)]);
        let after = make_table(vec![col("id", "integer", false, true)]);
        assert!(diff_schema(&[before], &[after]).is_empty());
    }

    #[test]
    fn diff_schema_table_added_detected() {
        let before = vec![make_table_named(
            "users",
            Some("public"),
            vec![col("id", "integer", false, true)],
        )];
        let after = vec![
            before[0].clone(),
            make_table_named(
                "orders",
                Some("public"),
                vec![col("id", "integer", false, true)],
            ),
        ];
        let changes = diff_schema(&before, &after);
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, TableChange::TableAdded(t) if t.name == "orders"))
        );
    }

    #[test]
    fn diff_schema_table_removed_detected() {
        let users = make_table_named(
            "users",
            Some("public"),
            vec![col("id", "integer", false, true)],
        );
        let orders = make_table_named(
            "orders",
            Some("public"),
            vec![col("id", "integer", false, true)],
        );
        let before = vec![users.clone(), orders];
        let after = vec![users];
        let changes = diff_schema(&before, &after);
        assert!(changes.iter().any(|c| matches!(
            c,
            TableChange::TableRemoved(table_ref) if table_ref.name == "orders"
        )));
    }

    #[test]
    fn diff_schema_table_modified_reports_risked_changes() {
        use dbflux_policy::ExecutionClassification;

        let before = vec![make_table_named(
            "users",
            Some("public"),
            vec![col("id", "integer", false, true)],
        )];
        let after = vec![make_table_named(
            "users",
            Some("public"),
            vec![
                col("id", "integer", false, true),
                col("email", "text", true, false),
            ],
        )];
        let changes = diff_schema(&before, &after);
        let modified = changes
            .iter()
            .find_map(|c| match c {
                TableChange::TableModified { table, changes } if table.name == "users" => {
                    Some(changes)
                }
                _ => None,
            })
            .expect("expected a TableModified entry for users");

        assert!(modified.iter().any(|risked| matches!(
            &risked.change,
            SchemaChange::ColumnAdded(col) if col.name == "email"
        ) && risked.risk
            == ExecutionClassification::AdminSafe));
    }

    #[test]
    fn diff_schema_rename_is_drop_plus_add() {
        let before = vec![make_table_named(
            "legacy_users",
            Some("public"),
            vec![col("id", "integer", false, true)],
        )];
        let after = vec![make_table_named(
            "users",
            Some("public"),
            vec![col("id", "integer", false, true)],
        )];
        let changes = diff_schema(&before, &after);
        assert!(changes.iter().any(
            |c| matches!(c, TableChange::TableRemoved(table_ref) if table_ref.name == "legacy_users")
        ));
        assert!(
            changes
                .iter()
                .any(|c| matches!(c, TableChange::TableAdded(t) if t.name == "users"))
        );
    }

    #[test]
    fn classify_table_added_is_admin_safe() {
        use dbflux_policy::ExecutionClassification;

        assert_eq!(classify_table_added(), ExecutionClassification::AdminSafe);
    }

    #[test]
    fn classify_table_removed_is_admin_destructive() {
        use dbflux_policy::ExecutionClassification;

        assert_eq!(
            classify_table_removed(),
            ExecutionClassification::AdminDestructive
        );
    }
}
