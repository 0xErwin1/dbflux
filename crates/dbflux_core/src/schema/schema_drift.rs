use crate::TableInfo;
use crate::schema::query_parser::QueryTableRef;

/// Snapshot of a column's schema-relevant properties.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSnapshot {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub is_primary_key: bool,
}

/// A diff between two column snapshots.
#[derive(Debug, Clone)]
pub struct ColumnDiff {
    pub column: String,
    pub before: ColumnSnapshot,
    pub after: ColumnSnapshot,
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
    PrimaryKeyChanged {
        before: Vec<String>,
        after: Vec<String>,
    },
    /// Simplified FK change — reports any FK set mutation without per-key detail.
    ForeignKeyChanged,
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
    /// cache transparently, keyed by `(database, table)`. Provided so that
    /// the "Refresh & re-run" handler can update all tables in one pass.
    pub refreshes: Vec<((String, String), crate::TableInfo)>,
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
                } else if ac.nullable != bc.nullable {
                    changes.push(SchemaChange::NullabilityChanged {
                        column: bc.name.clone(),
                        before: bc.nullable,
                        after: ac.nullable,
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

    // Detect foreign key set changes (simplified: any difference triggers the variant).
    let before_fks = before.foreign_keys.as_deref().unwrap_or(&[]);
    let after_fks = after.foreign_keys.as_deref().unwrap_or(&[]);

    let fk_changed = before_fks.len() != after_fks.len()
        || before_fks.iter().zip(after_fks.iter()).any(|(b, a)| {
            b.columns != a.columns
                || b.referenced_table != a.referenced_table
                || b.referenced_schema != a.referenced_schema
                || b.referenced_columns != a.referenced_columns
        });

    if fk_changed {
        changes.push(SchemaChange::ForeignKeyChanged);
    }

    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnInfo, ForeignKeyInfo, TableInfo};

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
}
