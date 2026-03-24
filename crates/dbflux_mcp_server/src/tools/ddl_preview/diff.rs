use super::types::*;
use dbflux_core::DbError;
use std::collections::HashSet;

/// Compute schema differences between before and after snapshots.
pub fn compute_schema_diff(
    before: &SchemaStateSnapshot,
    after: &SchemaStateSnapshot,
) -> Result<SchemaDiff, DbError> {
    let mut diff = SchemaDiff::empty();

    // Compute table changes
    compute_table_diff(&before.tables, &after.tables, &mut diff)?;

    // Compute index changes
    compute_index_diff(&before.indexes, &after.indexes, &mut diff)?;

    // Compute foreign key changes
    compute_foreign_key_diff(&before.foreign_keys, &after.foreign_keys, &mut diff)?;

    Ok(diff)
}

/// Compute table-level differences.
fn compute_table_diff(
    before: &std::collections::HashMap<String, TableSnapshot>,
    after: &std::collections::HashMap<String, TableSnapshot>,
    diff: &mut SchemaDiff,
) -> Result<(), DbError> {
    let before_tables: HashSet<_> = before.keys().collect();
    let after_tables: HashSet<_> = after.keys().collect();

    // Tables created
    for table_name in after_tables.difference(&before_tables) {
        let table = &after[*table_name];
        diff.tables_created.push(TableDiffEntry {
            schema: table.schema.clone(),
            name: table.name.clone(),
            column_count: table.columns.len(),
        });

        // Add all columns as new
        for col in &table.columns {
            diff.columns_added.push(ColumnDiffEntry {
                table_schema: table.schema.clone(),
                table_name: table.name.clone(),
                column_name: col.name.clone(),
                data_type: col.data_type.clone(),
                nullable: col.nullable,
            });
        }
    }

    // Tables dropped
    for table_name in before_tables.difference(&after_tables) {
        let table = &before[*table_name];
        diff.tables_dropped.push(TableDiffEntry {
            schema: table.schema.clone(),
            name: table.name.clone(),
            column_count: table.columns.len(),
        });

        // Add all columns as dropped
        for col in &table.columns {
            diff.columns_dropped.push(ColumnDiffEntry {
                table_schema: table.schema.clone(),
                table_name: table.name.clone(),
                column_name: col.name.clone(),
                data_type: col.data_type.clone(),
                nullable: col.nullable,
            });
        }
    }

    // Tables altered (exist in both)
    for table_name in before_tables.intersection(&after_tables) {
        let before_table = &before[*table_name];
        let after_table = &after[*table_name];

        let mut changes = Vec::new();

        // Compare columns
        let before_cols: HashSet<_> = before_table.columns.iter().map(|c| &c.name).collect();
        let after_cols: HashSet<_> = after_table.columns.iter().map(|c| &c.name).collect();

        // Columns added to this table
        for col_name in after_cols.difference(&before_cols) {
            let col = after_table
                .columns
                .iter()
                .find(|c| &c.name == *col_name)
                .unwrap();
            diff.columns_added.push(ColumnDiffEntry {
                table_schema: after_table.schema.clone(),
                table_name: after_table.name.clone(),
                column_name: col.name.clone(),
                data_type: col.data_type.clone(),
                nullable: col.nullable,
            });
            changes.push(format!("Added column: {}", col.name));
        }

        // Columns dropped from this table
        for col_name in before_cols.difference(&after_cols) {
            let col = before_table
                .columns
                .iter()
                .find(|c| &c.name == *col_name)
                .unwrap();
            diff.columns_dropped.push(ColumnDiffEntry {
                table_schema: before_table.schema.clone(),
                table_name: before_table.name.clone(),
                column_name: col.name.clone(),
                data_type: col.data_type.clone(),
                nullable: col.nullable,
            });
            changes.push(format!("Dropped column: {}", col.name));
        }

        // Columns modified (exist in both)
        for col_name in before_cols.intersection(&after_cols) {
            let before_col = before_table
                .columns
                .iter()
                .find(|c| &c.name == *col_name)
                .unwrap();
            let after_col = after_table
                .columns
                .iter()
                .find(|c| &c.name == *col_name)
                .unwrap();

            if before_col.data_type != after_col.data_type
                || before_col.nullable != after_col.nullable
            {
                diff.columns_modified.push(ColumnModificationEntry {
                    table_schema: after_table.schema.clone(),
                    table_name: after_table.name.clone(),
                    column_name: after_col.name.clone(),
                    old_type: before_col.data_type.clone(),
                    new_type: after_col.data_type.clone(),
                    old_nullable: before_col.nullable,
                    new_nullable: after_col.nullable,
                });
                changes.push(format!("Modified column: {}", after_col.name));
            }
        }

        if !changes.is_empty() {
            diff.tables_altered.push(TableAlterationEntry {
                schema: after_table.schema.clone(),
                name: after_table.name.clone(),
                changes,
            });
        }
    }

    Ok(())
}

/// Compute index-level differences.
fn compute_index_diff(
    before: &std::collections::HashMap<String, IndexSnapshot>,
    after: &std::collections::HashMap<String, IndexSnapshot>,
    diff: &mut SchemaDiff,
) -> Result<(), DbError> {
    let before_indexes: HashSet<_> = before.keys().collect();
    let after_indexes: HashSet<_> = after.keys().collect();

    // Indexes created
    for index_name in after_indexes.difference(&before_indexes) {
        let index = &after[*index_name];
        diff.indexes_created.push(IndexDiffEntry {
            schema: index.schema.clone(),
            table_name: index.table_name.clone(),
            index_name: index.index_name.clone(),
            columns: index.columns.clone(),
            is_unique: index.is_unique,
        });
    }

    // Indexes dropped
    for index_name in before_indexes.difference(&after_indexes) {
        let index = &before[*index_name];
        diff.indexes_dropped.push(IndexDiffEntry {
            schema: index.schema.clone(),
            table_name: index.table_name.clone(),
            index_name: index.index_name.clone(),
            columns: index.columns.clone(),
            is_unique: index.is_unique,
        });
    }

    Ok(())
}

/// Compute foreign key differences.
fn compute_foreign_key_diff(
    before: &std::collections::HashMap<String, ForeignKeySnapshot>,
    after: &std::collections::HashMap<String, ForeignKeySnapshot>,
    diff: &mut SchemaDiff,
) -> Result<(), DbError> {
    let before_fks: HashSet<_> = before.keys().collect();
    let after_fks: HashSet<_> = after.keys().collect();

    // Foreign keys added
    for fk_name in after_fks.difference(&before_fks) {
        let fk = &after[*fk_name];
        diff.foreign_keys_added.push(ForeignKeyDiffEntry {
            schema: fk.schema.clone(),
            table_name: fk.table_name.clone(),
            constraint_name: fk.constraint_name.clone(),
            columns: fk.columns.clone(),
            referenced_table: fk.referenced_table.clone(),
            referenced_columns: fk.referenced_columns.clone(),
        });
    }

    // Foreign keys dropped
    for fk_name in before_fks.difference(&after_fks) {
        let fk = &before[*fk_name];
        diff.foreign_keys_dropped.push(ForeignKeyDiffEntry {
            schema: fk.schema.clone(),
            table_name: fk.table_name.clone(),
            constraint_name: fk.constraint_name.clone(),
            columns: fk.columns.clone(),
            referenced_table: fk.referenced_table.clone(),
            referenced_columns: fk.referenced_columns.clone(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_table(name: &str, columns: Vec<(&str, &str, bool)>) -> TableSnapshot {
        TableSnapshot {
            schema: None,
            name: name.to_string(),
            columns: columns
                .into_iter()
                .map(|(name, data_type, nullable)| ColumnSnapshot {
                    name: name.to_string(),
                    data_type: data_type.to_string(),
                    nullable,
                    default_value: None,
                })
                .collect(),
        }
    }

    #[test]
    fn test_compute_table_created() {
        let before = HashMap::new();
        let mut after = HashMap::new();
        after.insert(
            "users".to_string(),
            create_test_table("users", vec![("id", "integer", false)]),
        );

        let before_snap = SchemaStateSnapshot {
            tables: before,
            indexes: HashMap::new(),
            foreign_keys: HashMap::new(),
        };
        let after_snap = SchemaStateSnapshot {
            tables: after,
            indexes: HashMap::new(),
            foreign_keys: HashMap::new(),
        };

        let diff = compute_schema_diff(&before_snap, &after_snap).unwrap();
        assert_eq!(diff.tables_created.len(), 1);
        assert_eq!(diff.tables_created[0].name, "users");
        assert_eq!(diff.columns_added.len(), 1);
    }

    #[test]
    fn test_compute_table_dropped() {
        let mut before = HashMap::new();
        before.insert(
            "users".to_string(),
            create_test_table("users", vec![("id", "integer", false)]),
        );
        let after = HashMap::new();

        let before_snap = SchemaStateSnapshot {
            tables: before,
            indexes: HashMap::new(),
            foreign_keys: HashMap::new(),
        };
        let after_snap = SchemaStateSnapshot {
            tables: after,
            indexes: HashMap::new(),
            foreign_keys: HashMap::new(),
        };

        let diff = compute_schema_diff(&before_snap, &after_snap).unwrap();
        assert_eq!(diff.tables_dropped.len(), 1);
        assert_eq!(diff.tables_dropped[0].name, "users");
        assert_eq!(diff.columns_dropped.len(), 1);
    }

    #[test]
    fn test_compute_column_added() {
        let mut before = HashMap::new();
        before.insert(
            "users".to_string(),
            create_test_table("users", vec![("id", "integer", false)]),
        );

        let mut after = HashMap::new();
        after.insert(
            "users".to_string(),
            create_test_table(
                "users",
                vec![("id", "integer", false), ("name", "text", true)],
            ),
        );

        let before_snap = SchemaStateSnapshot {
            tables: before,
            indexes: HashMap::new(),
            foreign_keys: HashMap::new(),
        };
        let after_snap = SchemaStateSnapshot {
            tables: after,
            indexes: HashMap::new(),
            foreign_keys: HashMap::new(),
        };

        let diff = compute_schema_diff(&before_snap, &after_snap).unwrap();
        assert_eq!(diff.columns_added.len(), 1);
        assert_eq!(diff.columns_added[0].column_name, "name");
        assert_eq!(diff.tables_altered.len(), 1);
    }

    #[test]
    fn test_compute_column_modified() {
        let mut before = HashMap::new();
        before.insert(
            "users".to_string(),
            create_test_table("users", vec![("id", "integer", false)]),
        );

        let mut after = HashMap::new();
        after.insert(
            "users".to_string(),
            create_test_table("users", vec![("id", "bigint", false)]),
        );

        let before_snap = SchemaStateSnapshot {
            tables: before,
            indexes: HashMap::new(),
            foreign_keys: HashMap::new(),
        };
        let after_snap = SchemaStateSnapshot {
            tables: after,
            indexes: HashMap::new(),
            foreign_keys: HashMap::new(),
        };

        let diff = compute_schema_diff(&before_snap, &after_snap).unwrap();
        assert_eq!(diff.columns_modified.len(), 1);
        assert_eq!(diff.columns_modified[0].old_type, "integer");
        assert_eq!(diff.columns_modified[0].new_type, "bigint");
    }
}
