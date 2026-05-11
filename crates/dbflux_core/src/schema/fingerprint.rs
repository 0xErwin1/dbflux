use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::TableInfo;

/// A hash over the schema-relevant properties of a table.
///
/// Covers column names, types, nullability, PK flags, and FK target tuples in
/// declaration order. Column ordering is intentional: reordering columns counts
/// as a change because it affects positional query results.
///
/// NOTE: built on `DefaultHasher`, which is NOT stable across Rust versions or
/// processes. This cache is per-session only. Do NOT persist fingerprints to
/// disk or compare them across builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaFingerprint(u64);

impl SchemaFingerprint {
    /// Compute a fingerprint from a loaded `TableInfo`.
    ///
    /// Only fields with `Some(...)` columns or foreign keys are hashed; a table
    /// whose details have not yet been loaded will hash as if it has no columns.
    pub fn from_table_info(info: &TableInfo) -> Self {
        let mut hasher = DefaultHasher::new();

        // Hash column properties in declared order.
        if let Some(columns) = &info.columns {
            for col in columns {
                col.name.hash(&mut hasher);
                col.type_name.hash(&mut hasher);
                col.nullable.hash(&mut hasher);
                col.is_primary_key.hash(&mut hasher);
            }
        }

        // Hash foreign-key tuples in declared order.
        if let Some(fks) = &info.foreign_keys {
            for fk in fks {
                fk.columns.hash(&mut hasher);
                fk.referenced_schema.hash(&mut hasher);
                fk.referenced_table.hash(&mut hasher);
                fk.referenced_columns.hash(&mut hasher);
            }
        }

        SchemaFingerprint(hasher.finish())
    }

    /// Returns the raw 64-bit hash value.
    pub fn raw(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnInfo, ForeignKeyInfo, TableInfo};

    fn base_table() -> TableInfo {
        TableInfo {
            name: "users".to_string(),
            schema: Some("public".to_string()),
            columns: Some(vec![
                ColumnInfo {
                    name: "id".to_string(),
                    type_name: "integer".to_string(),
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    enum_values: None,
                },
                ColumnInfo {
                    name: "email".to_string(),
                    type_name: "text".to_string(),
                    nullable: false,
                    is_primary_key: false,
                    default_value: None,
                    enum_values: None,
                },
            ]),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: Default::default(),
            child_items: None,
        }
    }

    #[test]
    fn same_table_produces_same_fingerprint() {
        let table = base_table();
        assert_eq!(
            SchemaFingerprint::from_table_info(&table),
            SchemaFingerprint::from_table_info(&table)
        );
    }

    #[test]
    fn different_column_type_produces_different_fingerprint() {
        let mut table = base_table();
        if let Some(cols) = &mut table.columns {
            cols[1].type_name = "varchar(255)".to_string();
        }
        let original = base_table();
        assert_ne!(
            SchemaFingerprint::from_table_info(&original),
            SchemaFingerprint::from_table_info(&table)
        );
    }

    #[test]
    fn columns_reordered_produces_different_fingerprint() {
        let mut reordered = base_table();
        if let Some(cols) = &mut reordered.columns {
            cols.reverse();
        }
        let original = base_table();
        assert_ne!(
            SchemaFingerprint::from_table_info(&original),
            SchemaFingerprint::from_table_info(&reordered)
        );
    }

    #[test]
    fn pk_change_produces_different_fingerprint() {
        let mut table = base_table();
        if let Some(cols) = &mut table.columns {
            cols[0].is_primary_key = false;
        }
        let original = base_table();
        assert_ne!(
            SchemaFingerprint::from_table_info(&original),
            SchemaFingerprint::from_table_info(&table)
        );
    }

    #[test]
    fn fk_change_produces_different_fingerprint() {
        let mut with_fk = base_table();
        with_fk.foreign_keys = Some(vec![ForeignKeyInfo {
            name: "fk_users_org".to_string(),
            columns: vec!["org_id".to_string()],
            referenced_table: "organizations".to_string(),
            referenced_schema: Some("public".to_string()),
            referenced_columns: vec!["id".to_string()],
            on_delete: None,
            on_update: None,
        }]);

        let original = base_table();
        assert_ne!(
            SchemaFingerprint::from_table_info(&original),
            SchemaFingerprint::from_table_info(&with_fk)
        );
    }
}
