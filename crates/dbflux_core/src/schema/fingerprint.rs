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

    /// Computes a stable SHA-256 hex digest over the schema-relevant
    /// properties of a table.
    ///
    /// Unlike [`Self::from_table_info`], this digest is stable across Rust
    /// versions and process restarts, making it safe to persist to disk and
    /// compare across builds (used for on-connect snapshot deduplication).
    /// Covers the same field tuples as `from_table_info` plus `default_value`,
    /// which the persisted snapshot format tracks but the session-only drift
    /// fingerprint does not.
    pub fn stable_hex(info: &TableInfo) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();

        hasher.update(info.schema.as_deref().unwrap_or(""));
        hasher.update([0u8]);
        hasher.update(&info.name);
        hasher.update([0u8]);

        if let Some(columns) = &info.columns {
            for col in columns {
                hasher.update(&col.name);
                hasher.update([0u8]);
                hasher.update(&col.type_name);
                hasher.update([0u8]);
                hasher.update([col.nullable as u8, col.is_primary_key as u8]);
                hasher.update(col.default_value.as_deref().unwrap_or(""));
                hasher.update([0u8]);
            }
        }

        hasher.update([0xFFu8]);

        if let Some(fks) = &info.foreign_keys {
            for fk in fks {
                hasher.update(fk.columns.join(","));
                hasher.update([0u8]);
                hasher.update(fk.referenced_schema.as_deref().unwrap_or(""));
                hasher.update([0u8]);
                hasher.update(&fk.referenced_table);
                hasher.update([0u8]);
                hasher.update(fk.referenced_columns.join(","));
                hasher.update([0u8]);
            }
        }

        hex::encode(hasher.finalize())
    }

    /// Computes a single stable SHA-256 hex digest over a whole set of
    /// tables, order-independent (sorted by each table's own `stable_hex`
    /// before combining).
    ///
    /// Used to fingerprint an entire captured schema snapshot for on-connect
    /// dedup: two captures of the same structure produce the same digest
    /// regardless of the order the driver returned the tables in.
    pub fn stable_hex_many(tables: &[TableInfo]) -> String {
        use sha2::{Digest, Sha256};

        let mut per_table: Vec<String> = tables.iter().map(Self::stable_hex).collect();
        per_table.sort();

        let mut hasher = Sha256::new();
        for hex in &per_table {
            hasher.update(hex.as_bytes());
            hasher.update([0u8]);
        }

        hex::encode(hasher.finalize())
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

    // --- stable_hex ---

    #[test]
    fn stable_hex_is_stable_across_repeated_computation() {
        let table = base_table();
        let first = SchemaFingerprint::stable_hex(&table);
        let second = SchemaFingerprint::stable_hex(&table);
        assert_eq!(first, second, "repeated computation must be deterministic");
        assert_eq!(first.len(), 64, "sha256 hex digest must be 64 chars");
    }

    #[test]
    fn stable_hex_differs_on_column_type_change() {
        let mut table = base_table();
        if let Some(cols) = &mut table.columns {
            cols[1].type_name = "varchar(255)".to_string();
        }
        let original = base_table();
        assert_ne!(
            SchemaFingerprint::stable_hex(&original),
            SchemaFingerprint::stable_hex(&table)
        );
    }

    #[test]
    fn stable_hex_differs_on_default_value_change() {
        let mut table = base_table();
        if let Some(cols) = &mut table.columns {
            cols[0].default_value = Some("0".to_string());
        }
        let original = base_table();
        assert_ne!(
            SchemaFingerprint::stable_hex(&original),
            SchemaFingerprint::stable_hex(&table),
            "default_value changes are tracked by stable_hex, unlike from_table_info"
        );
    }

    // --- stable_hex_many ---

    #[test]
    fn stable_hex_many_is_order_independent() {
        let mut users = base_table();
        users.name = "users".to_string();
        let mut orders = base_table();
        orders.name = "orders".to_string();

        let forward = SchemaFingerprint::stable_hex_many(&[users.clone(), orders.clone()]);
        let reversed = SchemaFingerprint::stable_hex_many(&[orders, users]);

        assert_eq!(
            forward, reversed,
            "combined fingerprint must not depend on driver-returned table order"
        );
    }

    #[test]
    fn stable_hex_many_differs_when_a_table_changes() {
        let mut users = base_table();
        users.name = "users".to_string();
        let mut orders = base_table();
        orders.name = "orders".to_string();

        let before = SchemaFingerprint::stable_hex_many(&[users.clone(), orders.clone()]);

        let mut orders_changed = orders.clone();
        if let Some(cols) = &mut orders_changed.columns {
            cols[0].type_name = "bigint".to_string();
        }
        let after = SchemaFingerprint::stable_hex_many(&[users, orders_changed]);

        assert_ne!(
            before, after,
            "changing one table's structure must change the combined fingerprint"
        );
    }

    #[test]
    fn stable_hex_many_empty_slice_is_stable() {
        let first = SchemaFingerprint::stable_hex_many(&[]);
        let second = SchemaFingerprint::stable_hex_many(&[]);
        assert_eq!(first, second);
    }
}
