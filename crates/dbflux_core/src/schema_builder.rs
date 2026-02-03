use crate::schema::{ForeignKeyInfo, IndexInfo, SchemaForeignKeyInfo, SchemaIndexInfo};
use std::collections::HashMap;

/// Builder for grouping FK rows by constraint name into ForeignKeyInfo structs.
///
/// SQL queries return one row per FK column. This builder groups them by name.
#[derive(Default)]
pub struct ForeignKeyBuilder {
    map: HashMap<String, ForeignKeyInfo>,
}

impl ForeignKeyBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a column to a foreign key. Creates the FK if it doesn't exist.
    #[allow(clippy::too_many_arguments)]
    pub fn add_column(
        &mut self,
        name: String,
        column: String,
        referenced_schema: Option<String>,
        referenced_table: String,
        referenced_column: String,
        on_update: Option<String>,
        on_delete: Option<String>,
    ) {
        let entry = self
            .map
            .entry(name.clone())
            .or_insert_with(|| ForeignKeyInfo {
                name,
                columns: Vec::new(),
                referenced_schema,
                referenced_table,
                referenced_columns: Vec::new(),
                on_update,
                on_delete,
            });

        if !entry.columns.contains(&column) {
            entry.columns.push(column);
        }
        if !entry.referenced_columns.contains(&referenced_column) {
            entry.referenced_columns.push(referenced_column);
        }
    }

    /// Finalize and return the collected foreign keys.
    pub fn build(self) -> Vec<ForeignKeyInfo> {
        self.map.into_values().collect()
    }

    /// Finalize and return sorted by name.
    pub fn build_sorted(self) -> Vec<ForeignKeyInfo> {
        let mut fks: Vec<_> = self.map.into_values().collect();
        fks.sort_by(|a, b| a.name.cmp(&b.name));
        fks
    }
}

/// Builder for grouping FK rows into SchemaForeignKeyInfo (includes table_name).
#[derive(Default)]
pub struct SchemaForeignKeyBuilder {
    map: HashMap<(String, String), SchemaForeignKeyInfo>,
}

impl SchemaForeignKeyBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a column to a schema-level foreign key. Creates the FK if it doesn't exist.
    #[allow(clippy::too_many_arguments)]
    pub fn add_column(
        &mut self,
        table_name: String,
        name: String,
        column: String,
        referenced_schema: Option<String>,
        referenced_table: String,
        referenced_column: String,
        on_update: Option<String>,
        on_delete: Option<String>,
    ) {
        let key = (table_name.clone(), name.clone());
        let entry = self.map.entry(key).or_insert_with(|| SchemaForeignKeyInfo {
            name,
            table_name,
            columns: Vec::new(),
            referenced_schema,
            referenced_table,
            referenced_columns: Vec::new(),
            on_update,
            on_delete,
        });

        if !entry.columns.contains(&column) {
            entry.columns.push(column);
        }
        if !entry.referenced_columns.contains(&referenced_column) {
            entry.referenced_columns.push(referenced_column);
        }
    }

    /// Finalize and return the collected foreign keys.
    pub fn build(self) -> Vec<SchemaForeignKeyInfo> {
        self.map.into_values().collect()
    }

    /// Finalize and return sorted by (table_name, name).
    pub fn build_sorted(self) -> Vec<SchemaForeignKeyInfo> {
        let mut fks: Vec<_> = self.map.into_values().collect();
        fks.sort_by(|a, b| (&a.table_name, &a.name).cmp(&(&b.table_name, &b.name)));
        fks
    }
}

/// Builder for grouping index rows by name into IndexInfo structs.
#[derive(Default)]
pub struct IndexBuilder {
    map: HashMap<String, IndexInfo>,
}

impl IndexBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a column to an index. Creates the index if it doesn't exist.
    pub fn add_column(&mut self, name: String, column: String, is_unique: bool) {
        let entry = self.map.entry(name.clone()).or_insert_with(|| IndexInfo {
            name,
            columns: Vec::new(),
            is_unique,
            is_primary: false,
        });

        entry.columns.push(column);
    }

    /// Mark an index as primary key.
    pub fn set_primary(&mut self, name: &str) {
        if let Some(idx) = self.map.get_mut(name) {
            idx.is_primary = true;
        }
    }

    /// Finalize and return the collected indexes.
    pub fn build(self) -> Vec<IndexInfo> {
        self.map.into_values().collect()
    }

    /// Finalize and return sorted by name.
    pub fn build_sorted(self) -> Vec<IndexInfo> {
        let mut indexes: Vec<_> = self.map.into_values().collect();
        indexes.sort_by(|a, b| a.name.cmp(&b.name));
        indexes
    }
}

/// Builder for grouping index rows into SchemaIndexInfo (includes table_name).
#[derive(Default)]
pub struct SchemaIndexBuilder {
    map: HashMap<(String, String), SchemaIndexInfo>,
}

impl SchemaIndexBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a column to a schema-level index. Creates the index if it doesn't exist.
    pub fn add_column(
        &mut self,
        table_name: String,
        name: String,
        column: String,
        is_unique: bool,
    ) {
        let key = (table_name.clone(), name.clone());
        let entry = self.map.entry(key).or_insert_with(|| SchemaIndexInfo {
            name,
            table_name,
            columns: Vec::new(),
            is_unique,
            is_primary: false,
        });

        entry.columns.push(column);
    }

    /// Mark an index as primary key.
    pub fn set_primary(&mut self, table_name: &str, name: &str) {
        let key = (table_name.to_string(), name.to_string());
        if let Some(idx) = self.map.get_mut(&key) {
            idx.is_primary = true;
        }
    }

    /// Finalize and return the collected indexes.
    pub fn build(self) -> Vec<SchemaIndexInfo> {
        self.map.into_values().collect()
    }

    /// Finalize and return sorted by (table_name, name).
    pub fn build_sorted(self) -> Vec<SchemaIndexInfo> {
        let mut indexes: Vec<_> = self.map.into_values().collect();
        indexes.sort_by(|a, b| (&a.table_name, &a.name).cmp(&(&b.table_name, &b.name)));
        indexes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fk_builder_groups_columns() {
        let mut builder = ForeignKeyBuilder::new();

        builder.add_column(
            "fk_order_customer".into(),
            "customer_id".into(),
            Some("public".into()),
            "customers".into(),
            "id".into(),
            Some("NO ACTION".into()),
            Some("CASCADE".into()),
        );

        builder.add_column(
            "fk_order_customer".into(),
            "tenant_id".into(),
            Some("public".into()),
            "customers".into(),
            "tenant_id".into(),
            Some("NO ACTION".into()),
            Some("CASCADE".into()),
        );

        let fks = builder.build();
        assert_eq!(fks.len(), 1);
        assert_eq!(fks[0].columns, vec!["customer_id", "tenant_id"]);
        assert_eq!(fks[0].referenced_columns, vec!["id", "tenant_id"]);
    }

    #[test]
    fn test_index_builder_groups_columns() {
        let mut builder = IndexBuilder::new();

        builder.add_column("idx_name_email".into(), "name".into(), false);
        builder.add_column("idx_name_email".into(), "email".into(), false);
        builder.add_column("users_pkey".into(), "id".into(), true);
        builder.set_primary("users_pkey");

        let indexes = builder.build_sorted();
        assert_eq!(indexes.len(), 2);

        let name_email = indexes.iter().find(|i| i.name == "idx_name_email").unwrap();
        assert_eq!(name_email.columns, vec!["name", "email"]);
        assert!(!name_email.is_primary);

        let pkey = indexes.iter().find(|i| i.name == "users_pkey").unwrap();
        assert!(pkey.is_primary);
        assert!(pkey.is_unique);
    }
}
