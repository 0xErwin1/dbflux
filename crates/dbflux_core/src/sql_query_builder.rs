use crate::Value;
use crate::crud::{RecordIdentity, RowDelete, RowInsert, RowPatch};
use crate::sql_dialect::SqlDialect;

/// Builds CRUD SQL statements using a specific dialect.
pub struct SqlQueryBuilder<'a> {
    dialect: &'a dyn SqlDialect,
}

impl<'a> SqlQueryBuilder<'a> {
    pub fn new(dialect: &'a dyn SqlDialect) -> Self {
        Self { dialect }
    }

    /// Build UPDATE statement from RowPatch.
    ///
    /// Returns SQL like: `UPDATE "table" SET "col1" = val1, "col2" = val2 WHERE "pk" = pkval`
    /// If `with_returning` is true and dialect supports it, appends `RETURNING *`.
    pub fn build_update(&self, patch: &RowPatch, with_returning: bool) -> Option<String> {
        if patch.changes.is_empty() {
            return None;
        }

        let table = self
            .dialect
            .qualified_table(patch.schema.as_deref(), &patch.table);

        let set_clause = self.build_set_clause(&patch.changes);
        let where_clause = self.build_where_clause(&patch.identity)?;

        let mut sql = format!("UPDATE {} SET {} WHERE {}", table, set_clause, where_clause);

        if with_returning && self.dialect.supports_returning() {
            sql.push_str(" RETURNING *");
        }

        Some(sql)
    }

    /// Build INSERT statement from RowInsert.
    ///
    /// Returns SQL like: `INSERT INTO "table" ("col1", "col2") VALUES (val1, val2)`
    /// If `with_returning` is true and dialect supports it, appends `RETURNING *`.
    pub fn build_insert(&self, insert: &RowInsert, with_returning: bool) -> Option<String> {
        if insert.columns.is_empty() {
            return None;
        }

        let table = self
            .dialect
            .qualified_table(insert.schema.as_deref(), &insert.table);

        let columns: Vec<String> = insert
            .columns
            .iter()
            .map(|c| self.dialect.quote_identifier(c))
            .collect();
        let columns_str = columns.join(", ");

        let values: Vec<String> = insert
            .values
            .iter()
            .map(|v| self.dialect.value_to_literal(v))
            .collect();
        let values_str = values.join(", ");

        let mut sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table, columns_str, values_str
        );

        if with_returning && self.dialect.supports_returning() {
            sql.push_str(" RETURNING *");
        }

        Some(sql)
    }

    /// Build DELETE statement from RowDelete.
    ///
    /// Returns SQL like: `DELETE FROM "table" WHERE "pk" = pkval`
    /// If `with_returning` is true and dialect supports it, appends `RETURNING *`.
    pub fn build_delete(&self, delete: &RowDelete, with_returning: bool) -> Option<String> {
        let table = self
            .dialect
            .qualified_table(delete.schema.as_deref(), &delete.table);

        let where_clause = self.build_where_clause(&delete.identity)?;

        let mut sql = format!("DELETE FROM {} WHERE {}", table, where_clause);

        if with_returning && self.dialect.supports_returning() {
            sql.push_str(" RETURNING *");
        }

        Some(sql)
    }

    /// Build SELECT statement to fetch a row by identity.
    ///
    /// Used by drivers that don't support RETURNING (SQLite, MySQL) to re-query
    /// the affected row after UPDATE/INSERT/DELETE.
    pub fn build_select_by_identity(
        &self,
        schema: Option<&str>,
        table: &str,
        identity: &RecordIdentity,
    ) -> Option<String> {
        let table_ref = self.dialect.qualified_table(schema, table);
        let where_clause = self.build_where_clause(identity)?;

        Some(format!(
            "SELECT * FROM {} WHERE {} LIMIT 1",
            table_ref, where_clause
        ))
    }

    /// Build WHERE clause from RecordIdentity.
    ///
    /// Returns `"col1" = val1 AND "col2" = val2` for composite keys.
    /// Returns None if identity is invalid or not a composite type.
    pub fn build_where_clause(&self, identity: &RecordIdentity) -> Option<String> {
        match identity {
            RecordIdentity::Composite { columns, values } => {
                if columns.is_empty() || columns.len() != values.len() {
                    return None;
                }

                let conditions: Vec<String> = columns
                    .iter()
                    .zip(values.iter())
                    .map(|(col, val)| {
                        let col_quoted = self.dialect.quote_identifier(col);
                        if val.is_null() {
                            format!("{} IS NULL", col_quoted)
                        } else {
                            format!("{} = {}", col_quoted, self.dialect.value_to_literal(val))
                        }
                    })
                    .collect();

                Some(conditions.join(" AND "))
            }
            RecordIdentity::ObjectId(_) | RecordIdentity::Key(_) => None,
        }
    }

    /// Build SET clause for UPDATE.
    ///
    /// Returns `"col1" = val1, "col2" = val2`.
    pub fn build_set_clause(&self, changes: &[(String, Value)]) -> String {
        let assignments: Vec<String> = changes
            .iter()
            .map(|(col, val)| {
                format!(
                    "{} = {}",
                    self.dialect.quote_identifier(col),
                    self.dialect.value_to_literal(val)
                )
            })
            .collect();

        assignments.join(", ")
    }

    /// Build column list for INSERT.
    ///
    /// Returns `"col1", "col2", "col3"`.
    pub fn build_column_list(&self, columns: &[String]) -> String {
        columns
            .iter()
            .map(|c| self.dialect.quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Build values list for INSERT.
    ///
    /// Returns `val1, val2, val3`.
    pub fn build_values_list(&self, values: &[Value]) -> String {
        values
            .iter()
            .map(|v| self.dialect.value_to_literal(v))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_dialect::DefaultSqlDialect;

    #[test]
    fn test_build_where_clause() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let identity = RecordIdentity::composite(vec!["id".to_string()], vec![Value::Int(42)]);

        let where_clause = builder.build_where_clause(&identity).unwrap();
        assert_eq!(where_clause, "\"id\" = 42");
    }

    #[test]
    fn test_build_where_clause_null() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let identity = RecordIdentity::composite(vec!["status".to_string()], vec![Value::Null]);

        let where_clause = builder.build_where_clause(&identity).unwrap();
        assert_eq!(where_clause, "\"status\" IS NULL");
    }

    #[test]
    fn test_build_where_clause_composite() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let identity = RecordIdentity::composite(
            vec!["tenant_id".to_string(), "user_id".to_string()],
            vec![Value::Int(1), Value::Int(100)],
        );

        let where_clause = builder.build_where_clause(&identity).unwrap();
        assert_eq!(where_clause, "\"tenant_id\" = 1 AND \"user_id\" = 100");
    }

    #[test]
    fn test_build_set_clause() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let changes = vec![
            ("name".to_string(), Value::Text("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
        ];

        let set_clause = builder.build_set_clause(&changes);
        assert_eq!(set_clause, "\"name\" = 'Alice', \"age\" = 30");
    }

    #[test]
    fn test_build_update() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let patch = RowPatch::new(
            RecordIdentity::composite(vec!["id".to_string()], vec![Value::Int(1)]),
            "users".to_string(),
            Some("public".to_string()),
            vec![("name".to_string(), Value::Text("Bob".to_string()))],
        );

        let sql = builder.build_update(&patch, false).unwrap();
        assert_eq!(
            sql,
            "UPDATE \"public\".\"users\" SET \"name\" = 'Bob' WHERE \"id\" = 1"
        );
    }

    #[test]
    fn test_build_insert() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let insert = RowInsert::new(
            "users".to_string(),
            None,
            vec!["name".to_string(), "age".to_string()],
            vec![Value::Text("Alice".to_string()), Value::Int(25)],
        );

        let sql = builder.build_insert(&insert, false).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"users\" (\"name\", \"age\") VALUES ('Alice', 25)"
        );
    }

    #[test]
    fn test_build_delete() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let delete = RowDelete::new(
            RecordIdentity::composite(vec!["id".to_string()], vec![Value::Int(42)]),
            "users".to_string(),
            None,
        );

        let sql = builder.build_delete(&delete, false).unwrap();
        assert_eq!(sql, "DELETE FROM \"users\" WHERE \"id\" = 42");
    }

    #[test]
    fn test_build_select_by_identity() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let identity = RecordIdentity::composite(vec!["id".to_string()], vec![Value::Int(42)]);

        let sql = builder
            .build_select_by_identity(Some("public"), "users", &identity)
            .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM \"public\".\"users\" WHERE \"id\" = 42 LIMIT 1"
        );
    }
}
