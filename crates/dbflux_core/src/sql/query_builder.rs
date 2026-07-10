use crate::Value;
use crate::data::crud::{
    ColumnAssignment, RecordIdentity, RowDelete, RowInsert, RowPatch, SqlDeleteRequest,
    SqlUpdateRequest, SqlUpsertRequest,
};
use crate::query::generator::{CreateTableSpec, GeneratorError};
use crate::render_semantic_filter_sql;
use crate::sql::dialect::SqlDialect;

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

    /// Build UPDATE statement from a semantic filtered update request.
    pub fn build_update_many(&self, update: &SqlUpdateRequest) -> Option<String> {
        if update.changes.is_empty() {
            return None;
        }

        let table = self
            .dialect
            .qualified_table(update.schema.as_deref(), &update.table);

        let set_clause = self.build_set_clause(&update.changes);
        let where_clause = render_semantic_filter_sql(&update.filter, self.dialect).ok()?;

        let mut sql = format!("UPDATE {} SET {} WHERE {}", table, set_clause, where_clause);

        if self.dialect.supports_returning()
            && let Some(returning) = update.returning.as_ref()
            && !returning.is_empty()
        {
            let columns = returning
                .iter()
                .map(|column| self.dialect.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(" RETURNING ");
            sql.push_str(&columns);
        }

        Some(sql)
    }

    /// Build INSERT statement from RowInsert.
    ///
    /// Returns SQL like: `INSERT INTO "table" ("col1", "col2") VALUES (val1, val2)`
    /// If `with_returning` is true and dialect supports it, appends `RETURNING *`.
    pub fn build_insert(&self, insert: &RowInsert, with_returning: bool) -> Option<String> {
        if insert.assignments.is_empty() {
            return None;
        }

        let table = self
            .dialect
            .qualified_table(insert.schema.as_deref(), &insert.table);

        let columns_str = insert
            .assignments
            .iter()
            .map(|a| self.dialect.quote_identifier(&a.name))
            .collect::<Vec<_>>()
            .join(", ");

        let values_str = insert
            .assignments
            .iter()
            .map(|a| {
                self.dialect
                    .value_to_literal_typed(&a.value, a.type_name.as_deref())
            })
            .collect::<Vec<_>>()
            .join(", ");

        let mut sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table, columns_str, values_str
        );

        if with_returning && self.dialect.supports_returning() {
            sql.push_str(" RETURNING *");
        }

        Some(sql)
    }

    /// Build a native multi-row INSERT statement.
    ///
    /// Returns SQL like: `INSERT INTO "table" ("col1", "col2") VALUES (r1c1, r1c2), (r2c1, r2c2)`.
    /// Returns `None` when `rows` is empty. Row-count caps (e.g. MSSQL's 1000-row
    /// limit per statement) are the caller's responsibility via `DriverLimits`;
    /// this builder emits exactly one `VALUES` tuple per row given.
    ///
    /// `column_types` runs parallel to `columns` — when present for a given
    /// index, the dialect is asked to format that column's values with
    /// [`SqlDialect::value_to_literal_typed`] instead of the untyped
    /// fallback (e.g. PostgreSQL needs the column's type to emit
    /// `ARRAY[...]::text[]` rather than a generic `::jsonb` cast).
    pub fn build_bulk_insert(
        &self,
        schema: Option<&str>,
        table: &str,
        columns: &[String],
        column_types: &[Option<String>],
        rows: &[&[Value]],
    ) -> Option<String> {
        if rows.is_empty() {
            return None;
        }

        let table_ref = self.dialect.qualified_table(schema, table);
        let columns_str = self.build_column_list(columns);

        let values_str = rows
            .iter()
            .map(|row| {
                let tuple = row
                    .iter()
                    .enumerate()
                    .map(|(index, v)| {
                        let col_type = column_types.get(index).and_then(|t| t.as_deref());
                        self.dialect.value_to_literal_typed(v, col_type)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("({tuple})")
            })
            .collect::<Vec<_>>()
            .join(", ");

        Some(format!(
            "INSERT INTO {table_ref} ({columns_str}) VALUES {values_str}"
        ))
    }

    /// Build a `CREATE TABLE` statement from a same-engine [`CreateTableSpec`].
    ///
    /// Column type, nullability, and primary-key membership map 1:1 from
    /// `spec.columns` since the source and target share the same SQL dialect
    /// family; no cross-dialect type coercion is attempted.
    ///
    /// Each column's `type_name` is validated against a conservative type-spec
    /// grammar (SEC-W1) before interpolation: on Import, `type_name` comes
    /// from an external `manifest.json` (bundles are shareable), so a crafted
    /// value like `TEXT); DROP TABLE users; --` must be rejected rather than
    /// emitted as arbitrary DDL. Column names are already escaped via
    /// `quote_identifier`; this closes the same gap for the type string.
    pub fn build_create_table(&self, spec: &CreateTableSpec) -> Result<String, GeneratorError> {
        let table_ref = self
            .dialect
            .qualified_table(spec.schema.as_deref(), &spec.table);

        let pk_columns: Vec<&str> = spec
            .columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.as_str())
            .collect();

        let mut lines: Vec<String> = Vec::with_capacity(spec.columns.len() + 1);

        for column in &spec.columns {
            let mut line = match &column.type_name {
                Some(type_name) => {
                    if !is_safe_column_type_spec(type_name) {
                        return Err(GeneratorError::InvalidColumnType {
                            column: column.name.clone(),
                            type_name: type_name.clone(),
                        });
                    }

                    format!(
                        "    {} {}",
                        self.dialect.quote_identifier(&column.name),
                        type_name
                    )
                }
                None => format!("    {}", self.dialect.quote_identifier(&column.name)),
            };

            if !column.nullable {
                line.push_str(" NOT NULL");
            }

            lines.push(line);
        }

        if !pk_columns.is_empty() {
            let pk_quoted: Vec<String> = pk_columns
                .iter()
                .map(|name| self.dialect.quote_identifier(name))
                .collect();
            lines.push(format!("    PRIMARY KEY ({})", pk_quoted.join(", ")));
        }

        let prefix = if spec.if_not_exists {
            "CREATE TABLE IF NOT EXISTS"
        } else {
            "CREATE TABLE"
        };

        Ok(format!("{prefix} {table_ref} (\n{}\n);", lines.join(",\n")))
    }

    /// Build an UPSERT statement from a semantic request.
    pub fn build_upsert(&self, upsert: &SqlUpsertRequest) -> Option<String> {
        if !upsert.is_valid() {
            return None;
        }

        self.dialect.build_upsert_statement(
            upsert.schema.as_deref(),
            &upsert.table,
            &upsert.assignments,
            &upsert.conflict_columns,
            &upsert.update_assignments,
        )
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

    /// Build DELETE statement from a semantic filtered delete request.
    pub fn build_delete_many(&self, delete: &SqlDeleteRequest) -> Option<String> {
        let table = self
            .dialect
            .qualified_table(delete.schema.as_deref(), &delete.table);

        let where_clause = render_semantic_filter_sql(&delete.filter, self.dialect).ok()?;

        let mut sql = format!("DELETE FROM {} WHERE {}", table, where_clause);

        if self.dialect.supports_returning()
            && let Some(returning) = delete.returning.as_ref()
            && !returning.is_empty()
        {
            let columns = returning
                .iter()
                .map(|column| self.dialect.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(" RETURNING ");
            sql.push_str(&columns);
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

    /// Build SET clause for UPDATE from typed assignments.
    ///
    /// Returns `"col1" = val1, "col2" = val2`. When an assignment carries a
    /// `type_name`, the dialect is asked to format the literal with that hint
    /// (e.g. PostgreSQL emits `ARRAY[...]::text[]` for array columns).
    pub fn build_set_clause(&self, changes: &[ColumnAssignment]) -> String {
        changes
            .iter()
            .map(|a| {
                format!(
                    "{} = {}",
                    self.dialect.quote_identifier(&a.name),
                    self.dialect
                        .value_to_literal_typed(&a.value, a.type_name.as_deref())
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
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

/// Structural grammar for a `CREATE TABLE` column type spec (SEC-W1).
///
/// A same-engine `type_name` is never a full SQL expression, so this parses
/// it as: an identifier (optionally schema-qualified with `.`, optionally a
/// double-quoted identifier, optionally multi-word like `double precision`),
/// followed by an optional single balanced `(...)` argument list (digits,
/// commas, spaces, and single-quoted string literals — for `enum`/`set`
/// values), followed by an optional trailing `[]`. A plain character
/// whitelist is not enough here: it accepts unbalanced quotes/parens and
/// still rejects legitimate specs like MySQL `enum('a','b')` or PostgreSQL
/// `myschema.mytype`, so structure is validated instead of just the charset.
fn is_safe_column_type_spec(type_name: &str) -> bool {
    if type_name.is_empty()
        || type_name.chars().any(|c| c.is_control())
        || type_name.contains(';')
        || type_name.contains("--")
        || type_name.contains("/*")
        || type_name.contains("*/")
        || type_name.contains('\\')
    {
        return false;
    }

    let trimmed = type_name.trim();
    if trimmed.is_empty() {
        return false;
    }

    let chars: Vec<char> = trimmed.chars().collect();

    let Some(mut pos) = parse_type_name(&chars, 0) else {
        return false;
    };

    pos = skip_ascii_spaces(&chars, pos);

    if chars.get(pos) == Some(&'(') {
        let Some(next) = parse_paren_args(&chars, pos) else {
            return false;
        };
        pos = skip_ascii_spaces(&chars, next);
    }

    if chars.get(pos) == Some(&'[') {
        if chars.get(pos + 1) != Some(&']') {
            return false;
        }
        pos = skip_ascii_spaces(&chars, pos + 2);
    }

    pos == chars.len()
}

fn skip_ascii_spaces(chars: &[char], mut pos: usize) -> usize {
    while chars.get(pos) == Some(&' ') {
        pos += 1;
    }
    pos
}

/// Parses one or more identifier segments joined by `.` (schema
/// qualification) or by whitespace (multi-word names such as
/// `timestamp with time zone`), stopping right before `(`, `[`, or the end.
fn parse_type_name(chars: &[char], pos: usize) -> Option<usize> {
    let mut pos = parse_ident_segment(chars, pos)?;

    loop {
        if chars.get(pos) == Some(&'.') {
            match parse_ident_segment(chars, pos + 1) {
                Some(next) => {
                    pos = next;
                    continue;
                }
                None => return None,
            }
        }

        let spaced = skip_ascii_spaces(chars, pos);
        if spaced > pos {
            match parse_ident_segment(chars, spaced) {
                Some(next) => {
                    pos = next;
                    continue;
                }
                None => break,
            }
        }

        break;
    }

    Some(pos)
}

/// Parses either a bare identifier (`[A-Za-z_][A-Za-z0-9_]*`) or a
/// double-quoted identifier (balanced `"..."`, no embedded quote).
fn parse_ident_segment(chars: &[char], pos: usize) -> Option<usize> {
    if chars.get(pos) == Some(&'"') {
        let mut end = pos + 1;
        loop {
            match chars.get(end) {
                Some('"') if end > pos + 1 => return Some(end + 1),
                Some(c) if c.is_ascii_alphanumeric() || *c == '_' || *c == ' ' => end += 1,
                _ => return None,
            }
        }
    }

    let start = pos;
    if !matches!(chars.get(start), Some(c) if c.is_ascii_alphabetic() || *c == '_') {
        return None;
    }

    let mut end = start + 1;
    while matches!(chars.get(end), Some(c) if c.is_ascii_alphanumeric() || *c == '_') {
        end += 1;
    }

    Some(end)
}

/// Parses a balanced `(...)` argument list containing only digits, commas,
/// spaces, and single-quoted string literals (`enum`/`set` values).
fn parse_paren_args(chars: &[char], pos: usize) -> Option<usize> {
    let mut end = pos + 1;

    loop {
        match chars.get(end) {
            Some('\'') => {
                end += 1;
                loop {
                    match chars.get(end) {
                        Some('\'') => {
                            end += 1;
                            break;
                        }
                        Some(_) => end += 1,
                        None => return None,
                    }
                }
            }
            Some(')') => return Some(end + 1),
            Some(c) if c.is_ascii_digit() || *c == ',' || *c == ' ' => end += 1,
            _ => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::dialect::{DefaultSqlDialect, PlaceholderStyle};

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
            ColumnAssignment::new("name", Value::Text("Alice".to_string())),
            ColumnAssignment::new("age", Value::Int(30)),
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
    fn test_build_update_many() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let update = SqlUpdateRequest::new(
            "users".to_string(),
            Some("public".to_string()),
            crate::SemanticFilter::compare(
                "status",
                crate::WhereOperator::Eq,
                Value::Text("active".to_string()),
            ),
            vec![("archived".to_string(), Value::Bool(true))],
        );

        let sql = builder.build_update_many(&update).unwrap();
        assert_eq!(
            sql,
            "UPDATE \"public\".\"users\" SET \"archived\" = TRUE WHERE \"status\" = 'active'"
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
    fn test_build_bulk_insert_multi_row() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let columns = vec!["name".to_string(), "age".to_string()];
        let owned_rows: Vec<Vec<Value>> = vec![
            vec![Value::Text("Alice".to_string()), Value::Int(25)],
            vec![Value::Text("Bob".to_string()), Value::Int(30)],
        ];
        let rows: Vec<&[Value]> = owned_rows.iter().map(|r| r.as_slice()).collect();

        let sql = builder
            .build_bulk_insert(None, "users", &columns, &[], &rows)
            .unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"users\" (\"name\", \"age\") VALUES ('Alice', 25), ('Bob', 30)"
        );
    }

    #[test]
    fn test_build_bulk_insert_qualified_table() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let columns = vec!["id".to_string()];
        let owned_rows: Vec<Vec<Value>> = vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
        ];
        let rows: Vec<&[Value]> = owned_rows.iter().map(|r| r.as_slice()).collect();

        let sql = builder
            .build_bulk_insert(Some("public"), "users", &columns, &[], &rows)
            .unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"public\".\"users\" (\"id\") VALUES (1), (2), (3)"
        );
    }

    #[test]
    fn test_build_bulk_insert_empty_rows_returns_none() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let columns = vec!["id".to_string()];
        let rows: Vec<&[Value]> = Vec::new();

        assert!(
            builder
                .build_bulk_insert(None, "users", &columns, &[], &rows)
                .is_none()
        );
    }

    #[test]
    fn test_build_bulk_insert_does_not_truncate_row_count() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let columns = vec!["id".to_string()];
        let owned_rows: Vec<[Value; 1]> = (0..1000i64).map(|i| [Value::Int(i)]).collect();
        let rows: Vec<&[Value]> = owned_rows.iter().map(|r| r.as_slice()).collect();

        let sql = builder
            .build_bulk_insert(None, "t", &columns, &[], &rows)
            .unwrap();

        assert_eq!(sql.matches("), (").count() + 1, 1000);
    }

    /// JD-C2 regression: `column_types` must thread through to
    /// `value_to_literal_typed` per column index, not just be accepted and
    /// ignored — proven with a dialect whose typed formatting diverges from
    /// its untyped formatting.
    #[test]
    fn test_build_bulk_insert_threads_column_types_to_typed_literal() {
        struct TypedDialect;

        impl SqlDialect for TypedDialect {
            fn quote_identifier(&self, name: &str) -> String {
                format!("\"{name}\"")
            }

            fn qualified_table(&self, _schema: Option<&str>, table: &str) -> String {
                self.quote_identifier(table)
            }

            fn value_to_literal(&self, _value: &Value) -> String {
                "UNTYPED".to_string()
            }

            fn value_to_literal_typed(&self, _value: &Value, col_type: Option<&str>) -> String {
                match col_type {
                    Some(ty) => format!("TYPED({ty})"),
                    None => "UNTYPED".to_string(),
                }
            }

            fn escape_string(&self, s: &str) -> String {
                s.to_string()
            }

            fn placeholder_style(&self) -> PlaceholderStyle {
                PlaceholderStyle::QuestionMark
            }
        }

        let dialect = TypedDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let columns = vec!["tags".to_string(), "name".to_string()];
        let column_types = vec![Some("text[]".to_string()), None];
        let owned_rows: Vec<Vec<Value>> = vec![vec![
            Value::Array(vec![Value::Text("a".to_string())]),
            Value::Text("Alice".to_string()),
        ]];
        let rows: Vec<&[Value]> = owned_rows.iter().map(|r| r.as_slice()).collect();

        let sql = builder
            .build_bulk_insert(None, "t", &columns, &column_types, &rows)
            .unwrap();

        assert_eq!(
            sql,
            "INSERT INTO \"t\" (\"tags\", \"name\") VALUES (TYPED(text[]), UNTYPED)"
        );
    }

    #[test]
    fn test_build_create_table_preserves_types_nullability_and_pk() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let spec = CreateTableSpec {
            schema: Some("public".to_string()),
            table: "users".to_string(),
            columns: vec![
                crate::TransferColumn {
                    name: "id".to_string(),
                    type_name: Some("integer".to_string()),
                    nullable: false,
                    is_primary_key: true,
                },
                crate::TransferColumn {
                    name: "name".to_string(),
                    type_name: Some("text".to_string()),
                    nullable: true,
                    is_primary_key: false,
                },
            ],
            if_not_exists: false,
        };

        let sql = builder.build_create_table(&spec).unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE \"public\".\"users\" (\n    \"id\" integer NOT NULL,\n    \"name\" text,\n    PRIMARY KEY (\"id\")\n);"
        );
    }

    #[test]
    fn test_build_create_table_if_not_exists() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let spec = CreateTableSpec {
            schema: None,
            table: "logs".to_string(),
            columns: vec![crate::TransferColumn {
                name: "id".to_string(),
                type_name: None,
                nullable: true,
                is_primary_key: false,
            }],
            if_not_exists: true,
        };

        let sql = builder.build_create_table(&spec).unwrap();
        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS \"logs\" ("));
    }

    /// B-005/SEC-W1 regression: a column `type_name` carrying a statement
    /// terminator/comment marker must be rejected instead of interpolated
    /// into DDL — the manifest.json `type_name` on Import is external input.
    #[test]
    fn test_build_create_table_rejects_a_ddl_injection_type_name() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        let spec = CreateTableSpec {
            schema: None,
            table: "users".to_string(),
            columns: vec![crate::TransferColumn {
                name: "id".to_string(),
                type_name: Some("TEXT); DROP TABLE users; --".to_string()),
                nullable: true,
                is_primary_key: false,
            }],
            if_not_exists: false,
        };

        let result = builder.build_create_table(&spec);
        assert!(
            matches!(result, Err(GeneratorError::InvalidColumnType { .. })),
            "a crafted type_name must be rejected, not interpolated into DDL: {:?}",
            result
        );
    }

    /// B-005/SEC-W1: legitimate type specs a same-engine schema query or a
    /// well-formed manifest could report must still pass.
    ///
    /// A-2-001/B2-001 regression: the original character-whitelist grammar
    /// rejected MySQL `enum`/`set` type strings (single-quoted values) and
    /// PostgreSQL schema-qualified or quoted user-defined types, hard-failing
    /// legitimate same-engine transfers.
    #[test]
    fn test_build_create_table_accepts_legitimate_type_specs() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        for type_name in [
            "varchar(255)",
            "numeric(10,2)",
            "int4[]",
            "double precision",
            "timestamp with time zone",
            "enum('active','inactive')",
            "set('a','b','c')",
            "myschema.mytype",
            "\"MyEnum\"",
            "character varying(50)",
        ] {
            let spec = CreateTableSpec {
                schema: None,
                table: "t".to_string(),
                columns: vec![crate::TransferColumn {
                    name: "c".to_string(),
                    type_name: Some(type_name.to_string()),
                    nullable: true,
                    is_primary_key: false,
                }],
                if_not_exists: false,
            };

            assert!(
                builder.build_create_table(&spec).is_ok(),
                "legitimate type spec '{type_name}' must be accepted"
            );
        }
    }

    /// A-2-001/B2-001 regression: the grammar rewrite must still reject DDL
    /// injection attempts, including ones a naive quote/paren allowance could
    /// otherwise let through (unbalanced quotes/parens, comment markers,
    /// embedded control characters).
    #[test]
    fn test_build_create_table_rejects_additional_ddl_injection_type_names() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        for type_name in [
            "int; DROP TABLE t",
            "text -- comment",
            "text /* c */",
            "text\nDROP TABLE t",
            "text\rDROP TABLE t",
            "enum('a",
            "text)",
            "text(",
        ] {
            let spec = CreateTableSpec {
                schema: None,
                table: "t".to_string(),
                columns: vec![crate::TransferColumn {
                    name: "c".to_string(),
                    type_name: Some(type_name.to_string()),
                    nullable: true,
                    is_primary_key: false,
                }],
                if_not_exists: false,
            };

            let result = builder.build_create_table(&spec);
            assert!(
                matches!(result, Err(GeneratorError::InvalidColumnType { .. })),
                "type spec '{type_name}' must be rejected: {:?}",
                result
            );
        }
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
    fn test_build_delete_many() {
        let dialect = DefaultSqlDialect;
        let builder = SqlQueryBuilder::new(&dialect);

        // DefaultSqlDialect does not support RETURNING, so the clause is omitted
        let delete = SqlDeleteRequest::new(
            "users".to_string(),
            Some("public".to_string()),
            crate::SemanticFilter::compare(
                "status",
                crate::WhereOperator::Eq,
                Value::Text("inactive".to_string()),
            ),
        )
        .with_returning(vec!["id".to_string()]);

        let sql = builder.build_delete_many(&delete).unwrap();
        assert_eq!(
            sql,
            "DELETE FROM \"public\".\"users\" WHERE \"status\" = 'inactive'"
        );
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
