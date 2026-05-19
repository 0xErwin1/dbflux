use crate::Value;
use serde::{Deserialize, Serialize};

/// Placeholder style for parameterized queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PlaceholderStyle {
    /// `?` placeholders (SQLite, MySQL).
    QuestionMark,
    /// `$1`, `$2`, etc. (PostgreSQL).
    DollarNumber,
    /// `:name` or `:1` named/ordinal placeholders (Oracle, some JDBC).
    NamedColon,
    /// `@param` style placeholders (SQL Server, SQL Azure).
    AtSign,
}

/// Database-specific SQL syntax (quoting, escaping, literals).
pub trait SqlDialect: Send + Sync {
    /// Quote an identifier (table/column name).
    ///
    /// - PostgreSQL/SQLite: `"name"` (double quotes)
    /// - MySQL: `` `name` `` (backticks)
    fn quote_identifier(&self, name: &str) -> String;

    /// Build a qualified table reference.
    ///
    /// - PostgreSQL: `"schema"."table"`
    /// - MySQL: `` `database`.`table` ``
    /// - SQLite: `"table"` (no schema prefix)
    fn qualified_table(&self, schema: Option<&str>, table: &str) -> String;

    /// Convert a Value to a SQL literal string.
    fn value_to_literal(&self, value: &Value) -> String;

    /// Convert a Value to a SQL literal string, optionally guided by the
    /// target column's driver-reported type name.
    ///
    /// Default implementation ignores the type and delegates to
    /// [`Self::value_to_literal`]. Dialects override this to handle cases
    /// where the destination column type is needed to pick the right literal
    /// syntax — e.g. PostgreSQL needs `ARRAY[...]::text[]` for `text[]`
    /// columns instead of the generic `::jsonb` fallback.
    fn value_to_literal_typed(&self, value: &Value, _col_type: Option<&str>) -> String {
        self.value_to_literal(value)
    }

    /// Escape a string for use inside a single-quoted literal.
    fn escape_string(&self, s: &str) -> String;

    /// Returns the placeholder style for this dialect.
    fn placeholder_style(&self) -> PlaceholderStyle;

    /// Whether this dialect supports RETURNING clause in INSERT/UPDATE/DELETE.
    /// PostgreSQL supports it natively; SQLite/MySQL require re-query.
    fn supports_returning(&self) -> bool {
        false
    }

    /// Build the column expression used for value comparisons.
    ///
    /// Most dialects can compare directly on the quoted column name.
    /// PostgreSQL overrides this for selected text-like types.
    fn comparison_column_expr(&self, col_name: &str, _col_type: &str) -> String {
        col_name.to_string()
    }

    /// Build a JSON comparison expression.
    ///
    /// Default implementation compares with the regular operator and literal.
    /// Dialects can override this when explicit casting is required.
    fn json_filter_expr(&self, col_name: &str, op: &str, literal: &str, _col_type: &str) -> String {
        format!("{} {} {}", col_name, op, literal)
    }

    /// Build an UPSERT statement for this dialect.
    fn build_upsert_statement(
        &self,
        _schema: Option<&str>,
        _table: &str,
        _assignments: &[crate::data::crud::ColumnAssignment],
        _conflict_columns: &[String],
        _update_assignments: &[crate::data::crud::ColumnAssignment],
    ) -> Option<String> {
        None
    }
}

/// Default SQL dialect using ANSI SQL conventions (double-quote identifiers).
pub struct DefaultSqlDialect;

impl SqlDialect for DefaultSqlDialect {
    fn quote_identifier(&self, name: &str) -> String {
        let escaped = name.replace('"', "\"\"");
        format!("\"{}\"", escaped)
    }

    fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
        match schema {
            Some(s) => format!(
                "{}.{}",
                self.quote_identifier(s),
                self.quote_identifier(table)
            ),
            None => self.quote_identifier(table),
        }
    }

    fn value_to_literal(&self, value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => {
                if f.is_nan() {
                    "'NaN'".to_string()
                } else if f.is_infinite() {
                    if *f > 0.0 {
                        "'Infinity'".to_string()
                    } else {
                        "'-Infinity'".to_string()
                    }
                } else {
                    f.to_string()
                }
            }
            Value::Text(s) => format!("'{}'", self.escape_string(s)),
            Value::Bytes(b) => {
                let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                format!("X'{}'", hex)
            }
            Value::Json(s) => format!("'{}'", self.escape_string(s)),
            Value::Decimal(s) => s.clone(),
            Value::DateTime(dt) => format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S%.f")),
            Value::Date(d) => format!("'{}'", d.format("%Y-%m-%d")),
            Value::Time(t) => format!("'{}'", t.format("%H:%M:%S%.f")),
            Value::Array(arr) => {
                let items: Vec<String> = arr.iter().map(|v| self.value_to_literal(v)).collect();
                format!("ARRAY[{}]", items.join(", "))
            }
            Value::Document(doc) => {
                let json = serde_json::to_string(doc).unwrap_or_else(|_| "{}".to_string());
                format!("'{}'", self.escape_string(&json))
            }
            Value::ObjectId(id) => format!("'{}'", self.escape_string(id)),
            Value::Unsupported(_) => "NULL".to_string(),
        }
    }

    fn escape_string(&self, s: &str) -> String {
        s.replace('\'', "''")
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::QuestionMark
    }

    fn build_upsert_statement(
        &self,
        schema: Option<&str>,
        table: &str,
        assignments: &[crate::data::crud::ColumnAssignment],
        conflict_columns: &[String],
        update_assignments: &[crate::data::crud::ColumnAssignment],
    ) -> Option<String> {
        if assignments.is_empty() || conflict_columns.is_empty() {
            return None;
        }

        let table = self.qualified_table(schema, table);
        let columns = assignments
            .iter()
            .map(|a| self.quote_identifier(&a.name))
            .collect::<Vec<_>>()
            .join(", ");
        let values = assignments
            .iter()
            .map(|a| self.value_to_literal_typed(&a.value, a.type_name.as_deref()))
            .collect::<Vec<_>>()
            .join(", ");
        let conflict_columns = conflict_columns
            .iter()
            .map(|column| self.quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");

        if update_assignments.is_empty() {
            return Some(format!(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
                table, columns, values, conflict_columns
            ));
        }

        let update_clause = update_assignments
            .iter()
            .map(|a| {
                format!(
                    "{} = {}",
                    self.quote_identifier(&a.name),
                    self.value_to_literal_typed(&a.value, a.type_name.as_deref())
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        Some(format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
            table, columns, values, conflict_columns, update_clause
        ))
    }
}
