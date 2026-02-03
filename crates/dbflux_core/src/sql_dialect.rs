use crate::Value;

/// Placeholder style for parameterized queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaceholderStyle {
    /// `?` placeholders (SQLite, MySQL).
    QuestionMark,
    /// `$1`, `$2`, etc. (PostgreSQL).
    DollarNumber,
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

    /// Escape a string for use inside a single-quoted literal.
    fn escape_string(&self, s: &str) -> String;

    /// Returns the placeholder style for this dialect.
    fn placeholder_style(&self) -> PlaceholderStyle;

    /// Whether this dialect supports RETURNING clause in INSERT/UPDATE/DELETE.
    /// PostgreSQL supports it natively; SQLite/MySQL require re-query.
    fn supports_returning(&self) -> bool {
        false
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
        }
    }

    fn escape_string(&self, s: &str) -> String {
        s.replace('\'', "''")
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::QuestionMark
    }
}
