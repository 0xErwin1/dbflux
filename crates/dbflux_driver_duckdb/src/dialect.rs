use dbflux_core::{ColumnAssignment, PlaceholderStyle, SqlDialect, Value};

pub struct DuckDbDialect;

pub(crate) static DUCKDB_DIALECT: DuckDbDialect = DuckDbDialect;

impl SqlDialect for DuckDbDialect {
    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn qualified_table(&self, schema: Option<&str>, table: &str) -> String {
        match schema {
            Some(schema) => format!(
                "{}.{}",
                self.quote_identifier(schema),
                self.quote_identifier(table)
            ),
            None => self.quote_identifier(table),
        }
    }

    fn value_to_literal(&self, value: &Value) -> String {
        match value {
            Value::Null | Value::Unsupported(_) => "NULL".to_string(),
            Value::Bool(value) => if *value { "TRUE" } else { "FALSE" }.to_string(),
            Value::Int(value) => value.to_string(),
            Value::Float(value) if value.is_finite() => value.to_string(),
            Value::Float(value) if value.is_nan() => "'nan'::DOUBLE".to_string(),
            Value::Float(value) if value.is_sign_positive() => "'inf'::DOUBLE".to_string(),
            Value::Float(_) => "'-inf'::DOUBLE".to_string(),
            Value::Decimal(value) => value.clone(),
            Value::Text(value) | Value::ObjectId(value) => {
                format!("'{}'", self.escape_string(value))
            }
            Value::Json(value) => format!("'{}'::JSON", self.escape_string(value)),
            Value::Bytes(bytes) => {
                let escaped: String = bytes.iter().map(|byte| format!("\\x{byte:02X}")).collect();
                format!("'{escaped}'::BLOB")
            }
            // DuckDB returns TIMESTAMP and TIMESTAMPTZ alike as UTC instants,
            // and TIMESTAMP is the common column type. A TIMESTAMPTZ literal
            // would shift by the session time zone once ICU is loaded.
            Value::DateTime(value) => format!(
                "TIMESTAMP '{}'",
                value.naive_utc().format("%Y-%m-%d %H:%M:%S%.f")
            ),
            Value::Date(value) => format!("DATE '{}'", value.format("%Y-%m-%d")),
            Value::Time(value) => format!("TIME '{}'", value.format("%H:%M:%S%.f")),
            Value::Array(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(|value| self.value_to_literal(value))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Value::Document(fields) => format!(
                "{{{}}}",
                fields
                    .iter()
                    .map(|(key, value)| format!(
                        "'{}': {}",
                        self.escape_string(key),
                        self.value_to_literal(value)
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }

    fn value_to_literal_typed(&self, value: &Value, column_type: Option<&str>) -> String {
        match (value, column_type) {
            (Value::DateTime(value), Some(column_type)) if has_time_zone(column_type) => {
                format!("'{}'::TIMESTAMPTZ", value.to_rfc3339())
            }
            _ => self.value_to_literal(value),
        }
    }

    fn escape_string(&self, value: &str) -> String {
        value.replace('\'', "''")
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::QuestionMark
    }

    fn supports_returning(&self) -> bool {
        true
    }

    fn supports_drop_cascade(&self) -> bool {
        true
    }

    fn build_upsert_statement(
        &self,
        schema: Option<&str>,
        table: &str,
        assignments: &[ColumnAssignment],
        conflict_columns: &[String],
        update_assignments: &[ColumnAssignment],
    ) -> Option<String> {
        if assignments.is_empty() || conflict_columns.is_empty() {
            return None;
        }

        let join = |items: Vec<String>| items.join(", ");
        let table = self.qualified_table(schema, table);
        let columns = join(
            assignments
                .iter()
                .map(|assignment| self.quote_identifier(&assignment.name))
                .collect(),
        );
        let values = join(
            assignments
                .iter()
                .map(|assignment| {
                    self.value_to_literal_typed(&assignment.value, assignment.type_name.as_deref())
                })
                .collect(),
        );
        let conflict = join(
            conflict_columns
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect(),
        );

        if update_assignments.is_empty() {
            return Some(format!(
                "INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT ({conflict}) DO NOTHING"
            ));
        }

        let updates = join(
            update_assignments
                .iter()
                .map(|assignment| {
                    format!(
                        "{} = {}",
                        self.quote_identifier(&assignment.name),
                        self.value_to_literal_typed(
                            &assignment.value,
                            assignment.type_name.as_deref()
                        )
                    )
                })
                .collect(),
        );

        Some(format!(
            "INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
        ))
    }
}

fn has_time_zone(column_type: &str) -> bool {
    let column_type = column_type.to_ascii_uppercase();
    column_type.contains("TIME ZONE") || column_type.starts_with("TIMESTAMPTZ")
}

#[cfg(test)]
mod tests {
    use super::DUCKDB_DIALECT;
    use dbflux_core::{SqlDialect, Value};

    #[test]
    fn quotes_identifiers_and_strings() {
        assert_eq!(DUCKDB_DIALECT.quote_identifier("a\"b"), "\"a\"\"b\"");
        assert_eq!(
            DUCKDB_DIALECT.qualified_table(Some("main"), "t"),
            "\"main\".\"t\""
        );
        assert_eq!(
            DUCKDB_DIALECT.value_to_literal(&Value::Text("it's".into())),
            "'it''s'"
        );
        assert_eq!(
            DUCKDB_DIALECT.value_to_literal(&Value::Bytes(vec![0x00, 0xAB])),
            "'\\x00\\xAB'::BLOB"
        );
    }

    #[test]
    fn timestamps_keep_their_column_time_zone_kind() {
        let instant = Value::DateTime(
            chrono::DateTime::parse_from_rfc3339("2026-01-02T03:04:05.5+00:00")
                .unwrap()
                .to_utc(),
        );
        assert_eq!(
            DUCKDB_DIALECT.value_to_literal_typed(&instant, Some("TIMESTAMP")),
            "TIMESTAMP '2026-01-02 03:04:05.500'"
        );
        assert_eq!(
            DUCKDB_DIALECT.value_to_literal_typed(&instant, Some("TIMESTAMP WITH TIME ZONE")),
            "'2026-01-02T03:04:05.500+00:00'::TIMESTAMPTZ"
        );
    }
}
