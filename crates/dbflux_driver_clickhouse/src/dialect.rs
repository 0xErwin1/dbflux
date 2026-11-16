use dbflux_core::{PlaceholderStyle, SqlDialect, Value};

pub struct ClickHouseDialect;

impl SqlDialect for ClickHouseDialect {
    fn type_value_expressions(&self, type_name: &str) -> Vec<&'static str> {
        use dbflux_core::TypeFamily;

        match dbflux_core::type_family(type_name) {
            Some(TypeFamily::Timestamp) => vec!["now()"],
            Some(TypeFamily::Date) => vec!["today()"],
            Some(TypeFamily::Time) => vec!["now()"],
            Some(TypeFamily::Boolean) => vec!["true", "false"],
            Some(TypeFamily::Numeric) => vec!["0"],
            Some(TypeFamily::Text) => vec!["''"],
            Some(TypeFamily::Uuid) => vec!["generateUUIDv4()"],
            None => Vec::new(),
        }
    }

    fn text_column_type(&self) -> &'static str {
        "String"
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
    }

    fn qualified_table(&self, database: Option<&str>, table: &str) -> String {
        match database {
            Some(database) => format!(
                "{}.{}",
                self.quote_identifier(database),
                self.quote_identifier(table)
            ),
            None => self.quote_identifier(table),
        }
    }

    fn value_to_literal(&self, value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(value) => if *value { "true" } else { "false" }.to_string(),
            Value::Int(value) => value.to_string(),
            Value::Float(value) if value.is_finite() => value.to_string(),
            Value::Float(value) if value.is_nan() => "nan".to_string(),
            Value::Float(value) if value.is_sign_positive() => "inf".to_string(),
            Value::Float(_) => "-inf".to_string(),
            Value::Decimal(value) => value.clone(),
            Value::Text(value) | Value::Json(value) | Value::ObjectId(value) => {
                format!("'{}'", self.escape_string(value))
            }
            Value::Bytes(value) => format!("unhex('{}')", hex::encode(value)),
            Value::DateTime(value) => format!("'{}'", value.to_rfc3339()),
            Value::Date(value) => format!("'{}'", value.format("%Y-%m-%d")),
            Value::Time(value) => format!("'{}'", value.format("%H:%M:%S%.f")),
            Value::Array(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(|value| self.value_to_literal(value))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Value::Document(value) => {
                let json = serde_json::to_string(value).unwrap_or_else(|error| {
                    log::warn!("Failed to serialize ClickHouse object literal: {error}");
                    "{}".to_string()
                });
                format!("'{}'", self.escape_string(&json))
            }
            Value::Unsupported(_) => "NULL".to_string(),
        }
    }

    fn escape_string(&self, value: &str) -> String {
        value.replace('\\', "\\\\").replace('\'', "\\'")
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::QuestionMark
    }

    fn normalize_identifier<'a>(&self, name: &'a str) -> std::borrow::Cow<'a, str> {
        std::borrow::Cow::Borrowed(name)
    }
}

pub static CLICKHOUSE_DIALECT: ClickHouseDialect = ClickHouseDialect;

#[cfg(test)]
mod tests {
    #[test]
    fn clickhouse_type_value_expressions() {
        use dbflux_core::SqlDialect as _;

        let dialect = &super::ClickHouseDialect;
        assert_eq!(dialect.type_value_expressions("DateTime"), vec!["now()"]);
        assert_eq!(
            dialect.type_value_expressions("Nullable(Date)"),
            vec!["today()"]
        );
        assert_eq!(
            dialect.type_value_expressions("UUID"),
            vec!["generateUUIDv4()"]
        );
        assert_eq!(dialect.type_value_expressions("UInt64"), vec!["0"]);
        assert_eq!(
            dialect.type_value_expressions("Array(String)"),
            Vec::<&str>::new()
        );
    }

    use super::CLICKHOUSE_DIALECT;
    use dbflux_core::{PlaceholderStyle, SqlDialect, Value};

    #[test]
    fn untyped_columns_use_the_string_type() {
        assert_eq!(CLICKHOUSE_DIALECT.text_column_type(), "String");
    }

    #[test]
    fn quotes_clickhouse_identifiers() {
        assert_eq!(CLICKHOUSE_DIALECT.quote_identifier("events"), "`events`");
        assert_eq!(
            CLICKHOUSE_DIALECT.quote_identifier("odd\\`name"),
            "`odd\\\\\\`name`"
        );
        assert_eq!(
            CLICKHOUSE_DIALECT.qualified_table(Some("analytics"), "events"),
            "`analytics`.`events`"
        );
    }

    #[test]
    fn escapes_clickhouse_string_literals() {
        assert_eq!(
            CLICKHOUSE_DIALECT.value_to_literal(&Value::Text("a'b\\c".to_string())),
            "'a\\'b\\\\c'"
        );
        assert_eq!(
            CLICKHOUSE_DIALECT.placeholder_style(),
            PlaceholderStyle::QuestionMark
        );
    }
}
