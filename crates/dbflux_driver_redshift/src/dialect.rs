use dbflux_core::{PlaceholderStyle, SqlDialect, Value};

/// Amazon Redshift SQL dialect.
///
/// Redshift's wire protocol and SQL surface are PostgreSQL-derived: double-quoted
/// identifiers, `$n` placeholders, and a plain `LIMIT n` clause. This driver is
/// read-only, so the dialect intentionally leaves `supports_returning` and
/// `build_upsert_statement` at their trait defaults (`false` / `None`) — Redshift
/// has no RETURNING clause and this crate never generates a mutation statement.
pub struct RedshiftDialect;

impl SqlDialect for RedshiftDialect {
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
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => {
                if f.is_nan() {
                    "'NaN'".to_string()
                } else if f.is_infinite() {
                    if f.is_sign_positive() {
                        "'Infinity'".to_string()
                    } else {
                        "'-Infinity'".to_string()
                    }
                } else {
                    f.to_string()
                }
            }
            Value::Decimal(s) => format!("'{}'", self.escape_string(s)),
            Value::Text(s) => format!("'{}'", self.escape_string(s)),
            Value::Bytes(b) => format!("'\\x{}'", hex::encode(b)),
            Value::DateTime(dt) => format!("'{}'", dt.to_rfc3339()),
            Value::Date(d) => format!("'{}'", d.format("%Y-%m-%d")),
            Value::Time(t) => format!("'{}'", t.format("%H:%M:%S%.f")),
            Value::Json(s) => format!("'{}'", self.escape_string(s)),
            Value::ObjectId(id) => format!("'{}'", self.escape_string(id)),
            Value::Array(arr) => {
                let items: Vec<String> = arr.iter().map(|v| self.value_to_literal(v)).collect();
                format!(
                    "'{}'",
                    self.escape_string(&format!("[{}]", items.join(", ")))
                )
            }
            Value::Document(doc) => {
                let json = serde_json::to_string(doc).unwrap_or_else(|_| "{}".to_string());
                format!("'{}'", self.escape_string(&json))
            }
            Value::Unsupported(_) => "NULL".to_string(),
        }
    }

    fn escape_string(&self, s: &str) -> String {
        s.replace('\'', "''")
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::DollarNumber
    }
}

pub static REDSHIFT_DIALECT: RedshiftDialect = RedshiftDialect;

#[cfg(test)]
mod tests {
    use super::REDSHIFT_DIALECT;
    use dbflux_core::{
        ColumnInfo, PlaceholderStyle, QueryGenerator, ReadTemplateOperation, ReadTemplateRequest,
        SqlDialect, SqlGenerationOptions, SqlMutationGenerator, Value,
    };

    #[test]
    fn quote_identifier_wraps_in_double_quotes_and_escapes_embedded_quotes() {
        assert_eq!(REDSHIFT_DIALECT.quote_identifier("users"), "\"users\"");
        assert_eq!(
            REDSHIFT_DIALECT.quote_identifier("weird\"name"),
            "\"weird\"\"name\""
        );
    }

    #[test]
    fn qualified_table_includes_schema_when_present() {
        assert_eq!(
            REDSHIFT_DIALECT.qualified_table(Some("public"), "orders"),
            "\"public\".\"orders\""
        );
        assert_eq!(
            REDSHIFT_DIALECT.qualified_table(None, "orders"),
            "\"orders\""
        );
    }

    #[test]
    fn placeholder_style_is_dollar_number() {
        assert_eq!(
            REDSHIFT_DIALECT.placeholder_style(),
            PlaceholderStyle::DollarNumber
        );
    }

    #[test]
    fn limit_clause_defaults_to_ansi_limit() {
        assert_eq!(REDSHIFT_DIALECT.limit_clause(25), "LIMIT 25");
    }

    #[test]
    fn limit_offset_clause_appends_offset_when_nonzero() {
        assert_eq!(
            REDSHIFT_DIALECT.limit_offset_clause(25, 50),
            "LIMIT 25 OFFSET 50"
        );
        assert_eq!(REDSHIFT_DIALECT.limit_offset_clause(25, 0), "LIMIT 25");
    }

    #[test]
    fn supports_returning_is_false() {
        assert!(!REDSHIFT_DIALECT.supports_returning());
    }

    #[test]
    fn build_upsert_statement_is_unsupported() {
        assert!(
            REDSHIFT_DIALECT
                .build_upsert_statement(None, "t", &[], &[], &[])
                .is_none()
        );
    }

    #[test]
    fn value_to_literal_escapes_single_quotes_in_text() {
        assert_eq!(
            REDSHIFT_DIALECT.value_to_literal(&Value::Text("O'Brien".to_string())),
            "'O''Brien'"
        );
    }

    #[test]
    fn value_to_literal_renders_null_and_bool() {
        assert_eq!(REDSHIFT_DIALECT.value_to_literal(&Value::Null), "NULL");
        assert_eq!(
            REDSHIFT_DIALECT.value_to_literal(&Value::Bool(true)),
            "TRUE"
        );
        assert_eq!(
            REDSHIFT_DIALECT.value_to_literal(&Value::Bool(false)),
            "FALSE"
        );
    }

    /// Golden-SQL regression: `SqlMutationGenerator` is the seam the visual
    /// query builder and browse paths use to render a table's SELECT
    /// statement. Exercising it here (rather than only the raw dialect
    /// methods) proves the dialect renders correctly through that pipeline.
    #[test]
    fn generate_read_template_select_all_renders_qualified_quoted_select() {
        let generator = SqlMutationGenerator::new(&REDSHIFT_DIALECT);
        let columns: Vec<ColumnInfo> = Vec::new();

        let generated = generator
            .generate_read_template(&ReadTemplateRequest {
                operation: ReadTemplateOperation::SelectAll,
                schema: Some("public"),
                table: "orders",
                columns: &columns,
                options: SqlGenerationOptions {
                    fully_qualified: true,
                    compact: true,
                },
            })
            .expect("SqlMutationGenerator must support SelectAll");

        assert_eq!(generated.text, "SELECT * FROM \"public\".\"orders\";");
    }

    #[test]
    fn generate_read_template_select_all_multiline_when_not_compact() {
        let generator = SqlMutationGenerator::new(&REDSHIFT_DIALECT);
        let columns: Vec<ColumnInfo> = Vec::new();

        let generated = generator
            .generate_read_template(&ReadTemplateRequest {
                operation: ReadTemplateOperation::SelectAll,
                schema: None,
                table: "orders",
                columns: &columns,
                options: SqlGenerationOptions {
                    fully_qualified: false,
                    compact: false,
                },
            })
            .expect("SqlMutationGenerator must support SelectAll");

        assert_eq!(generated.text, "SELECT *\nFROM \"orders\";");
    }
}
