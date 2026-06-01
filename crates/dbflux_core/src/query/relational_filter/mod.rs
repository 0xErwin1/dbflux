pub mod count;
pub mod parser;
pub mod resolver;

#[cfg(test)]
mod integration_sqlite;

pub use parser::ParseError;
pub use resolver::{RelationalLowering, ResolveError};

use crate::query::visual_query::SourceTable;
use crate::schema::types::SchemaForeignKeyInfo;
use crate::sql::dialect::SqlDialect;

/// Unified error returned by `parse_and_resolve`.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RelationalFilterError {
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),
    #[error("resolve error: {0}")]
    Resolve(#[from] Box<ResolveError>),
}

/// Parse `input` and resolve all dotted-path predicates against `fks`.
///
/// Returns `Ok(RelationalLowering)` on full success. On parse failure or
/// resolve failure (ambiguity / unknown relation), returns `Err` so the
/// caller can fall back to the raw-filter path or surface an inline error.
pub fn parse_and_resolve(
    input: &str,
    source: SourceTable,
    fks: &[SchemaForeignKeyInfo],
    dialect: &dyn SqlDialect,
) -> Result<RelationalLowering, RelationalFilterError> {
    let ast = parser::parse(input).map_err(RelationalFilterError::Parse)?;
    let lowering = resolver::resolve(ast, source, fks, dialect)
        .map_err(|e| RelationalFilterError::Resolve(Box::new(e)))?;
    Ok(lowering)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::visual_query::SourceTable;
    use crate::schema::types::SchemaForeignKeyInfo;
    use crate::sql::dialect::{DefaultSqlDialect, PlaceholderStyle, SqlDialect};

    fn make_source(table: &str) -> SourceTable {
        SourceTable {
            schema: None,
            table: table.to_string(),
            alias: table.to_string(),
        }
    }

    fn make_fk(
        from_table: &str,
        from_col: &str,
        to_table: &str,
        to_col: &str,
    ) -> SchemaForeignKeyInfo {
        SchemaForeignKeyInfo {
            name: format!("fk_{}_{}", from_table, from_col),
            table_name: from_table.to_string(),
            columns: vec![from_col.to_string()],
            referenced_schema: None,
            referenced_table: to_table.to_string(),
            referenced_columns: vec![to_col.to_string()],
            on_delete: None,
            on_update: None,
        }
    }

    // T19: parse_and_resolve happy path
    #[test]
    fn parse_and_resolve_happy_path() {
        let fks = [make_fk("posts", "created_by_id", "users", "id")];
        let result = parse_and_resolve(
            "created_by.email = 'x'",
            make_source("posts"),
            &fks,
            &DefaultSqlDialect,
        );

        let lowering = result.expect("should succeed");
        assert_eq!(lowering.spec.joins.len(), 1);
        assert_eq!(lowering.diagnostics.join_count, 1);
        assert_eq!(lowering.diagnostics.relational_predicate_count, 1);
    }

    #[test]
    fn parse_and_resolve_parse_error_propagated() {
        let result = parse_and_resolve("status", make_source("posts"), &[], &DefaultSqlDialect);
        assert!(
            matches!(result, Err(RelationalFilterError::Parse(_))),
            "bare identifier with no comparator should be a parse error"
        );
    }

    #[test]
    fn parse_and_resolve_resolve_error_propagated() {
        let result = parse_and_resolve(
            "nonexistent.col = 1",
            make_source("posts"),
            &[],
            &DefaultSqlDialect,
        );
        assert!(
            matches!(result, Err(RelationalFilterError::Resolve(_))),
            "unknown relation should be a resolve error"
        );
    }

    #[test]
    fn module_exists() {
        // Smoke: the public function is callable from the module path.
        struct NopDialect;
        impl SqlDialect for NopDialect {
            fn quote_identifier(&self, name: &str) -> String {
                format!("\"{}\"", name)
            }
            fn qualified_table(&self, _schema: Option<&str>, table: &str) -> String {
                table.to_string()
            }
            fn value_to_literal(&self, _v: &crate::Value) -> String {
                "?".to_string()
            }
            fn escape_string(&self, s: &str) -> String {
                s.to_string()
            }
            fn placeholder_style(&self) -> PlaceholderStyle {
                PlaceholderStyle::QuestionMark
            }
        }

        let source = SourceTable {
            schema: None,
            table: "t".to_string(),
            alias: "t".to_string(),
        };

        let _ = parse_and_resolve("a = 1", source, &[], &NopDialect);
    }
}
