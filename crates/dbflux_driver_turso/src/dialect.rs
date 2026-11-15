//! SQLite dialect as spoken by Turso, plus pure value/type conversions.
//!
//! Everything here is I/O free so it can be unit tested without a server.

use dbflux_core::{
    AddColumnRequest, AlterColumnRequest, CodeGenCapabilities, CodeGenerator, ColumnInfo,
    ColumnKind, CreateIndexRequest, DbError, DdlRejection, DropColumnRequest, DropIndexRequest,
    PlaceholderStyle, ReindexRequest, SqlDialect, TableInfo, Value, validate_ddl_fragment,
};
use turso_serverless::Value as TursoValue;

pub struct TursoDialect;

pub(crate) static TURSO_DIALECT: TursoDialect = TursoDialect;

impl SqlDialect for TursoDialect {
    fn quote_identifier(&self, name: &str) -> String {
        quote_ident(name)
    }

    fn qualified_table(&self, _schema: Option<&str>, table: &str) -> String {
        // SQLite has no schema prefixes for table references.
        quote_ident(table)
    }

    fn value_to_literal(&self, value: &Value) -> String {
        value_to_literal(value)
    }

    fn escape_string(&self, s: &str) -> String {
        escape_string(s)
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::QuestionMark
    }

    fn mutation_target_alias(&self) -> dbflux_core::MutationTargetAlias {
        dbflux_core::MutationTargetAlias::RequiresAs
    }

    fn build_upsert_statement(
        &self,
        schema: Option<&str>,
        table: &str,
        assignments: &[dbflux_core::ColumnAssignment],
        conflict_columns: &[String],
        update_assignments: &[dbflux_core::ColumnAssignment],
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
                "INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT ({conflict_columns}) DO NOTHING"
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
            "INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT ({conflict_columns}) DO UPDATE SET {update_clause}"
        ))
    }
}

pub(crate) fn quote_ident(ident: &str) -> String {
    debug_assert!(!ident.is_empty(), "identifier cannot be empty");
    dbflux_core::quote_double_quoted_identifier(ident)
}

pub(crate) fn escape_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// A quoted text literal. SQLite reads a statement up to its first NUL, so
/// text holding one is written as its quoted segments joined with
/// `char(0)`. Unlike a hex cast, this does not depend on the database's text
/// encoding; text without a NUL stays a plain quoted literal.
fn text_literal(text: &str) -> String {
    if !text.contains('\0') {
        return format!("'{}'", escape_string(text));
    }

    let segments: Vec<String> = text
        .split('\0')
        .map(|segment| format!("'{}'", escape_string(segment)))
        .collect();

    format!("({})", join_with_nul(&segments))
}

/// The most segments joined in one flat `||` chain. SQLite parses a chain as a
/// left-deep tree and refuses one deeper than its expression depth limit
/// (1000 by default), so a longer run is split into parenthesized halves.
const MAX_FLAT_SEGMENTS: usize = 64;

fn join_with_nul(segments: &[String]) -> String {
    const SEPARATOR: &str = " || char(0) || ";

    if segments.len() <= MAX_FLAT_SEGMENTS {
        return segments.join(SEPARATOR);
    }

    let (left, right) = segments.split_at(segments.len() / 2);
    format!(
        "({}){SEPARATOR}({})",
        join_with_nul(left),
        join_with_nul(right)
    )
}

/// Renders a value as a SQLite literal for statements that inline values.
pub(crate) fn value_to_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.is_nan() || f.is_infinite() {
                "NULL".to_string()
            } else {
                f.to_string()
            }
        }
        Value::Decimal(s) => format!("'{}'", escape_string(s)),
        Value::Text(s) | Value::Json(s) => text_literal(s),
        Value::Bytes(b) => format!("X'{}'", hex::encode(b)),
        Value::DateTime(dt) => format!("'{}'", dt.to_rfc3339()),
        Value::Date(d) => format!("'{}'", d.format("%Y-%m-%d")),
        Value::Time(t) => format!("'{}'", t.format("%H:%M:%S%.f")),
        Value::ObjectId(id) => text_literal(id),
        Value::Unsupported(_) => "NULL".to_string(),
        Value::Array(_) | Value::Document(_) => {
            format!("'{}'", escape_string(&value.to_json_string()))
        }
    }
}

/// Converts a core value into a bound SDK parameter.
///
/// Non-finite floats are rejected instead of being silently encoded as NULL by
/// the SDK. Structured values travel as JSON text, which is how SQLite stores
/// them anyway.
pub(crate) fn value_to_param(value: &Value) -> Result<TursoValue, DbError> {
    Ok(match value {
        Value::Null => TursoValue::Null,
        Value::Bool(b) => TursoValue::Integer(i64::from(*b)),
        Value::Int(i) => TursoValue::Integer(*i),
        Value::Float(f) => {
            if !f.is_finite() {
                return Err(DbError::query_failed(
                    "Cannot bind a non-finite floating point parameter",
                ));
            }
            TursoValue::Real(*f)
        }
        Value::Text(s) | Value::Json(s) | Value::Decimal(s) | Value::ObjectId(s) => {
            TursoValue::Text(s.clone())
        }
        Value::Bytes(b) => TursoValue::Blob(b.clone()),
        Value::DateTime(dt) => TursoValue::Text(dt.to_rfc3339()),
        Value::Date(d) => TursoValue::Text(d.format("%Y-%m-%d").to_string()),
        Value::Time(t) => TursoValue::Text(t.format("%H:%M:%S%.f").to_string()),
        Value::Array(_) | Value::Document(_) => TursoValue::Text(value.to_json_string()),
        Value::Unsupported(kind) => {
            return Err(DbError::query_failed(format!(
                "Cannot bind unsupported value of kind '{kind}'"
            )));
        }
    })
}

/// Converts an SDK result value into a core value. Lossless for every variant.
pub(crate) fn value_from_turso(value: TursoValue) -> Value {
    match value {
        TursoValue::Null => Value::Null,
        TursoValue::Integer(i) => Value::Int(i),
        TursoValue::Real(f) => Value::Float(f),
        TursoValue::Text(t) => Value::Text(t),
        TursoValue::Blob(b) => Value::Bytes(b),
    }
}

/// Maps a SQLite declared type to a `ColumnKind` using type-affinity rules.
pub(crate) fn kind_from_decltype(decl: Option<&str>) -> ColumnKind {
    let decl = match decl {
        Some(d) if !d.is_empty() => d.to_uppercase(),
        _ => return ColumnKind::Unknown,
    };

    if decl.contains("INT") {
        return ColumnKind::Integer;
    }
    if decl.contains("REAL")
        || decl.contains("FLOA")
        || decl.contains("DOUB")
        || decl.contains("NUMERIC")
        || decl.contains("DECIMAL")
    {
        return ColumnKind::Float;
    }
    // A time of day has no absolute instant, so the chart engine cannot plot
    // it; the other drivers report it as Unknown too.
    if decl.starts_with("TIME") && !decl.starts_with("TIMESTAMP") {
        return ColumnKind::Unknown;
    }
    if decl.contains("DATE") || decl.contains("TIME") || decl.contains("STAMP") {
        return ColumnKind::Timestamp;
    }
    if decl.contains("CHAR") || decl.contains("TEXT") || decl.contains("CLOB") {
        return ColumnKind::Text;
    }

    ColumnKind::Unknown
}

/// Infers a kind from the actual values of a column when no declared type is
/// available (expressions, `SELECT 1 + 1`, PRAGMA output). Every non-null value
/// must agree; blobs and mixed columns stay `Unknown`.
pub(crate) fn kind_from_values<'a>(values: impl Iterator<Item = &'a Value>) -> ColumnKind {
    let mut kind: Option<ColumnKind> = None;
    for value in values {
        let current = match value {
            Value::Null => continue,
            Value::Int(_) => ColumnKind::Integer,
            Value::Float(_) => ColumnKind::Float,
            Value::Text(_) => ColumnKind::Text,
            _ => return ColumnKind::Unknown,
        };
        match kind {
            None => kind = Some(current),
            Some(existing) if existing == current => {}
            Some(_) => return ColumnKind::Unknown,
        }
    }
    kind.unwrap_or(ColumnKind::Unknown)
}

/// Turso-flavoured CREATE TABLE that keeps `INTEGER PRIMARY KEY` inline so the
/// column keeps SQLite rowid semantics.
pub(crate) fn generate_create_table(table: &TableInfo) -> String {
    let mut sql = format!("CREATE TABLE {} (\n", quote_ident(&table.name));
    let cols = table.columns.as_deref().unwrap_or(&[]);

    let pk_columns: Vec<&ColumnInfo> = cols.iter().filter(|c| c.is_primary_key).collect();
    let single_integer_pk = pk_columns.len() == 1
        && pk_columns
            .first()
            .is_some_and(|c| c.type_name.eq_ignore_ascii_case("INTEGER"));

    for (i, col) in cols.iter().enumerate() {
        let mut line = if col.type_name.is_empty() {
            format!("    {}", quote_ident(&col.name))
        } else {
            format!("    {} {}", quote_ident(&col.name), col.type_name)
        };

        if !col.nullable {
            line.push_str(" NOT NULL");
        }
        if single_integer_pk && col.is_primary_key {
            line.push_str(" PRIMARY KEY");
        }
        if let Some(ref default) = col.default_value {
            line.push_str(&format!(" DEFAULT {default}"));
        }

        let is_last_column = i + 1 == cols.len();
        let needs_pk_constraint = !pk_columns.is_empty() && !single_integer_pk;
        if !is_last_column || needs_pk_constraint {
            line.push(',');
        }

        sql.push_str(&line);
        sql.push('\n');
    }

    if !pk_columns.is_empty() && !single_integer_pk {
        let pk_quoted: Vec<String> = pk_columns.iter().map(|c| quote_ident(&c.name)).collect();
        sql.push_str(&format!("    PRIMARY KEY ({})\n", pk_quoted.join(", ")));
    }

    sql.push_str(");");
    sql
}

// =============================================================================
// DDL code generation
// =============================================================================

pub struct TursoCodeGenerator;

pub(crate) static TURSO_CODE_GENERATOR: TursoCodeGenerator = TursoCodeGenerator;

impl TursoCodeGenerator {
    fn quote(&self, name: &str) -> String {
        quote_ident(name)
    }

    fn qualified(&self, schema: Option<&str>, name: &str) -> String {
        TURSO_DIALECT.qualified_table(schema, name)
    }
}

impl CodeGenerator for TursoCodeGenerator {
    fn capabilities(&self) -> CodeGenCapabilities {
        CodeGenCapabilities::CRUD
            | CodeGenCapabilities::INDEXES
            | CodeGenCapabilities::REINDEX
            | CodeGenCapabilities::CREATE_TABLE
            | CodeGenCapabilities::DROP_TABLE
            | CodeGenCapabilities::ADD_COLUMN
            | CodeGenCapabilities::DROP_COLUMN
    }

    fn generate_create_index(&self, req: &CreateIndexRequest) -> Option<String> {
        let unique = if req.unique { "UNIQUE " } else { "" };
        let table = self.qualified(req.schema_name, req.table_name);
        let cols = req
            .columns
            .iter()
            .map(|c| self.quote(c))
            .collect::<Vec<_>>()
            .join(", ");

        Some(format!(
            "CREATE {unique}INDEX {} ON {table} ({cols});",
            self.quote(req.index_name)
        ))
    }

    fn generate_drop_index(&self, req: &DropIndexRequest) -> Option<String> {
        Some(format!(
            "DROP INDEX {};",
            self.qualified(req.schema_name, req.index_name)
        ))
    }

    fn generate_reindex(&self, req: &ReindexRequest) -> Option<String> {
        Some(format!(
            "REINDEX {};",
            self.qualified(req.schema_name, req.index_name)
        ))
    }

    fn generate_add_column(&self, req: &AddColumnRequest) -> Result<Vec<String>, DdlRejection> {
        validate_ddl_fragment(req.type_name, "column type")?;
        if let Some(default) = req.default {
            validate_ddl_fragment(default, "column default")?;
        }

        let table = self.qualified(req.schema_name, req.table_name);
        let mut sql = format!(
            "ALTER TABLE {table} ADD COLUMN {} {}",
            self.quote(req.column_name),
            req.type_name
        );
        if !req.nullable {
            sql.push_str(" NOT NULL");
        }
        if let Some(default) = req.default {
            sql.push_str(&format!(" DEFAULT {default}"));
        }
        sql.push(';');

        Ok(vec![sql])
    }

    fn generate_drop_column(&self, req: &DropColumnRequest) -> Result<Vec<String>, DdlRejection> {
        // Turso and libSQL ship SQLite >= 3.35, which supports DROP COLUMN.
        let table = self.qualified(req.schema_name, req.table_name);
        Ok(vec![format!(
            "ALTER TABLE {table} DROP COLUMN {};",
            self.quote(req.column_name)
        )])
    }

    fn generate_alter_column(
        &self,
        _req: &AlterColumnRequest,
    ) -> Result<Vec<String>, DdlRejection> {
        Err(DdlRejection {
            reason: "SQLite requires a table rebuild".to_string(),
            followup: Some("DBF-158"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn decltype_affinity_mapping() {
        assert_eq!(kind_from_decltype(Some("INTEGER")), ColumnKind::Integer);
        assert_eq!(kind_from_decltype(Some("bigint")), ColumnKind::Integer);
        assert_eq!(kind_from_decltype(Some("REAL")), ColumnKind::Float);
        assert_eq!(kind_from_decltype(Some("DECIMAL(10,2)")), ColumnKind::Float);
        assert_eq!(kind_from_decltype(Some("DATETIME")), ColumnKind::Timestamp);
        assert_eq!(kind_from_decltype(Some("VARCHAR(20)")), ColumnKind::Text);
        assert_eq!(kind_from_decltype(Some("BLOB")), ColumnKind::Unknown);
        assert_eq!(kind_from_decltype(Some("")), ColumnKind::Unknown);
        assert_eq!(kind_from_decltype(None), ColumnKind::Unknown);
    }

    #[test]
    fn decltype_time_of_day_is_unknown() {
        assert_eq!(kind_from_decltype(Some("TIME")), ColumnKind::Unknown);
        assert_eq!(kind_from_decltype(Some("time")), ColumnKind::Unknown);
        assert_eq!(kind_from_decltype(Some("TIME(3)")), ColumnKind::Unknown);
        assert_eq!(kind_from_decltype(Some("TIMETZ")), ColumnKind::Unknown);
        assert_eq!(
            kind_from_decltype(Some("TIME WITH TIME ZONE")),
            ColumnKind::Unknown
        );
        assert_eq!(kind_from_decltype(Some("TIMESTAMP")), ColumnKind::Timestamp);
        assert_eq!(
            kind_from_decltype(Some("TIMESTAMP WITH TIME ZONE")),
            ColumnKind::Timestamp
        );
    }

    #[test]
    fn text_with_nul_bytes_is_written_as_segments_joined_with_char_zero() {
        let literal = |text: &str| value_to_literal(&Value::Text(text.to_string()));

        assert_eq!(literal("a\0b"), "('a' || char(0) || 'b')");
        assert_eq!(literal("\0a"), "('' || char(0) || 'a')");
        assert_eq!(literal("a\0"), "('a' || char(0) || '')");
        assert_eq!(
            literal("a\0\0b"),
            "('a' || char(0) || '' || char(0) || 'b')"
        );
        assert_eq!(literal("\0"), "('' || char(0) || '')");
        assert_eq!(literal("it's\0"), "('it''s' || char(0) || '')");
        assert_eq!(
            value_to_literal(&Value::Json("a\0b".to_string())),
            "('a' || char(0) || 'b')"
        );
    }

    #[test]
    fn long_nul_runs_build_a_shallow_expression() {
        let literal = value_to_literal(&Value::Text("\0".repeat(2_000)));
        let mut operators_per_level = vec![0usize];
        let mut widest = 0usize;
        for part in literal.split_inclusive(['(', ')', '|']) {
            match part.chars().last() {
                Some('(') => operators_per_level.push(0),
                Some(')') => {
                    operators_per_level.pop();
                }
                Some('|') => {
                    if let Some(count) = operators_per_level.last_mut() {
                        *count += 1;
                        widest = widest.max(*count);
                    }
                }
                _ => {}
            }
        }
        assert!(widest <= 260, "{widest} pipes chained at one level");
        assert!(
            literal.matches(" || ").count() >= 4_000,
            "every NUL still needs its separators"
        );
        assert!(!literal.contains('\0'));
    }

    #[test]
    fn value_scan_infers_homogeneous_kinds_only() {
        let ints = [Value::Null, Value::Int(1), Value::Int(2)];
        assert_eq!(kind_from_values(ints.iter()), ColumnKind::Integer);

        let mixed = [Value::Int(1), Value::Text("x".into())];
        assert_eq!(kind_from_values(mixed.iter()), ColumnKind::Unknown);

        let blobs = [Value::Bytes(vec![1])];
        assert_eq!(kind_from_values(blobs.iter()), ColumnKind::Unknown);

        let empty: [Value; 0] = [];
        assert_eq!(kind_from_values(empty.iter()), ColumnKind::Unknown);
    }

    #[test]
    fn params_round_trip_and_reject_non_finite_floats() {
        assert_eq!(
            value_to_param(&Value::Bool(true)).unwrap(),
            TursoValue::Integer(1)
        );
        assert_eq!(
            value_to_param(&Value::Bytes(vec![1, 2])).unwrap(),
            TursoValue::Blob(vec![1, 2])
        );
        assert!(value_to_param(&Value::Float(f64::NAN)).is_err());
        assert!(value_to_param(&Value::Unsupported("x".into())).is_err());

        let mut doc = BTreeMap::new();
        doc.insert("a".to_string(), Value::Int(1));
        assert_eq!(
            value_to_param(&Value::Document(doc)).unwrap(),
            TursoValue::Text("{\"a\":1}".into())
        );

        assert_eq!(
            value_from_turso(TursoValue::Text("t".into())),
            Value::Text("t".into())
        );
        assert_eq!(value_from_turso(TursoValue::Real(1.5)), Value::Float(1.5));
    }

    #[test]
    fn literals_escape_quotes_and_encode_bytes() {
        assert_eq!(value_to_literal(&Value::Text("it's".into())), "'it''s'");
        assert_eq!(value_to_literal(&Value::Bytes(vec![0xAB])), "X'ab'");
        assert_eq!(value_to_literal(&Value::Float(f64::INFINITY)), "NULL");
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
    }

    #[test]
    fn create_table_keeps_integer_primary_key_inline() {
        let table = TableInfo {
            name: "t".into(),
            schema: None,
            columns: Some(vec![
                ColumnInfo {
                    name: "id".into(),
                    type_name: "INTEGER".into(),
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    enum_values: None,
                },
                ColumnInfo {
                    name: "name".into(),
                    type_name: "TEXT".into(),
                    nullable: true,
                    is_primary_key: false,
                    default_value: Some("'x'".into()),
                    enum_values: None,
                },
            ]),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::DataGrid,
            child_items: None,
            storage_hints: None,
            pseudo_columns: Box::default(),
        };
        let sql = generate_create_table(&table);
        assert_eq!(
            sql,
            "CREATE TABLE \"t\" (\n    \"id\" INTEGER NOT NULL PRIMARY KEY,\n    \"name\" TEXT DEFAULT 'x'\n);"
        );
    }

    #[test]
    fn upsert_statement_shapes() {
        let assignments = vec![dbflux_core::ColumnAssignment {
            name: "id".into(),
            value: Value::Int(1),
            type_name: None,
        }];
        let sql = TURSO_DIALECT
            .build_upsert_statement(None, "t", &assignments, &["id".to_string()], &[])
            .unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"t\" (\"id\") VALUES (1) ON CONFLICT (\"id\") DO NOTHING"
        );
        assert!(
            TURSO_DIALECT
                .build_upsert_statement(None, "t", &assignments, &[], &[])
                .is_none()
        );
    }

    #[test]
    fn code_generator_emits_sqlite_ddl() {
        let add = TURSO_CODE_GENERATOR
            .generate_add_column(&AddColumnRequest {
                schema_name: None,
                table_name: "t",
                column_name: "c",
                type_name: "TEXT",
                nullable: false,
                default: Some("'a'"),
            })
            .unwrap();
        assert_eq!(
            add,
            vec!["ALTER TABLE \"t\" ADD COLUMN \"c\" TEXT NOT NULL DEFAULT 'a';".to_string()]
        );
        let drop = TURSO_CODE_GENERATOR
            .generate_drop_column(&DropColumnRequest {
                schema_name: None,
                table_name: "t",
                column_name: "c",
            })
            .unwrap();
        assert_eq!(
            drop,
            vec!["ALTER TABLE \"t\" DROP COLUMN \"c\";".to_string()]
        );
    }
}
