use std::collections::HashSet;

use dbflux_core::{
    CollectionPresentation, ColumnInfo, ConstraintInfo, ConstraintKind, DatabaseInfo, DbError,
    DbSchemaInfo, ForeignKeyBuilder, ForeignKeyInfo, TableInfo, TableStorageHint, ViewInfo,
};
use postgres::Client;

use crate::error_formatter::format_redshift_query_error;

// SQL text is kept as named constants (rather than inlined in each function)
// so tests can assert on exact query stability, mirroring the golden-SQL
// pattern used for `RedshiftDialect`.

const DATABASES_QUERY: &str = r#"
    SELECT datname
    FROM pg_database
    WHERE datistemplate = false
    ORDER BY datname
"#;

const CURRENT_DATABASE_QUERY: &str = "SELECT current_database()";

const SCHEMAS_QUERY: &str = r#"
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_internal')
    ORDER BY schema_name
"#;

const TABLES_QUERY: &str = r#"
    SELECT table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
      AND table_schema = $1
    ORDER BY table_name
"#;

const VIEWS_QUERY: &str = r#"
    SELECT table_name
    FROM information_schema.views
    WHERE table_schema = $1
    ORDER BY table_name
"#;

const COLUMNS_QUERY: &str = r#"
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_schema = $1
      AND table_name = $2
    ORDER BY ordinal_position
"#;

const PRIMARY_KEY_COLUMNS_QUERY: &str = r#"
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
       AND tc.table_schema = kcu.table_schema
    WHERE tc.table_schema = $1
      AND tc.table_name = $2
      AND tc.constraint_type = 'PRIMARY KEY'
    ORDER BY kcu.ordinal_position
"#;

const FOREIGN_KEYS_QUERY: &str = r#"
    SELECT
        kcu.constraint_name,
        kcu.column_name,
        ccu.table_schema AS referenced_schema,
        ccu.table_name AS referenced_table,
        ccu.column_name AS referenced_column
    FROM information_schema.key_column_usage kcu
    JOIN information_schema.table_constraints tc
        ON kcu.constraint_name = tc.constraint_name
       AND kcu.table_schema = tc.table_schema
    JOIN information_schema.constraint_column_usage ccu
        ON kcu.constraint_name = ccu.constraint_name
       AND kcu.constraint_schema = ccu.constraint_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND kcu.table_schema = $1
      AND kcu.table_name = $2
    ORDER BY kcu.constraint_name, kcu.ordinal_position
"#;

const UNIQUE_CONSTRAINTS_QUERY: &str = r#"
    SELECT
        tc.constraint_name,
        kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
       AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'UNIQUE'
      AND tc.table_schema = $1
      AND tc.table_name = $2
    ORDER BY tc.constraint_name, kcu.ordinal_position
"#;

/// `SVV_TABLE_INFO` is a Redshift system view (no `search_path` dependency)
/// giving a per-table distribution style plus, at best, the *first* sort key
/// column and the total sort key count. It is the primary source for
/// distribution-key detail and a fallback for sort-key detail when
/// `PG_TABLE_DEF` (see [`TABLE_SORT_COLUMNS_QUERY`]) cannot see the table.
const TABLE_STORAGE_INFO_QUERY: &str = r#"
    SELECT diststyle, sortkey1
    FROM svv_table_info
    WHERE "schema" = $1
      AND "table" = $2
"#;

/// `PG_TABLE_DEF` exposes one row per column with a `sortkey` position
/// (negative values denote an interleaved sort key). Redshift only returns
/// rows for schemas on the current `search_path`, so an empty result here is
/// expected/benign, not an error — callers fall back to `sortkey1` from
/// [`TABLE_STORAGE_INFO_QUERY`]. The `sortkey` cast normalizes a
/// smallint/integer ambiguity in the underlying catalog column.
const TABLE_SORT_COLUMNS_QUERY: &str = r#"
    SELECT "column", sortkey::integer AS sortkey
    FROM pg_table_def
    WHERE schemaname = $1
      AND tablename = $2
      AND sortkey <> 0
    ORDER BY abs(sortkey::integer)
"#;

pub(crate) fn get_databases(client: &mut Client) -> Result<Vec<DatabaseInfo>, DbError> {
    let current = get_current_database(client)?;

    let rows = client
        .query(DATABASES_QUERY, &[])
        .map_err(|e| format_redshift_query_error(&e))?;

    Ok(rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let is_current = current.as_ref() == Some(&name);
            DatabaseInfo { name, is_current }
        })
        .collect())
}

pub(crate) fn get_current_database(client: &mut Client) -> Result<Option<String>, DbError> {
    let rows = client
        .query(CURRENT_DATABASE_QUERY, &[])
        .map_err(|e| format_redshift_query_error(&e))?;

    Ok(rows.first().map(|row| row.get(0)))
}

pub(crate) fn get_schemas(client: &mut Client) -> Result<Vec<DbSchemaInfo>, DbError> {
    let schema_rows = client
        .query(SCHEMAS_QUERY, &[])
        .map_err(|e| format_redshift_query_error(&e))?;

    let mut schemas = Vec::new();

    for row in schema_rows {
        let schema_name: String = row.get(0);

        let tables = get_tables_for_schema(client, &schema_name)?;
        let views = get_views_for_schema(client, &schema_name)?;

        schemas.push(DbSchemaInfo {
            name: schema_name,
            tables,
            views,
            custom_types: None,
        });
    }

    Ok(schemas)
}

fn get_tables_for_schema(client: &mut Client, schema: &str) -> Result<Vec<TableInfo>, DbError> {
    let rows = client
        .query(TABLES_QUERY, &[&schema])
        .map_err(|e| format_redshift_query_error(&e))?;

    Ok(rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            TableInfo {
                name,
                schema: Some(schema.to_string()),
                columns: None,
                indexes: None,
                foreign_keys: None,
                constraints: None,
                sample_fields: None,
                presentation: CollectionPresentation::DataGrid,
                child_items: None,
                storage_hints: None,
            }
        })
        .collect())
}

fn get_views_for_schema(client: &mut Client, schema: &str) -> Result<Vec<ViewInfo>, DbError> {
    let rows = client
        .query(VIEWS_QUERY, &[&schema])
        .map_err(|e| format_redshift_query_error(&e))?;

    Ok(rows
        .iter()
        .map(|row| ViewInfo {
            name: row.get(0),
            schema: Some(schema.to_string()),
        })
        .collect())
}

fn get_primary_key_columns(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> Result<HashSet<String>, DbError> {
    let rows = client
        .query(PRIMARY_KEY_COLUMNS_QUERY, &[&schema, &table])
        .map_err(|e| format_redshift_query_error(&e))?;

    Ok(rows.iter().map(|row| row.get(0)).collect())
}

fn get_columns(client: &mut Client, schema: &str, table: &str) -> Result<Vec<ColumnInfo>, DbError> {
    let rows = client
        .query(COLUMNS_QUERY, &[&schema, &table])
        .map_err(|e| format_redshift_query_error(&e))?;

    let primary_key_columns = get_primary_key_columns(client, schema, table)?;

    Ok(rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let type_name: String = row.get(1);
            let is_nullable: String = row.get(2);
            let default_value: Option<String> = row.get(3);
            let is_primary_key = primary_key_columns.contains(&name);

            ColumnInfo {
                name,
                type_name,
                nullable: is_nullable == "YES",
                is_primary_key,
                default_value,
                enum_values: None,
            }
        })
        .collect())
}

fn get_foreign_keys(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> Result<Vec<ForeignKeyInfo>, DbError> {
    let rows = client
        .query(FOREIGN_KEYS_QUERY, &[&schema, &table])
        .map_err(|e| format_redshift_query_error(&e))?;

    let mut builder = ForeignKeyBuilder::new();

    for row in &rows {
        let name: String = row.get(0);
        let column: String = row.get(1);
        let referenced_schema: Option<String> = row.get(2);
        let referenced_table: String = row.get(3);
        let referenced_column: String = row.get(4);

        // Redshift's informational foreign keys carry no ON UPDATE/DELETE
        // action semantics (they are never enforced), so both stay `None`.
        builder.add_column(
            name,
            column,
            referenced_schema,
            referenced_table,
            referenced_column,
            None,
            None,
        );
    }

    Ok(builder.build_sorted())
}

fn get_unique_constraints(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> Result<Vec<ConstraintInfo>, DbError> {
    let rows = client
        .query(UNIQUE_CONSTRAINTS_QUERY, &[&schema, &table])
        .map_err(|e| format_redshift_query_error(&e))?;

    let mut grouped: Vec<ConstraintInfo> = Vec::new();

    for row in &rows {
        let name: String = row.get(0);
        let column: String = row.get(1);

        match grouped.iter_mut().find(|c| c.name == name) {
            Some(constraint) => constraint.columns.push(column),
            None => grouped.push(ConstraintInfo {
                name,
                kind: ConstraintKind::Unique,
                columns: vec![column],
                check_clause: None,
            }),
        }
    }

    Ok(grouped)
}

/// A single column of a Redshift sort key, as reported by `PG_TABLE_DEF`.
///
/// `position` mirrors the catalog's signed `sortkey` value: its absolute
/// value gives ordering within the key, and a negative sign marks an
/// interleaved (rather than compound) sort key.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SortKeyColumn {
    pub name: String,
    pub position: i32,
}

/// Builds this table's `TableStorageHint`s from already-decoded catalog
/// values.
///
/// Pure and independently testable: takes plain Rust values, never a live
/// `Client`/`Row`, so hint-construction logic can be verified without a
/// cluster. `sort_columns` (from `PG_TABLE_DEF`) is preferred when present;
/// `fallback_sortkey1` (from `SVV_TABLE_INFO`) covers the case where the
/// table's schema isn't on the connection's `search_path` and `PG_TABLE_DEF`
/// therefore returns nothing.
pub(crate) fn build_storage_hints(
    diststyle: Option<&str>,
    sort_columns: &[SortKeyColumn],
    fallback_sortkey1: Option<&str>,
    has_advisory_constraints: bool,
) -> Vec<TableStorageHint> {
    let mut hints = Vec::new();

    if let Some(style) = diststyle {
        let (detail, columns) = parse_diststyle(style);
        hints.push(TableStorageHint {
            label: "Distribution Key".to_string(),
            columns,
            detail: Some(detail),
        });
    }

    let mut ordered_columns = sort_columns.to_vec();
    ordered_columns.sort_by_key(|column| column.position.abs());

    let (sort_key_names, interleaved): (Vec<String>, bool) = if !ordered_columns.is_empty() {
        let interleaved = ordered_columns.iter().any(|column| column.position < 0);
        let names = ordered_columns
            .into_iter()
            .map(|column| column.name)
            .collect();
        (names, interleaved)
    } else {
        let names = fallback_sortkey1
            .filter(|name| !name.is_empty())
            .map(|name| vec![name.to_string()])
            .unwrap_or_default();
        (names, false)
    };

    if !sort_key_names.is_empty() {
        let detail = if interleaved {
            "interleaved"
        } else {
            "compound"
        };
        hints.push(TableStorageHint {
            label: "Sort Key".to_string(),
            columns: sort_key_names,
            detail: Some(detail.to_string()),
        });
    }

    if has_advisory_constraints {
        hints.push(TableStorageHint {
            label: "Constraints advisory".to_string(),
            columns: Vec::new(),
            detail: Some("PK/FK/UNIQUE are informational, not enforced".to_string()),
        });
    }

    hints
}

/// Parses `SVV_TABLE_INFO.diststyle` text into a short detail label plus,
/// for `"KEY(col)"`, the distribution column name.
///
/// Observed forms (per AWS documentation): `"EVEN"`, `"ALL"`,
/// `"KEY(column_name)"`, and `"AUTO(...)"`.
fn parse_diststyle(style: &str) -> (String, Vec<String>) {
    if let Some(inner) = style
        .strip_prefix("KEY(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        return ("KEY".to_string(), vec![inner.to_string()]);
    }

    if style.starts_with("AUTO") {
        return ("AUTO".to_string(), Vec::new());
    }

    (style.to_string(), Vec::new())
}

fn get_table_storage_hints(
    client: &mut Client,
    schema: &str,
    table: &str,
    has_advisory_constraints: bool,
) -> Result<Vec<TableStorageHint>, DbError> {
    let storage_rows = client
        .query(TABLE_STORAGE_INFO_QUERY, &[&schema, &table])
        .map_err(|e| format_redshift_query_error(&e))?;

    let (diststyle, sortkey1): (Option<String>, Option<String>) = storage_rows
        .first()
        .map(|row| (row.get(0), row.get(1)))
        .unwrap_or((None, None));

    let sort_column_rows = client
        .query(TABLE_SORT_COLUMNS_QUERY, &[&schema, &table])
        .map_err(|e| format_redshift_query_error(&e))?;

    let sort_columns: Vec<SortKeyColumn> = sort_column_rows
        .iter()
        .map(|row| SortKeyColumn {
            name: row.get(0),
            position: row.get(1),
        })
        .collect();

    Ok(build_storage_hints(
        diststyle.as_deref(),
        &sort_columns,
        sortkey1.as_deref(),
        has_advisory_constraints,
    ))
}

/// Reduces a storage-hints fetch to best-effort.
///
/// Storage hints (distribution/sort keys, advisory-constraint notes) are a
/// cosmetic enhancement layered on top of the columns/foreign-keys/constraints
/// that already succeeded. A failure here — e.g. `SVV_TABLE_INFO`/`PG_TABLE_DEF`
/// being unreadable for this role — must not discard those core details, so the
/// error is logged once and downgraded to `None` rather than propagated.
fn storage_hints_best_effort(
    result: Result<Vec<TableStorageHint>, DbError>,
    schema: &str,
    table: &str,
) -> Option<Vec<TableStorageHint>> {
    match result {
        Ok(hints) => Some(hints),
        Err(error) => {
            log::warn!("Redshift storage hints unavailable for {schema}.{table}: {error}");
            None
        }
    }
}

/// Fetches full table details (columns, foreign keys, unique constraints,
/// and distribution/sort-key storage hints).
///
/// No `IndexData` is populated: Redshift has no true indexes, and fabricating
/// one from the (non-enforced) primary key would misrepresent it as a real
/// index structure.
///
/// Storage hints are best-effort: if they fail to load the rest of the details
/// are still returned (see [`storage_hints_best_effort`]).
pub(crate) fn get_table_details(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> Result<TableInfo, DbError> {
    let columns = get_columns(client, schema, table)?;
    let foreign_keys = get_foreign_keys(client, schema, table)?;
    let constraints = get_unique_constraints(client, schema, table)?;

    let has_advisory_constraints = columns.iter().any(|column| column.is_primary_key)
        || !foreign_keys.is_empty()
        || !constraints.is_empty();

    let storage_hints = storage_hints_best_effort(
        get_table_storage_hints(client, schema, table, has_advisory_constraints),
        schema,
        table,
    );

    Ok(TableInfo {
        name: table.to_string(),
        schema: Some(schema.to_string()),
        columns: Some(columns),
        indexes: None,
        foreign_keys: Some(foreign_keys),
        constraints: Some(constraints),
        sample_fields: None,
        presentation: CollectionPresentation::DataGrid,
        child_items: None,
        storage_hints,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        CURRENT_DATABASE_QUERY, DATABASES_QUERY, FOREIGN_KEYS_QUERY, PRIMARY_KEY_COLUMNS_QUERY,
        SCHEMAS_QUERY, SortKeyColumn, TABLE_SORT_COLUMNS_QUERY, TABLE_STORAGE_INFO_QUERY,
        TABLES_QUERY, UNIQUE_CONSTRAINTS_QUERY, VIEWS_QUERY, build_storage_hints, parse_diststyle,
        storage_hints_best_effort,
    };
    use dbflux_core::{DbError, TableStorageHint};

    #[test]
    fn schema_and_table_listing_queries_are_stable() {
        assert!(DATABASES_QUERY.contains("FROM pg_database"));
        assert!(DATABASES_QUERY.contains("datistemplate = false"));

        assert_eq!(CURRENT_DATABASE_QUERY, "SELECT current_database()");

        assert!(SCHEMAS_QUERY.contains("FROM information_schema.schemata"));
        assert!(SCHEMAS_QUERY.contains("'pg_catalog'"));
        assert!(SCHEMAS_QUERY.contains("'information_schema'"));
        assert!(SCHEMAS_QUERY.contains("'pg_internal'"));

        assert!(TABLES_QUERY.contains("FROM information_schema.tables"));
        assert!(TABLES_QUERY.contains("table_type = 'BASE TABLE'"));
        assert!(TABLES_QUERY.contains("table_schema = $1"));

        assert!(VIEWS_QUERY.contains("FROM information_schema.views"));
        assert!(VIEWS_QUERY.contains("table_schema = $1"));
    }

    #[test]
    fn constraint_and_key_queries_are_stable() {
        assert!(PRIMARY_KEY_COLUMNS_QUERY.contains("constraint_type = 'PRIMARY KEY'"));
        assert!(FOREIGN_KEYS_QUERY.contains("constraint_type = 'FOREIGN KEY'"));
        assert!(UNIQUE_CONSTRAINTS_QUERY.contains("constraint_type = 'UNIQUE'"));
    }

    #[test]
    fn storage_hint_queries_are_stable() {
        assert!(TABLE_STORAGE_INFO_QUERY.contains("FROM svv_table_info"));
        assert!(TABLE_STORAGE_INFO_QUERY.contains("diststyle"));
        assert!(TABLE_STORAGE_INFO_QUERY.contains("sortkey1"));

        assert!(TABLE_SORT_COLUMNS_QUERY.contains("FROM pg_table_def"));
        assert!(TABLE_SORT_COLUMNS_QUERY.contains("sortkey <> 0"));
    }

    #[test]
    fn parse_diststyle_extracts_key_column_name() {
        assert_eq!(
            parse_diststyle("KEY(customer_id)"),
            ("KEY".to_string(), vec!["customer_id".to_string()])
        );
    }

    #[test]
    fn parse_diststyle_handles_even_all_and_auto() {
        assert_eq!(parse_diststyle("EVEN"), ("EVEN".to_string(), Vec::new()));
        assert_eq!(parse_diststyle("ALL"), ("ALL".to_string(), Vec::new()));
        assert_eq!(
            parse_diststyle("AUTO(ALL)"),
            ("AUTO".to_string(), Vec::new())
        );
    }

    #[test]
    fn build_storage_hints_maps_distkey_sortkey_and_advisory_constraints() {
        let hints = build_storage_hints(
            Some("KEY(customer_id)"),
            &[
                SortKeyColumn {
                    name: "order_date".to_string(),
                    position: 1,
                },
                SortKeyColumn {
                    name: "region".to_string(),
                    position: 2,
                },
            ],
            None,
            true,
        );

        assert_eq!(hints.len(), 3);

        assert_eq!(hints[0].label, "Distribution Key");
        assert_eq!(hints[0].columns, vec!["customer_id".to_string()]);
        assert_eq!(hints[0].detail.as_deref(), Some("KEY"));

        assert_eq!(hints[1].label, "Sort Key");
        assert_eq!(
            hints[1].columns,
            vec!["order_date".to_string(), "region".to_string()]
        );
        assert_eq!(hints[1].detail.as_deref(), Some("compound"));

        assert_eq!(hints[2].label, "Constraints advisory");
        assert!(hints[2].columns.is_empty());
        assert_eq!(
            hints[2].detail.as_deref(),
            Some("PK/FK/UNIQUE are informational, not enforced")
        );
    }

    #[test]
    fn build_storage_hints_detects_interleaved_sort_style_from_negative_position() {
        let hints = build_storage_hints(
            None,
            &[SortKeyColumn {
                name: "id".to_string(),
                position: -1,
            }],
            None,
            false,
        );

        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].label, "Sort Key");
        assert_eq!(hints[0].detail.as_deref(), Some("interleaved"));
    }

    #[test]
    fn build_storage_hints_falls_back_to_sortkey1_when_pg_table_def_is_empty() {
        let hints = build_storage_hints(Some("EVEN"), &[], Some("created_at"), false);

        assert_eq!(hints.len(), 2);
        assert_eq!(hints[0].label, "Distribution Key");
        assert_eq!(hints[0].detail.as_deref(), Some("EVEN"));
        assert!(hints[0].columns.is_empty());

        assert_eq!(hints[1].label, "Sort Key");
        assert_eq!(hints[1].columns, vec!["created_at".to_string()]);
        assert_eq!(hints[1].detail.as_deref(), Some("compound"));
    }

    #[test]
    fn build_storage_hints_returns_empty_when_table_has_no_storage_metadata() {
        let hints = build_storage_hints(None, &[], None, false);
        assert!(hints.is_empty());
    }

    #[test]
    fn storage_hints_best_effort_downgrades_error_to_none() {
        let errored: Result<Vec<TableStorageHint>, DbError> = Err(DbError::QueryFailed(
            "svv_table_info unreadable".to_string().into(),
        ));

        assert!(storage_hints_best_effort(errored, "public", "orders").is_none());
    }

    #[test]
    fn storage_hints_best_effort_passes_through_ok_hints() {
        let ok: Result<Vec<TableStorageHint>, DbError> = Ok(vec![TableStorageHint {
            label: "Sort Key".to_string(),
            columns: vec!["created_at".to_string()],
            detail: Some("compound".to_string()),
        }]);

        let hints = storage_hints_best_effort(ok, "public", "orders").unwrap_or_default();

        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].label, "Sort Key");
    }
}
