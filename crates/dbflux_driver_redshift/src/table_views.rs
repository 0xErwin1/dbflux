//! The Columns view of a Redshift table.
//!
//! `pg_attribute` holds each column's compression encoding, its place in the
//! sort key and whether it is the distribution key; `svv_table_info` holds
//! the table's distribution style, row count and size in 1 MB blocks. None of
//! it reads table rows. Redshift reports no bytes per column without
//! superuser access to `stv_blocklist`, so column sizes stay unknown.

use chrono::{DateTime, Utc};
use dbflux_core::{
    ColumnBadge, ColumnProfile, DbError, ProfileSource, TableFact, TableFactKind, TableProfile,
    TableRef,
};
use postgres::Client;

use crate::error_formatter::format_redshift_query_error;

const CATALOG_SOURCE: &str = "pg_attribute";

/// Bytes in one of the 1 MB blocks `svv_table_info.size` counts.
const BLOCK_BYTES: u64 = 1024 * 1024;

/// One column as `pg_attribute` describes it.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ColumnStorage {
    pub name: String,
    pub type_name: String,
    pub encoding: Option<String>,
    pub distribution_key: bool,
    /// Position in the sort key; negative for an interleaved sort key, zero
    /// when the column is not part of it.
    pub sort_key_position: i32,
}

/// What `svv_table_info` says about the table.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct TableStorage {
    pub distribution_style: Option<String>,
    pub rows: Option<f64>,
    pub size_blocks: Option<i64>,
    pub unsorted_percent: Option<f64>,
}

const COLUMNS_QUERY: &str = r#"
    SELECT a.attname::text,
           pg_catalog.format_type(a.atttypid, a.atttypmod)::text,
           pg_catalog.format_encoding(a.attencodingtype::integer)::text,
           a.attisdistkey,
           a.attsortkeyord::integer
    FROM pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
"#;

/// `svv_table_info` lists only tables with data blocks, so an empty table
/// has no row there.
const TABLE_QUERY: &str = r#"
    SELECT diststyle::text, tbl_rows::float8, size::bigint, unsorted::float8
    FROM svv_table_info
    WHERE "schema" = $1 AND "table" = $2
"#;

pub(crate) fn table_profile(
    client: &mut Client,
    table: &TableRef,
) -> Result<TableProfile, DbError> {
    let schema = table.schema.as_deref().unwrap_or("public");
    let name = table.name.as_str();

    let columns: Vec<ColumnStorage> = client
        .query(COLUMNS_QUERY, &[&schema, &name])
        .map_err(|error| format_redshift_query_error(&error))?
        .iter()
        .map(|row| ColumnStorage {
            name: row.get(0),
            type_name: row.get(1),
            encoding: row.get(2),
            distribution_key: row.get::<_, Option<bool>>(3).unwrap_or(false),
            sort_key_position: row.get::<_, Option<i32>>(4).unwrap_or(0),
        })
        .collect();

    if columns.is_empty() {
        return Err(DbError::ObjectNotFound(
            format!("Relation '{schema}.{name}' not found").into(),
        ));
    }

    let storage = client
        .query_opt(TABLE_QUERY, &[&schema, &name])
        .map_err(|error| format_redshift_query_error(&error))?
        .map(|row| TableStorage {
            distribution_style: row.get(0),
            rows: row.get(1),
            size_blocks: row.get(2),
            unsorted_percent: row.get(3),
        })
        .unwrap_or_default();

    Ok(build_profile(&storage, &columns, Utc::now()))
}

pub(crate) fn build_profile(
    storage: &TableStorage,
    columns: &[ColumnStorage],
    captured_at: DateTime<Utc>,
) -> TableProfile {
    let interleaved = columns.iter().any(|column| column.sort_key_position < 0);

    let mut sort_columns: Vec<&ColumnStorage> = columns
        .iter()
        .filter(|column| column.sort_key_position != 0)
        .collect();
    sort_columns.sort_by_key(|column| column.sort_key_position.unsigned_abs());

    let profiles = columns
        .iter()
        .map(|column| {
            let mut badges = Vec::new();

            if column.sort_key_position != 0 {
                badges.push(ColumnBadge::SortKey {
                    position: u16::try_from(column.sort_key_position.unsigned_abs()).ok(),
                });
            }

            if column.distribution_key {
                badges.push(ColumnBadge::DistributionKey);
            }

            ColumnProfile {
                badges,
                codec: column.encoding.as_deref().map(Into::into),
                ..ColumnProfile::new(column.name.as_str(), column.type_name.as_str())
            }
        })
        .collect();

    let mut facts = vec![TableFact::new(TableFactKind::Engine, "Redshift")];

    if let Some(style) = &storage.distribution_style {
        facts.push(TableFact::new(
            TableFactKind::DistributionStyle,
            style.as_str(),
        ));
    }

    if !sort_columns.is_empty() {
        let names = sort_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let kind = if interleaved {
            "INTERLEAVED"
        } else {
            "COMPOUND"
        };

        facts.push(TableFact::new(
            TableFactKind::SortKey,
            format!("{kind} ({names})"),
        ));
    }

    if let Some(unsorted) = storage.unsorted_percent {
        facts.push(TableFact::new(
            TableFactKind::Unsorted,
            format!("{unsorted:.1}%"),
        ));
    }

    TableProfile {
        row_count: storage
            .rows
            .filter(|rows| *rows >= 0.0)
            .map(|rows| rows.round() as u64),
        row_count_exact: false,
        total_compressed_bytes: storage
            .size_blocks
            .and_then(|blocks| u64::try_from(blocks).ok())
            .map(|blocks| blocks * BLOCK_BYTES),
        engine: Some("Redshift".into()),
        facts,
        captured_at: Some(captured_at),
        ..TableProfile::new(profiles, ProfileSource::Catalog(CATALOG_SOURCE))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, encoding: &str, distribution_key: bool, sort: i32) -> ColumnStorage {
        ColumnStorage {
            name: name.to_string(),
            type_name: "integer".to_string(),
            encoding: Some(encoding.to_string()),
            distribution_key,
            sort_key_position: sort,
        }
    }

    #[test]
    fn a_profile_carries_encodings_keys_and_table_storage() {
        let storage = TableStorage {
            distribution_style: Some("KEY(user_id)".to_string()),
            rows: Some(1_840_000.0),
            size_blocks: Some(96),
            unsorted_percent: Some(3.25),
        };
        let columns = vec![
            column("ts", "az64", false, 2),
            column("site_id", "raw", false, 1),
            column("user_id", "az64", true, 0),
            column("path", "lzo", false, 0),
        ];

        let profile = build_profile(&storage, &columns, Utc::now());

        assert_eq!(profile.source_label, ProfileSource::Catalog("pg_attribute"));
        assert_eq!(profile.row_count, Some(1_840_000));
        assert_eq!(profile.total_compressed_bytes, Some(96 * 1024 * 1024));
        assert_eq!(
            profile.columns[0].badges,
            vec![ColumnBadge::SortKey { position: Some(2) }]
        );
        assert_eq!(
            profile.columns[2].badges,
            vec![ColumnBadge::DistributionKey]
        );
        assert_eq!(profile.columns[3].codec.as_deref(), Some("lzo"));
        assert_eq!(profile.columns[3].compressed_bytes, None);

        assert!(profile.facts.contains(&TableFact::new(
            TableFactKind::DistributionStyle,
            "KEY(user_id)"
        )));
        assert!(profile.facts.contains(&TableFact::new(
            TableFactKind::SortKey,
            "COMPOUND (site_id, ts)"
        )));
        assert!(
            profile
                .facts
                .contains(&TableFact::new(TableFactKind::Unsorted, "3.2%"))
        );
    }

    #[test]
    fn an_interleaved_key_and_an_empty_table_are_reported() {
        let columns = vec![column("a", "raw", false, -1), column("b", "raw", false, -2)];

        let profile = build_profile(&TableStorage::default(), &columns, Utc::now());

        assert_eq!(profile.row_count, None);
        assert_eq!(profile.total_compressed_bytes, None);
        assert_eq!(
            profile.columns[1].badges,
            vec![ColumnBadge::SortKey { position: Some(2) }]
        );
        assert!(profile.facts.contains(&TableFact::new(
            TableFactKind::SortKey,
            "INTERLEAVED (a, b)"
        )));
        assert_eq!(profile.facts.len(), 2, "{:?}", profile.facts);
    }
}
