//! Columns, Partitions and read estimates of a ClickHouse table.
//!
//! The profile and the partitions come from `system.columns`,
//! `system.tables` and `system.parts`, which hold sizes, codecs and keys
//! without reading the table. The read estimate runs `EXPLAIN ESTIMATE`,
//! which asks the primary index how many parts and granules a query touches
//! without running it. Only the sample profile reads rows, from a subquery
//! whose `LIMIT` caps what it reads.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dbflux_core::{
    ColumnBadge, ColumnProfile, DbError, EstimateScope, PartUnit, ProfileSource, ReadEstimate,
    SqlDialect, TableBrowseRequest, TableFact, TableFactKind, TablePartition, TablePartitions,
    TableProfile, TableRef, Value, ValueRange,
};

use crate::connection::ClickHouseConnection;
use crate::dialect::CLICKHOUSE_DIALECT;

/// What a profile names as its source.
const COLUMNS_SOURCE: &str = "system.columns";
const PARTS_SOURCE: &str = "system.parts";

/// The database and table name of `table`, with the active database for a
/// table that names none.
fn database_and_name(
    connection: &ClickHouseConnection,
    table: &TableRef,
) -> Result<(String, String), DbError> {
    let database = match &table.schema {
        Some(schema) => schema.clone(),
        None => connection.current_database()?,
    };

    Ok((database, table.name.clone()))
}

fn literal(text: &str) -> String {
    CLICKHOUSE_DIALECT.value_to_literal(&Value::Text(text.to_string()))
}

pub(crate) fn table_profile(
    connection: &ClickHouseConnection,
    table: &TableRef,
) -> Result<TableProfile, DbError> {
    let (database, name) = database_and_name(connection, table)?;
    let filter = format!(
        "database = {} AND table = {}",
        literal(&database),
        literal(&name)
    );

    let columns_sql = format!(
        "SELECT name, type, compression_codec, data_compressed_bytes, data_uncompressed_bytes, \
         is_in_sorting_key, is_in_partition_key \
         FROM system.columns WHERE {filter} ORDER BY position"
    );
    let table_sql = format!(
        "SELECT engine, partition_key, sorting_key, primary_key, sampling_key, total_rows, \
         total_bytes, engine_full \
         FROM system.tables WHERE database = {} AND name = {} LIMIT 1",
        literal(&database),
        literal(&name)
    );
    let parts_sql = format!(
        "SELECT count(), sum(data_compressed_bytes), sum(data_uncompressed_bytes) \
         FROM system.parts WHERE {filter} AND active"
    );

    let column_rows = connection
        .execute_sql(&columns_sql, Some("system"), None, None, None)?
        .rows;
    let table_rows = connection
        .execute_sql(&table_sql, Some("system"), None, Some(1), None)?
        .rows;

    let Some(table_row) = table_rows.first() else {
        return Err(DbError::ObjectNotFound(
            format!("ClickHouse table {database}.{name} was not found").into(),
        ));
    };

    let parts_rows = connection
        .execute_sql(&parts_sql, Some("system"), None, Some(1), None)?
        .rows;
    let parts_row = parts_rows.first();

    Ok(build_profile(
        table_row,
        &column_rows,
        parts_row.map(Vec::as_slice),
        Utc::now(),
    ))
}

/// The profile from one `system.tables` row, the `system.columns` rows in
/// position order and the active-part totals of `system.parts`.
pub(crate) fn build_profile(
    table_row: &[Value],
    column_rows: &[Vec<Value>],
    parts_row: Option<&[Value]>,
    captured_at: DateTime<Utc>,
) -> TableProfile {
    let engine = text_at(table_row, 0).map(str::to_string);
    let partition_key = text_at(table_row, 1).filter(|key| !key.is_empty());
    let sorting_key = text_at(table_row, 2).filter(|key| !key.is_empty());
    let primary_key = text_at(table_row, 3).filter(|key| !key.is_empty());
    let sampling_key = text_at(table_row, 4).filter(|key| !key.is_empty());
    let total_rows = u64_at(table_row, 5);
    let total_bytes = u64_at(table_row, 6);
    let ttl = text_at(table_row, 7).and_then(ttl_clause);

    let sort_items = sorting_key.map(split_key).unwrap_or_default();

    let columns: Vec<ColumnProfile> = column_rows
        .iter()
        .filter_map(|row| {
            let name = text_at(row, 0)?;
            let type_name = text_at(row, 1)?;
            let mut badges = Vec::new();

            if flag_at(row, 5) {
                badges.push(ColumnBadge::SortKey {
                    position: key_position(&sort_items, name),
                });
            }

            if flag_at(row, 6) {
                badges.push(ColumnBadge::PartitionKey);
            }

            let codec = text_at(row, 2)
                .filter(|codec| !codec.is_empty())
                .map(strip_codec_wrapper)
                .unwrap_or_else(|| "default".to_string());

            Some(ColumnProfile {
                badges,
                codec: Some(codec.into()),
                compressed_bytes: u64_at(row, 3),
                uncompressed_bytes: u64_at(row, 4),
                ..ColumnProfile::new(name, type_name)
            })
        })
        .collect();

    let active_parts = parts_row.and_then(|row| u64_at(row, 0));
    let parts_compressed = parts_row
        .and_then(|row| u64_at(row, 1))
        .filter(|_| active_parts.unwrap_or(0) > 0);
    let parts_uncompressed = parts_row
        .and_then(|row| u64_at(row, 2))
        .filter(|_| active_parts.unwrap_or(0) > 0);

    let mut facts = Vec::new();

    if let Some(engine) = &engine {
        facts.push(TableFact::new(TableFactKind::Engine, engine.as_str()));
    }

    if let Some(key) = sorting_key {
        facts.push(TableFact::new(TableFactKind::SortKey, key));
    }

    if let Some(key) = primary_key.filter(|key| Some(*key) != sorting_key) {
        facts.push(TableFact::new(TableFactKind::PrimaryKey, key));
    }

    if let Some(key) = partition_key {
        facts.push(TableFact::new(TableFactKind::PartitionKey, key));
    }

    if let Some(key) = sampling_key {
        facts.push(TableFact::new(TableFactKind::SamplingKey, key));
    }

    if let Some(ttl) = ttl {
        facts.push(TableFact::new(TableFactKind::Ttl, ttl));
    }

    if let Some(parts) = active_parts.filter(|parts| *parts > 0) {
        facts.push(TableFact::new(
            TableFactKind::ActiveParts,
            parts.to_string(),
        ));
    }

    let column_compressed: Option<u64> = columns.iter().map(|column| column.compressed_bytes).sum();

    TableProfile {
        row_count: total_rows,
        row_count_exact: true,
        total_compressed_bytes: parts_compressed.or(total_bytes).or(column_compressed),
        total_uncompressed_bytes: parts_uncompressed
            .or_else(|| columns.iter().map(|column| column.uncompressed_bytes).sum()),
        engine: engine.map(Into::into),
        facts,
        captured_at: Some(captured_at),
        ..TableProfile::new(columns, ProfileSource::Catalog(COLUMNS_SOURCE))
    }
}

pub(crate) fn table_partitions(
    connection: &ClickHouseConnection,
    table: &TableRef,
) -> Result<TablePartitions, DbError> {
    let (database, name) = database_and_name(connection, table)?;

    let key_sql = format!(
        "SELECT partition_key FROM system.tables WHERE database = {} AND name = {} LIMIT 1",
        literal(&database),
        literal(&name)
    );
    let partitions_sql = format!(
        "SELECT partition, count(), sum(rows), sum(data_compressed_bytes), \
         sum(data_uncompressed_bytes), toString(min(min_time)), toString(max(max_time)), \
         toString(min(min_date)), toString(max(max_date)), toUnixTimestamp(max(modification_time)) \
         FROM system.parts WHERE database = {} AND table = {} AND active \
         GROUP BY partition ORDER BY partition DESC",
        literal(&database),
        literal(&name)
    );

    let key = connection
        .execute_sql(&key_sql, Some("system"), None, Some(1), None)?
        .rows
        .first()
        .and_then(|row| text_at(row, 0))
        .filter(|key| !key.is_empty())
        .map(Arc::<str>::from);

    let rows = connection
        .execute_sql(&partitions_sql, Some("system"), None, None, None)?
        .rows;

    Ok(TablePartitions {
        key,
        partitions: rows
            .iter()
            .filter_map(|row| partition_from_row(row))
            .collect(),
        source: ProfileSource::Catalog(PARTS_SOURCE),
        captured_at: Some(Utc::now()),
    })
}

/// One partition from a grouped `system.parts` row. The key range comes from
/// the part times when the partition key is a time, from the part dates when
/// it is a date, and is unknown otherwise (ClickHouse stores zero then).
pub(crate) fn partition_from_row(row: &[Value]) -> Option<TablePartition> {
    let name = text_at(row, 0)?;

    let range = range_unless_zero(text_at(row, 5), text_at(row, 6), "1970-01-01 00:00:00")
        .or_else(|| range_unless_zero(text_at(row, 7), text_at(row, 8), "1970-01-01"));

    let modified_at = u64_at(row, 9)
        .filter(|seconds| *seconds > 0)
        .and_then(|seconds| DateTime::from_timestamp(seconds as i64, 0));

    Some(TablePartition {
        parts: u64_at(row, 1),
        rows: u64_at(row, 2),
        compressed_bytes: u64_at(row, 3),
        uncompressed_bytes: u64_at(row, 4),
        range,
        modified_at,
        ..TablePartition::new(name)
    })
}

fn range_unless_zero(min: Option<&str>, max: Option<&str>, zero: &str) -> Option<ValueRange> {
    let (min, max) = (min?, max?);

    if min == zero && max == zero {
        return None;
    }

    Some(ValueRange {
        min: min.into(),
        max: max.into(),
        exact: true,
    })
}

pub(crate) fn estimate_table_read(
    connection: &ClickHouseConnection,
    request: &TableBrowseRequest,
    profile: &TableProfile,
) -> Result<ReadEstimate, DbError> {
    let (database, name) = database_and_name(connection, &request.table)?;

    let explain_sql = format!(
        "EXPLAIN ESTIMATE {}",
        request.build_sql_with(&CLICKHOUSE_DIALECT)
    );
    let totals_sql = format!(
        "SELECT count(), sum(marks) FROM system.parts \
         WHERE database = {} AND table = {} AND active",
        literal(&database),
        literal(&name)
    );

    let explain_rows = connection
        .execute_sql_read_only(&explain_sql, Some(&database), None, None, None)?
        .rows;
    let totals_rows = connection
        .execute_sql(&totals_sql, Some("system"), None, Some(1), None)?
        .rows;

    let estimate_row = explain_rows
        .iter()
        .find(|row| {
            text_at(row, 0) == Some(database.as_str()) && text_at(row, 1) == Some(name.as_str())
        })
        .or_else(|| explain_rows.first());

    let touched_parts = estimate_row.and_then(|row| u64_at(row, 2)).unwrap_or(0);
    let rows = estimate_row.and_then(|row| u64_at(row, 3));
    let touched_marks = estimate_row.and_then(|row| u64_at(row, 4)).unwrap_or(0);

    let totals = totals_rows.first();
    let total_parts = totals.and_then(|row| u64_at(row, 0)).unwrap_or(0);
    let total_marks = totals.and_then(|row| u64_at(row, 1)).unwrap_or(0);

    Ok(build_estimate(
        profile,
        &request.columns,
        (touched_parts, total_parts),
        (touched_marks, total_marks),
        rows,
    ))
}

/// Scales each read column's bytes on disk by the share of granules the read
/// touches. The bytes are an approximation: granules differ in size.
pub(crate) fn build_estimate(
    profile: &TableProfile,
    projected: &[String],
    parts: (u64, u64),
    marks: (u64, u64),
    rows: Option<u64>,
) -> ReadEstimate {
    let share = if marks.1 == 0 {
        0.0
    } else {
        (marks.0 as f64 / marks.1 as f64).min(1.0)
    };

    let per_column: Vec<(usize, u64)> = profile
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            projected.is_empty() || projected.iter().any(|name| name == column.name.as_ref())
        })
        .filter_map(|(index, column)| {
            let bytes = column.compressed_bytes?;
            Some((index, (bytes as f64 * share).round() as u64))
        })
        .collect();

    ReadEstimate {
        total_bytes: per_column.iter().map(|(_, bytes)| bytes).sum(),
        per_column,
        scopes: vec![
            EstimateScope::Parts {
                touched: parts.0 as usize,
                total: parts.1 as usize,
                unit: PartUnit::Parts,
            },
            EstimateScope::Parts {
                touched: marks.0 as usize,
                total: marks.1 as usize,
                unit: PartUnit::Granules,
            },
        ],
        rows,
        exact: false,
    }
}

pub(crate) fn sample_table_profile(
    connection: &ClickHouseConnection,
    table: &TableRef,
    columns: &[String],
    types: &HashMap<String, String>,
    row_limit: u64,
) -> Result<TableProfile, DbError> {
    let (database, _) = database_and_name(connection, table)?;
    let sql = sample_sql(table, columns, types, row_limit);

    let rows = connection
        .execute_sql_read_only(&sql, Some(&database), None, None, None)?
        .rows;

    let row = rows.first().ok_or_else(|| {
        DbError::QueryFailed("The sample query returned no rows".to_string().into())
    })?;

    Ok(sample_from_row(row, columns, types, Utc::now()))
}

/// The one-row query a sample profile runs: the rows read, then for each
/// column its nulls, an approximate distinct count and, for comparable
/// types, its smallest and largest value as text. The `LIMIT` inside the
/// subquery caps the rows read.
pub(crate) fn sample_sql(
    table: &TableRef,
    columns: &[String],
    types: &HashMap<String, String>,
    row_limit: u64,
) -> String {
    let mut aggregates = vec!["count()".to_string()];

    for column in columns {
        let quoted = CLICKHOUSE_DIALECT.quote_identifier(column);
        aggregates.push(format!("countIf(isNull({quoted}))"));
        aggregates.push(format!("uniq({quoted})"));

        if is_comparable(types.get(column).map(String::as_str).unwrap_or("")) {
            aggregates.push(format!("toString(min({quoted}))"));
            aggregates.push(format!("toString(max({quoted}))"));
        }
    }

    let projection = if columns.is_empty() {
        "*".to_string()
    } else {
        columns
            .iter()
            .map(|column| CLICKHOUSE_DIALECT.quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ")
    };

    format!(
        "SELECT {} FROM (SELECT {projection} FROM {} LIMIT {row_limit})",
        aggregates.join(", "),
        table.quoted_with(&CLICKHOUSE_DIALECT)
    )
}

pub(crate) fn sample_from_row(
    row: &[Value],
    columns: &[String],
    types: &HashMap<String, String>,
    captured_at: DateTime<Utc>,
) -> TableProfile {
    let rows_read = u64_at(row, 0).unwrap_or(0);
    let mut index = 1;
    let mut profiles = Vec::new();

    for column in columns {
        let type_name = types.get(column).map(String::as_str).unwrap_or("");
        let null_count = u64_at(row, index);
        let distinct_count = u64_at(row, index + 1);
        index += 2;

        let range = if is_comparable(type_name) {
            let range = match (text_at(row, index), text_at(row, index + 1)) {
                (Some(min), Some(max)) if rows_read > 0 => Some(ValueRange {
                    min: min.into(),
                    max: max.into(),
                    exact: true,
                }),
                _ => None,
            };
            index += 2;
            range
        } else {
            None
        };

        profiles.push(ColumnProfile {
            null_count,
            // An approximate count can overshoot; a sample never holds more
            // distinct values than rows.
            distinct_count: distinct_count.map(|count| count.min(rows_read)),
            distinct_exact: false,
            range,
            ..ColumnProfile::new(column.as_str(), type_name)
        });
    }

    TableProfile {
        row_count: Some(rows_read),
        captured_at: Some(captured_at),
        ..TableProfile::new(profiles, ProfileSource::Sample { rows: rows_read })
    }
}

/// Whether `min` and `max` work on a column of `type_name` and print as one
/// readable value: numbers, strings, dates, enums and UUIDs, possibly
/// nullable or low-cardinality, but no arrays, maps, tuples, JSON or
/// aggregate states.
pub(crate) fn is_comparable(type_name: &str) -> bool {
    let mut inner = type_name.trim();

    loop {
        let unwrapped = ["Nullable(", "LowCardinality("]
            .iter()
            .find_map(|wrapper| inner.strip_prefix(wrapper))
            .and_then(|rest| rest.strip_suffix(')'));

        match unwrapped {
            Some(rest) => inner = rest.trim(),
            None => break,
        }
    }

    let base = inner.split('(').next().unwrap_or("");

    matches!(
        base,
        "UInt8"
            | "UInt16"
            | "UInt32"
            | "UInt64"
            | "UInt128"
            | "UInt256"
            | "Int8"
            | "Int16"
            | "Int32"
            | "Int64"
            | "Int128"
            | "Int256"
            | "Float32"
            | "Float64"
            | "Decimal"
            | "Decimal32"
            | "Decimal64"
            | "Decimal128"
            | "Decimal256"
            | "String"
            | "FixedString"
            | "Date"
            | "Date32"
            | "DateTime"
            | "DateTime64"
            | "Enum8"
            | "Enum16"
            | "UUID"
            | "IPv4"
            | "IPv6"
            | "Bool"
    )
}

/// The top-level items of a key expression: `(site_id, toDate(ts), user_id)`
/// gives `site_id`, `toDate(ts)` and `user_id`.
pub(crate) fn split_key(key: &str) -> Vec<String> {
    let trimmed = key.trim();
    let inner = trimmed
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
        .filter(|rest| balanced(rest))
        .unwrap_or(trimmed);

    let mut items = Vec::new();
    let mut depth = 0usize;
    let mut current = String::new();

    for character in inner.chars() {
        match character {
            '(' => {
                depth += 1;
                current.push(character);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(character);
            }
            ',' if depth == 0 => {
                items.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(character),
        }
    }

    if !current.trim().is_empty() {
        items.push(current.trim().to_string());
    }

    items
}

fn balanced(text: &str) -> bool {
    let mut depth = 0i32;

    for character in text.chars() {
        match character {
            '(' => depth += 1,
            ')' => {
                depth -= 1;

                if depth < 0 {
                    return false;
                }
            }
            _ => {}
        }
    }

    depth == 0
}

/// The one-based position of the first key item that names `column` as a
/// whole identifier, either alone or inside an expression.
pub(crate) fn key_position(items: &[String], column: &str) -> Option<u16> {
    items
        .iter()
        .position(|item| names_identifier(item, column))
        .and_then(|index| u16::try_from(index + 1).ok())
}

fn names_identifier(expression: &str, identifier: &str) -> bool {
    let unquoted = expression.replace('`', "");
    let bytes = unquoted.as_bytes();
    let is_identifier_byte = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';

    unquoted.match_indices(identifier).any(|(start, matched)| {
        let end = start + matched.len();
        let before = start
            .checked_sub(1)
            .and_then(|index| bytes.get(index))
            .is_none_or(|byte| !is_identifier_byte(*byte));
        let after = bytes.get(end).is_none_or(|byte| !is_identifier_byte(*byte));
        before && after
    })
}

/// `CODEC(Delta(4), ZSTD(1))` as `Delta(4), ZSTD(1)`.
fn strip_codec_wrapper(codec: &str) -> String {
    codec
        .trim()
        .strip_prefix("CODEC(")
        .and_then(|rest| rest.strip_suffix(')'))
        .unwrap_or(codec)
        .to_string()
}

/// The TTL clause of an `engine_full` text, without the keyword and without
/// the `SETTINGS` that follow it.
pub(crate) fn ttl_clause(engine_full: &str) -> Option<String> {
    let start = engine_full.find(" TTL ")? + " TTL ".len();
    let rest = &engine_full[start..];
    let end = rest.find(" SETTINGS ").unwrap_or(rest.len());
    let ttl = rest[..end].trim();

    (!ttl.is_empty()).then(|| ttl.to_string())
}

fn text_at(row: &[Value], index: usize) -> Option<&str> {
    match row.get(index) {
        Some(Value::Text(value) | Value::Json(value) | Value::Decimal(value)) => Some(value),
        _ => None,
    }
}

/// An unsigned count however the HTTP format sent it: a number, or a string
/// for the 64-bit integers ClickHouse quotes.
fn u64_at(row: &[Value], index: usize) -> Option<u64> {
    match row.get(index) {
        Some(Value::Int(value)) => u64::try_from(*value).ok(),
        Some(Value::Float(value)) if *value >= 0.0 => Some(*value as u64),
        Some(Value::Text(value) | Value::Decimal(value)) => value.trim().parse().ok(),
        _ => None,
    }
}

fn flag_at(row: &[Value], index: usize) -> bool {
    u64_at(row, index).is_some_and(|value| value != 0)
        || matches!(row.get(index), Some(Value::Bool(true)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn text(value: &str) -> Value {
        Value::Text(value.to_string())
    }

    #[test]
    fn a_profile_carries_keys_codecs_sizes_and_facts() {
        let table_row = vec![
            text("MergeTree"),
            text("toYYYYMM(ts)"),
            text("site_id, toDate(ts), user_id"),
            text("site_id, toDate(ts), user_id"),
            text(""),
            text("1840000000"),
            Value::Int(400),
            text(
                "MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (site_id, toDate(ts), user_id) TTL ts + toIntervalMonth(13) SETTINGS index_granularity = 8192",
            ),
        ];
        let column_rows = vec![
            vec![
                text("site_id"),
                text("UInt32"),
                text(""),
                text("100"),
                text("4100"),
                Value::Int(1),
                Value::Int(0),
            ],
            vec![
                text("ts"),
                text("DateTime64(3)"),
                text("CODEC(Delta(8), ZSTD(1))"),
                text("300"),
                text("1800"),
                Value::Int(1),
                Value::Int(1),
            ],
            vec![
                text("path"),
                text("String"),
                text("CODEC(ZSTD(3))"),
                text("50"),
                text("500"),
                Value::Int(0),
                Value::Int(0),
            ],
        ];
        let parts_row = vec![text("1912"), text("450"), text("6400")];
        let captured_at = Utc.with_ymd_and_hms(2026, 9, 30, 12, 0, 0).unwrap();

        let profile = build_profile(&table_row, &column_rows, Some(&parts_row), captured_at);

        assert_eq!(
            profile.source_label,
            ProfileSource::Catalog("system.columns")
        );
        assert_eq!(profile.captured_at, Some(captured_at));
        assert_eq!(profile.row_count, Some(1_840_000_000));
        assert_eq!(profile.total_compressed_bytes, Some(450));
        assert_eq!(profile.total_uncompressed_bytes, Some(6400));
        assert_eq!(profile.engine.as_deref(), Some("MergeTree"));

        let site_id = &profile.columns[0];
        assert_eq!(
            site_id.badges,
            vec![ColumnBadge::SortKey { position: Some(1) }]
        );
        assert_eq!(site_id.codec.as_deref(), Some("default"));
        assert_eq!(site_id.compressed_bytes, Some(100));
        assert_eq!(site_id.compression_ratio(), Some(41.0));

        let ts = &profile.columns[1];
        assert_eq!(
            ts.badges,
            vec![
                ColumnBadge::SortKey { position: Some(2) },
                ColumnBadge::PartitionKey
            ]
        );
        assert_eq!(ts.codec.as_deref(), Some("Delta(8), ZSTD(1)"));

        assert!(profile.columns[2].badges.is_empty());
        assert_eq!(
            profile.columns[2].null_count, None,
            "metadata knows no nulls"
        );

        let facts: Vec<(TableFactKind, &str)> = profile
            .facts
            .iter()
            .map(|fact| (fact.kind.clone(), fact.value.as_ref()))
            .collect();
        assert_eq!(
            facts,
            vec![
                (TableFactKind::Engine, "MergeTree"),
                (TableFactKind::SortKey, "site_id, toDate(ts), user_id"),
                (TableFactKind::PartitionKey, "toYYYYMM(ts)"),
                (TableFactKind::Ttl, "ts + toIntervalMonth(13)"),
                (TableFactKind::ActiveParts, "1912"),
            ]
        );
    }

    #[test]
    fn a_table_without_parts_falls_back_to_column_sizes() {
        let table_row = vec![
            text("Memory"),
            text(""),
            text(""),
            text(""),
            text(""),
            Value::Null,
            Value::Null,
            text("Memory"),
        ];
        let column_rows = vec![vec![
            text("id"),
            text("UInt64"),
            text(""),
            text("8"),
            text("16"),
            Value::Int(0),
            Value::Int(0),
        ]];

        let profile = build_profile(
            &table_row,
            &column_rows,
            Some(&[text("0"), text("0"), text("0")]),
            Utc::now(),
        );

        assert_eq!(profile.total_compressed_bytes, Some(8));
        assert_eq!(profile.total_uncompressed_bytes, Some(16));
        assert_eq!(profile.row_count, None);
        assert_eq!(
            profile.facts.len(),
            1,
            "only the engine is known: {:?}",
            profile.facts
        );
    }

    #[test]
    fn keys_split_at_top_level_commas_and_name_whole_identifiers() {
        assert_eq!(
            split_key("(site_id, toDate(ts), user_id)"),
            vec!["site_id", "toDate(ts)", "user_id"]
        );
        assert_eq!(split_key("site_id"), vec!["site_id"]);
        assert_eq!(
            split_key("(a, b), c"),
            vec!["(a, b)", "c"],
            "an outer pair that does not wrap the whole key stays"
        );

        let items = split_key("site_id, toDate(ts), user_id");
        assert_eq!(key_position(&items, "ts"), Some(2));
        assert_eq!(key_position(&items, "user_id"), Some(3));
        assert_eq!(key_position(&items, "id"), None, "`id` is not `user_id`");
        assert_eq!(key_position(&split_key("`my col`"), "my col"), Some(1));
    }

    #[test]
    fn ttl_is_read_from_engine_full() {
        assert_eq!(
            ttl_clause("MergeTree ORDER BY id TTL ts + toIntervalDay(30) DELETE SETTINGS index_granularity = 8192").as_deref(),
            Some("ts + toIntervalDay(30) DELETE")
        );
        assert_eq!(
            ttl_clause("MergeTree ORDER BY id SETTINGS index_granularity = 8192"),
            None
        );
    }

    #[test]
    fn partitions_keep_a_time_range_and_drop_zero_ranges() {
        let with_time = vec![
            text("202609"),
            text("412"),
            text("1204331"),
            text("10"),
            text("65"),
            text("2026-09-01 00:00:00"),
            text("2026-09-30 23:59:59"),
            text("1970-01-01"),
            text("1970-01-01"),
            text("1790000000"),
        ];
        let partition = partition_from_row(&with_time).unwrap();

        assert_eq!(partition.name.as_ref(), "202609");
        assert_eq!(partition.parts, Some(412));
        assert_eq!(partition.rows, Some(1_204_331));
        assert_eq!(partition.compression_ratio(), Some(6.5));
        assert_eq!(
            partition.range.as_ref().map(|range| range.min.as_ref()),
            Some("2026-09-01 00:00:00")
        );
        assert_eq!(
            partition.modified_at,
            DateTime::from_timestamp(1_790_000_000, 0)
        );

        let without_time = vec![
            text("tuple()"),
            text("1"),
            text("5"),
            text("1"),
            text("1"),
            text("1970-01-01 00:00:00"),
            text("1970-01-01 00:00:00"),
            text("1970-01-01"),
            text("1970-01-01"),
            text("0"),
        ];
        let partition = partition_from_row(&without_time).unwrap();
        assert_eq!(partition.range, None);
        assert_eq!(partition.modified_at, None);
    }

    #[test]
    fn an_estimate_scales_projected_column_bytes_by_the_granules_read() {
        let profile = TableProfile::new(
            vec![
                ColumnProfile {
                    compressed_bytes: Some(1_000),
                    ..ColumnProfile::new("ts", "DateTime")
                },
                ColumnProfile {
                    compressed_bytes: Some(4_000),
                    ..ColumnProfile::new("path", "String")
                },
                ColumnProfile::new("unknown_size", "String"),
            ],
            ProfileSource::Catalog("system.columns"),
        );

        let estimate = build_estimate(
            &profile,
            &["path".to_string()],
            (1, 13),
            (512, 2048),
            Some(4_194_304),
        );

        assert_eq!(estimate.per_column, vec![(1, 1_000)]);
        assert_eq!(estimate.total_bytes, 1_000);
        assert_eq!(estimate.rows, Some(4_194_304));
        assert!(!estimate.exact);
        assert_eq!(
            estimate.scopes,
            vec![
                EstimateScope::Parts {
                    touched: 1,
                    total: 13,
                    unit: PartUnit::Parts
                },
                EstimateScope::Parts {
                    touched: 512,
                    total: 2048,
                    unit: PartUnit::Granules
                },
            ]
        );

        let everything = build_estimate(&profile, &[], (0, 0), (0, 0), None);
        assert_eq!(
            everything.per_column,
            vec![(0, 0), (1, 0)],
            "an empty table reads nothing"
        );
    }

    #[test]
    fn a_sample_query_caps_rows_inside_the_subquery_and_skips_ranges_of_complex_types() {
        let table = TableRef::with_schema("events", "page_views");
        let columns = vec!["ts".to_string(), "tags".to_string()];
        let types = HashMap::from([
            ("ts".to_string(), "Nullable(DateTime64(3))".to_string()),
            ("tags".to_string(), "Array(String)".to_string()),
        ]);

        assert_eq!(
            sample_sql(&table, &columns, &types, 1_000_000),
            "SELECT count(), countIf(isNull(`ts`)), uniq(`ts`), toString(min(`ts`)), toString(max(`ts`)), \
             countIf(isNull(`tags`)), uniq(`tags`) \
             FROM (SELECT `ts`, `tags` FROM `events`.`page_views` LIMIT 1000000)"
        );

        let row = vec![
            text("1000"),
            text("3"),
            text("998"),
            text("2026-09-01 00:00:00.000"),
            text("2026-09-30 23:59:59.999"),
            text("0"),
            text("12"),
        ];
        let sample = sample_from_row(&row, &columns, &types, Utc::now());

        assert_eq!(sample.source_label, ProfileSource::Sample { rows: 1000 });
        assert_eq!(sample.columns[0].null_count, Some(3));
        assert_eq!(sample.columns[0].distinct_count, Some(998));
        assert!(!sample.columns[0].distinct_exact);
        assert_eq!(
            sample.columns[0]
                .range
                .as_ref()
                .map(|range| range.max.as_ref()),
            Some("2026-09-30 23:59:59.999")
        );
        assert_eq!(sample.columns[1].distinct_count, Some(12));
        assert_eq!(sample.columns[1].range, None);
    }

    #[test]
    fn comparable_types_see_through_nullable_and_low_cardinality() {
        assert!(is_comparable("LowCardinality(Nullable(String))"));
        assert!(is_comparable("DateTime64(3, 'UTC')"));
        assert!(is_comparable("Decimal(18, 4)"));
        assert!(!is_comparable("Array(UInt8)"));
        assert!(!is_comparable("Map(String, UInt64)"));
        assert!(!is_comparable("AggregateFunction(uniq, UInt64)"));
        assert!(!is_comparable("JSON"));
    }
}
