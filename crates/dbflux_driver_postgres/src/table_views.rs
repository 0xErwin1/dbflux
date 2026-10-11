//! The Columns view of a PostgreSQL table, from planner statistics.
//!
//! `pg_stats` keeps, per column, the share of nulls, an estimate of distinct
//! values, histogram bounds and the most common values with their
//! frequencies. They are as fresh as the last `ANALYZE` and reading them
//! reads no table rows. A column the planner has no statistics for keeps
//! every fact unknown.

use chrono::{DateTime, Utc};
use dbflux_core::{
    ColumnProfile, DbError, ProfileSource, TableFact, TableFactKind, TableProfile, TableRef,
    ValueCount, ValueRange,
};
use postgres::Client;

const STATISTICS_SOURCE: &str = "pg_stats";

/// What `pg_stats` says about one column.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ColumnStatistics {
    pub name: String,
    pub type_name: String,
    pub null_fraction: Option<f32>,
    /// Positive: the number of distinct values. Negative: minus the share of
    /// rows that are distinct, as `pg_stats` stores it.
    pub distinct: Option<f32>,
    pub histogram_low: Option<String>,
    pub histogram_high: Option<String>,
    pub common_values: Vec<String>,
    pub common_frequencies: Vec<f32>,
}

/// What the catalog says about the table itself.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct TableStatistics {
    /// The planner's row estimate; negative when the table was never
    /// analyzed (PostgreSQL 14 and later).
    pub row_estimate: Option<f64>,
    pub table_bytes: Option<i64>,
    pub access_method: Option<String>,
    pub partition_key: Option<String>,
    /// Seconds since the epoch of the last manual or automatic analyze.
    pub analyzed_at: Option<f64>,
}

/// A partitioned table has no storage of its own: its size and row estimate
/// are the sums over its leaf partitions, found through `pg_inherits` so that
/// servers older than PostgreSQL 12 (no `pg_partition_tree`) are covered. A
/// leaf that was never analyzed has no row estimate and is left out of the
/// sum; when no leaf has one, the estimate stays unknown.
const TABLE_QUERY: &str = r#"
    WITH RECURSIVE target AS (
        SELECT c.oid, c.relkind, c.reltuples, c.relam
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2
    ),
    tree(relid) AS (
        SELECT oid FROM target WHERE relkind = 'p'
        UNION ALL
        SELECT i.inhrelid
        FROM pg_catalog.pg_inherits i
        JOIN tree ON i.inhparent = tree.relid
    ),
    leaves AS (
        SELECT
            count(*) AS leaf_count,
            sum(c.reltuples::float8) FILTER (WHERE c.reltuples >= 0) AS row_estimate,
            sum(pg_catalog.pg_table_size(c.oid))::bigint AS table_bytes
        FROM tree
        JOIN pg_catalog.pg_class c ON c.oid = tree.relid
        WHERE c.relkind <> 'p'
    )
    SELECT CASE
               WHEN t.relkind <> 'p' THEN t.reltuples::float8
               WHEN l.leaf_count = 0 THEN 0
               ELSE l.row_estimate
           END,
           CASE
               WHEN t.relkind <> 'p' THEN pg_catalog.pg_table_size(t.oid)
               ELSE COALESCE(l.table_bytes, 0)
           END,
           am.amname::text,
           CASE WHEN t.relkind = 'p' THEN pg_catalog.pg_get_partkeydef(t.oid) END,
           EXTRACT(EPOCH FROM GREATEST(st.last_analyze, st.last_autoanalyze))::float8
    FROM target t
    CROSS JOIN leaves l
    LEFT JOIN pg_catalog.pg_am am ON am.oid = t.relam
    LEFT JOIN pg_catalog.pg_stat_all_tables st ON st.relid = t.oid
"#;

/// A partitioned table keeps its statistics as inherited rows.
const COLUMNS_QUERY: &str = r#"
    SELECT a.attname::text,
           pg_catalog.format_type(a.atttypid, a.atttypmod),
           s.null_frac,
           s.n_distinct,
           (s.histogram_bounds::text::text[])[1],
           (s.histogram_bounds::text::text[])[pg_catalog.array_upper(s.histogram_bounds::text::text[], 1)],
           s.most_common_vals::text::text[],
           s.most_common_freqs
    FROM pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_stats s
        ON s.schemaname = n.nspname
       AND s.tablename = c.relname
       AND s.attname = a.attname
       AND s.inherited = (c.relkind = 'p')
    WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
"#;

pub(crate) fn table_profile(
    client: &mut Client,
    table: &TableRef,
) -> Result<TableProfile, DbError> {
    let schema = table.schema.as_deref().unwrap_or("public");
    let name = table.name.as_str();

    let table_row = client
        .query_opt(TABLE_QUERY, &[&schema, &name])
        .map_err(|error| crate::driver::format_pg_query_error(&error))?
        .ok_or_else(|| {
            DbError::ObjectNotFound(format!("Relation '{schema}.{name}' not found").into())
        })?;

    let table_statistics = TableStatistics {
        row_estimate: table_row.get(0),
        table_bytes: table_row.get(1),
        access_method: table_row.get(2),
        partition_key: table_row.get(3),
        analyzed_at: table_row.get(4),
    };

    let columns = client
        .query(COLUMNS_QUERY, &[&schema, &name])
        .map_err(|error| crate::driver::format_pg_query_error(&error))?
        .iter()
        .map(|row| ColumnStatistics {
            name: row.get(0),
            type_name: row.get(1),
            null_fraction: row.get(2),
            distinct: row.get(3),
            histogram_low: row.get(4),
            histogram_high: row.get(5),
            common_values: row.get::<_, Option<Vec<String>>>(6).unwrap_or_default(),
            common_frequencies: row.get::<_, Option<Vec<f32>>>(7).unwrap_or_default(),
        })
        .collect::<Vec<_>>();

    Ok(build_profile(&table_statistics, &columns, Utc::now()))
}

pub(crate) fn build_profile(
    table: &TableStatistics,
    columns: &[ColumnStatistics],
    captured_at: DateTime<Utc>,
) -> TableProfile {
    let rows = table
        .row_estimate
        .filter(|rows| *rows >= 0.0)
        .map(|rows| rows.round() as u64);

    let profiles = columns
        .iter()
        .map(|column| column_profile(column, rows))
        .collect();

    let mut facts = Vec::new();

    if let Some(method) = &table.access_method {
        facts.push(TableFact::new(TableFactKind::Engine, method.as_str()));
    }

    if let Some(key) = &table.partition_key {
        facts.push(TableFact::new(TableFactKind::PartitionKey, key.as_str()));
    }

    let analyzed = table
        .analyzed_at
        .and_then(|seconds| DateTime::from_timestamp(seconds as i64, 0))
        .map(|at| at.format("%Y-%m-%d %H:%M UTC").to_string());

    facts.push(TableFact::new(
        TableFactKind::StatisticsUpdated,
        analyzed.unwrap_or_else(|| "never".to_string()),
    ));

    TableProfile {
        row_count: rows,
        row_count_exact: false,
        total_compressed_bytes: table
            .table_bytes
            .and_then(|bytes| u64::try_from(bytes).ok()),
        engine: table.access_method.as_deref().map(Into::into),
        facts,
        captured_at: Some(captured_at),
        ..TableProfile::new(
            profiles,
            ProfileSource::PlannerStatistics(STATISTICS_SOURCE),
        )
    }
}

fn column_profile(column: &ColumnStatistics, rows: Option<u64>) -> ColumnProfile {
    let scale = |fraction: f64| rows.map(|rows| (fraction * rows as f64).round() as u64);

    let null_count = column
        .null_fraction
        .and_then(|fraction| scale(f64::from(fraction)));

    let distinct_count = column.distinct.and_then(|distinct| {
        if distinct >= 0.0 {
            Some(distinct.round() as u64)
        } else {
            scale(f64::from(-distinct))
        }
    });

    let range = match (&column.histogram_low, &column.histogram_high) {
        (Some(low), Some(high)) => Some(ValueRange {
            min: low.as_str().into(),
            max: high.as_str().into(),
            // Histogram bounds are values of the column, but the most common
            // values are left out of the histogram, so a bound may not be the
            // smallest or largest value.
            exact: false,
        }),
        _ => None,
    };

    let top_values = column
        .common_values
        .iter()
        .zip(&column.common_frequencies)
        .filter_map(|(value, frequency)| {
            Some(ValueCount {
                value: value.as_str().into(),
                count: scale(f64::from(*frequency))?,
            })
        })
        .collect();

    ColumnProfile {
        null_count,
        distinct_count,
        distinct_exact: false,
        range,
        top_values,
        ..ColumnProfile::new(column.name.as_str(), column.type_name.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn analyzed_table() -> TableStatistics {
        TableStatistics {
            row_estimate: Some(1_000.0),
            table_bytes: Some(81_920),
            access_method: Some("heap".to_string()),
            partition_key: None,
            analyzed_at: Some(1_790_000_000.0),
        }
    }

    #[test]
    fn statistics_become_counts_ranges_and_top_values() {
        let columns = vec![
            ColumnStatistics {
                name: "country".to_string(),
                type_name: "text".to_string(),
                null_fraction: Some(0.25),
                distinct: Some(3.0),
                common_values: vec!["US".to_string(), "DE".to_string()],
                common_frequencies: vec![0.5, 0.2],
                ..ColumnStatistics::default()
            },
            ColumnStatistics {
                name: "id".to_string(),
                type_name: "integer".to_string(),
                null_fraction: Some(0.0),
                distinct: Some(-1.0),
                histogram_low: Some("1".to_string()),
                histogram_high: Some("1000".to_string()),
                ..ColumnStatistics::default()
            },
            ColumnStatistics {
                name: "never_analyzed".to_string(),
                type_name: "jsonb".to_string(),
                ..ColumnStatistics::default()
            },
        ];

        let profile = build_profile(&analyzed_table(), &columns, Utc::now());

        assert_eq!(
            profile.source_label,
            ProfileSource::PlannerStatistics("pg_stats")
        );
        assert_eq!(profile.row_count, Some(1_000));
        assert!(!profile.row_count_exact);
        assert_eq!(profile.total_compressed_bytes, Some(81_920));
        assert_eq!(profile.engine.as_deref(), Some("heap"));
        assert!(profile.facts.contains(&TableFact::new(
            TableFactKind::StatisticsUpdated,
            "2026-09-21 14:13 UTC"
        )));

        let country = &profile.columns[0];
        assert_eq!(country.null_count, Some(250));
        assert_eq!(country.distinct_count, Some(3));
        assert!(!country.distinct_exact);
        assert_eq!(
            country.top_values,
            vec![
                ValueCount {
                    value: "US".into(),
                    count: 500
                },
                ValueCount {
                    value: "DE".into(),
                    count: 200
                },
            ]
        );

        let id = &profile.columns[1];
        assert_eq!(
            id.distinct_count,
            Some(1_000),
            "-1 means every row is distinct"
        );
        assert_eq!(id.null_count, Some(0));
        let range = id.range.as_ref().unwrap();
        assert_eq!(
            (range.min.as_ref(), range.max.as_ref(), range.exact),
            ("1", "1000", false)
        );

        let unknown = &profile.columns[2];
        assert_eq!(unknown.null_count, None);
        assert_eq!(unknown.distinct_count, None);
        assert!(unknown.top_values.is_empty());
        assert_eq!(
            unknown.codec, None,
            "planner statistics know nothing about storage"
        );
    }

    #[test]
    fn a_table_never_analyzed_says_so_and_scales_nothing() {
        let table = TableStatistics {
            row_estimate: Some(-1.0),
            analyzed_at: None,
            partition_key: Some("RANGE (created_at)".to_string()),
            ..analyzed_table()
        };
        let columns = vec![ColumnStatistics {
            name: "id".to_string(),
            type_name: "integer".to_string(),
            null_fraction: Some(0.5),
            distinct: Some(-0.5),
            ..ColumnStatistics::default()
        }];

        let profile = build_profile(&table, &columns, Utc::now());

        assert_eq!(profile.row_count, None);
        assert_eq!(
            profile.columns[0].null_count, None,
            "a fraction of unknown rows is unknown"
        );
        assert_eq!(profile.columns[0].distinct_count, None);
        assert!(
            profile
                .facts
                .contains(&TableFact::new(TableFactKind::StatisticsUpdated, "never"))
        );
        assert!(profile.facts.contains(&TableFact::new(
            TableFactKind::PartitionKey,
            "RANGE (created_at)"
        )));
    }
}
