//! The Columns view of a DuckDB table.
//!
//! The profile comes from `duckdb_columns()` and `pragma_storage_info`, which
//! report each column's compression and the min/max and null flags of every
//! stored segment without reading the rows. DuckDB does not report bytes per
//! column, so sizes stay unknown. The sample profile reads rows from a
//! subquery whose `LIMIT` caps what it reads.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use dbflux_core::{
    ColumnProfile, ProfileSource, SqlDialect, TableFact, TableFactKind, TableProfile, TableRef,
    Value, ValueRange,
};

use crate::dialect::DUCKDB_DIALECT;

const STORAGE_SOURCE: &str = "pragma_storage_info";

/// One stored segment of a top-level column or of its validity mask.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Segment {
    pub column_id: u32,
    /// True for the segment holding the values, false for the validity mask.
    pub values: bool,
    pub compression: String,
    pub count: u64,
    pub min: Option<String>,
    pub max: Option<String>,
    /// Whether the segment saw a null; `None` when its statistics omit it.
    pub has_null: Option<bool>,
    pub row_group: u64,
}

/// The `pragma_storage_info` rows of a table (row group, column id, path,
/// compression, count, min, max, has-null), keeping only the top-level
/// value segments (`[i]`) and their validity masks (`[i, 0]`). Nested
/// children of lists and structs are dropped.
pub(crate) fn segments_from_rows(rows: &[Vec<Value>]) -> Vec<Segment> {
    rows.iter()
        .filter_map(|row| {
            let row_group = integer(row.first()?)?;
            let column_id = u32::try_from(integer(row.get(1)?)?).ok()?;
            let path = text(row.get(2)?)?;
            let compression = text(row.get(3)?)?.to_string();
            let count = integer(row.get(4)?)?;
            let min = row.get(5).and_then(text).map(str::to_string);
            let max = row.get(6).and_then(text).map(str::to_string);
            let has_null = match row.get(7) {
                Some(Value::Bool(flag)) => Some(*flag),
                _ => None,
            };

            let parts: Vec<&str> = path
                .trim_matches(['[', ']'])
                .split(',')
                .map(str::trim)
                .collect();

            let values = match parts.as_slice() {
                [_] => true,
                [_, "0"] => false,
                _ => return None,
            };

            Some(Segment {
                column_id,
                values,
                compression,
                count,
                min,
                max,
                has_null,
                row_group,
            })
        })
        .collect()
}

/// The profile of a table from its columns (name and type in order) and
/// its stored segments.
pub(crate) fn build_profile(
    columns: &[(String, String)],
    segments: &[Segment],
    captured_at: DateTime<Utc>,
) -> TableProfile {
    let mut by_column: BTreeMap<u32, Vec<&Segment>> = BTreeMap::new();

    for segment in segments {
        by_column
            .entry(segment.column_id)
            .or_default()
            .push(segment);
    }

    let row_count = by_column.get(&0).map(|segments| {
        segments
            .iter()
            .filter(|segment| segment.values)
            .map(|segment| segment.count)
            .sum::<u64>()
    });

    let row_groups = segments
        .iter()
        .map(|segment| segment.row_group)
        .collect::<std::collections::BTreeSet<_>>()
        .len();

    let profiles = columns
        .iter()
        .enumerate()
        .map(|(index, (name, type_name))| {
            let column_segments = u32::try_from(index)
                .ok()
                .and_then(|id| by_column.get(&id))
                .map(Vec::as_slice)
                .unwrap_or(&[]);

            column_profile(name, type_name, column_segments)
        })
        .collect();

    let mut facts = vec![TableFact::new(TableFactKind::Engine, "DuckDB")];

    if row_groups > 0 {
        facts.push(TableFact::new(
            TableFactKind::RowGroups,
            row_groups.to_string(),
        ));
    }

    TableProfile {
        row_count,
        row_count_exact: true,
        engine: Some("DuckDB".into()),
        facts,
        captured_at: Some(captured_at),
        ..TableProfile::new(profiles, ProfileSource::Catalog(STORAGE_SOURCE))
    }
}

fn column_profile(name: &str, type_name: &str, segments: &[&Segment]) -> ColumnProfile {
    let value_segments: Vec<&Segment> = segments
        .iter()
        .copied()
        .filter(|segment| segment.values)
        .collect();

    let mut codecs: Vec<&str> = Vec::new();
    for segment in &value_segments {
        if !codecs.contains(&segment.compression.as_str()) {
            codecs.push(&segment.compression);
        }
    }

    let flat = is_flat(type_name);
    let range = if flat {
        combined_range(
            &value_segments,
            numeric_type(type_name),
            exact_bounds(type_name),
        )
    } else {
        None
    };

    // A validity mask that never saw a null proves the column has none; a
    // segment that did see one does not say how many.
    let validity: Vec<&Segment> = segments
        .iter()
        .copied()
        .filter(|segment| !segment.values)
        .collect();
    let null_count = (!validity.is_empty()
        && validity
            .iter()
            .all(|segment| segment.has_null == Some(false)))
    .then_some(0);

    ColumnProfile {
        codec: (!codecs.is_empty()).then(|| codecs.join(", ").into()),
        null_count,
        range,
        ..ColumnProfile::new(name, type_name)
    }
}

/// The smallest minimum and largest maximum of the segments' statistics.
fn combined_range(segments: &[&Segment], numeric: bool, exact: bool) -> Option<ValueRange> {
    let mut low: Option<String> = None;
    let mut high: Option<String> = None;

    for segment in segments {
        let (Some(min), Some(max)) = (segment.min.clone(), segment.max.clone()) else {
            continue;
        };

        if low
            .as_deref()
            .is_none_or(|current| compare(&min, current, numeric) == Ordering::Less)
        {
            low = Some(min);
        }

        if high
            .as_deref()
            .is_none_or(|current| compare(&max, current, numeric) == Ordering::Greater)
        {
            high = Some(max);
        }
    }

    Some(ValueRange {
        min: low?.into(),
        max: high?.into(),
        exact,
    })
}

fn compare(left: &str, right: &str, numeric: bool) -> Ordering {
    if numeric {
        if let (Ok(left), Ok(right)) = (left.parse::<i128>(), right.parse::<i128>()) {
            return left.cmp(&right);
        }

        if let (Ok(left), Ok(right)) = (left.parse::<f64>(), right.parse::<f64>()) {
            return left.partial_cmp(&right).unwrap_or(Ordering::Equal);
        }
    }

    left.cmp(right)
}

/// Whether the type holds one scalar per row: no lists, arrays, structs,
/// maps or unions, whose statistics describe their children.
pub(crate) fn is_flat(type_name: &str) -> bool {
    let upper = type_name.to_ascii_uppercase();

    !upper.contains('[')
        && !upper.starts_with("STRUCT")
        && !upper.starts_with("MAP")
        && !upper.starts_with("UNION")
}

fn numeric_type(type_name: &str) -> bool {
    let upper = type_name.to_ascii_uppercase();

    [
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "UHUGEINT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL",
        "REAL",
    ]
    .iter()
    .any(|name| upper.starts_with(name))
}

/// Whether the statistics bounds are values of the column. DuckDB keeps only
/// a prefix of a string in its statistics, so string bounds are not.
fn exact_bounds(type_name: &str) -> bool {
    let upper = type_name.to_ascii_uppercase();

    !(upper.starts_with("VARCHAR") || upper.starts_with("BLOB") || upper == "JSON")
}

/// The one-row query a sample profile runs: the rows read, then for each
/// column its nulls, an approximate distinct count and, for flat types, its
/// smallest and largest value as text. The `LIMIT` inside the subquery caps
/// the rows read.
pub(crate) fn sample_sql(
    qualified_table: &str,
    columns: &[(String, String)],
    row_limit: u64,
) -> String {
    let mut aggregates = vec!["count(*)".to_string()];

    for (name, type_name) in columns {
        let quoted = DUCKDB_DIALECT.quote_identifier(name);
        aggregates.push(format!("count(*) - count({quoted})"));
        aggregates.push(format!("approx_count_distinct({quoted})"));

        if is_flat(type_name) {
            aggregates.push(format!("min({quoted})::VARCHAR"));
            aggregates.push(format!("max({quoted})::VARCHAR"));
        }
    }

    let projection = columns
        .iter()
        .map(|(name, _)| DUCKDB_DIALECT.quote_identifier(name))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "SELECT {} FROM (SELECT {projection} FROM {qualified_table} LIMIT {row_limit})",
        aggregates.join(", ")
    )
}

pub(crate) fn sample_from_row(
    row: &[Value],
    columns: &[(String, String)],
    captured_at: DateTime<Utc>,
) -> TableProfile {
    let rows_read = row.first().and_then(integer).unwrap_or(0);
    let mut index = 1;
    let mut profiles = Vec::new();

    for (name, type_name) in columns {
        let null_count = row.get(index).and_then(integer);
        let distinct_count = row.get(index + 1).and_then(integer);
        index += 2;

        let range = if is_flat(type_name) {
            let range = match (
                row.get(index).and_then(text),
                row.get(index + 1).and_then(text),
            ) {
                (Some(min), Some(max)) => Some(ValueRange {
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
            ..ColumnProfile::new(name.as_str(), type_name.as_str())
        });
    }

    TableProfile {
        row_count: Some(rows_read),
        captured_at: Some(captured_at),
        ..TableProfile::new(profiles, ProfileSource::Sample { rows: rows_read })
    }
}

/// `catalog.schema.table`, each part quoted, for a table of the active
/// catalog when it names none.
pub(crate) fn qualified_name(catalog: &str, table: &TableRef) -> String {
    let schema = table.schema.as_deref().unwrap_or("main");

    [catalog, schema, table.name.as_str()]
        .iter()
        .map(|part| DUCKDB_DIALECT.quote_identifier(part))
        .collect::<Vec<_>>()
        .join(".")
}

fn integer(value: &Value) -> Option<u64> {
    match value {
        Value::Int(value) => u64::try_from(*value).ok(),
        Value::Text(value) | Value::Decimal(value) => value.parse().ok(),
        _ => None,
    }
}

fn text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) => Some(value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        row_group: i64,
        column: i64,
        path: &str,
        compression: &str,
        count: i64,
        range: Option<(&str, &str)>,
        has_null: Option<bool>,
    ) -> Vec<Value> {
        let bound =
            |value: Option<&str>| value.map_or(Value::Null, |text| Value::Text(text.into()));

        vec![
            Value::Int(row_group),
            Value::Int(column),
            Value::Text(path.to_string()),
            Value::Text(compression.to_string()),
            Value::Int(count),
            bound(range.map(|(min, _)| min)),
            bound(range.map(|(_, max)| max)),
            has_null.map_or(Value::Null, Value::Bool),
        ]
    }

    fn columns() -> Vec<(String, String)> {
        [
            ("id", "BIGINT"),
            ("path", "VARCHAR"),
            ("n", "BIGINT"),
            ("arr", "BIGINT[]"),
        ]
        .iter()
        .map(|(name, type_name)| (name.to_string(), type_name.to_string()))
        .collect()
    }

    fn storage_rows() -> Vec<Vec<Value>> {
        vec![
            row(
                0,
                0,
                "[0]",
                "BitPacking",
                122_880,
                Some(("0", "122879")),
                Some(false),
            ),
            row(0, 0, "[0, 0]", "Constant", 122_880, None, Some(false)),
            row(
                1,
                0,
                "[0]",
                "BitPacking",
                177_120,
                Some(("122880", "299999")),
                Some(false),
            ),
            row(1, 0, "[0, 0]", "Constant", 177_120, None, Some(false)),
            row(
                0,
                1,
                "[1]",
                "Dictionary",
                122_880,
                Some(("path0", "path9")),
                Some(false),
            ),
            row(
                1,
                1,
                "[1]",
                "FSST",
                177_120,
                Some(("path1", "path8")),
                Some(false),
            ),
            row(
                0,
                2,
                "[2]",
                "BitPacking",
                122_880,
                Some(("0", "6")),
                Some(true),
            ),
            row(0, 2, "[2, 0]", "Uncompressed", 122_880, None, Some(true)),
            row(0, 3, "[3]", "BitPacking", 122_880, None, Some(false)),
            row(
                0,
                3,
                "[3, 1]",
                "BitPacking",
                245_760,
                Some(("0", "122880")),
                Some(false),
            ),
            row(0, 3, "[3, 1, 0]", "Constant", 245_760, None, Some(false)),
        ]
    }

    #[test]
    fn a_profile_reads_codecs_ranges_and_null_flags_from_storage() {
        let segments = segments_from_rows(&storage_rows());
        assert_eq!(segments.len(), 9, "nested child segments are dropped");

        let profile = build_profile(&columns(), &segments, Utc::now());

        assert_eq!(
            profile.source_label,
            ProfileSource::Catalog("pragma_storage_info")
        );
        assert_eq!(profile.row_count, Some(300_000));
        assert_eq!(profile.engine.as_deref(), Some("DuckDB"));
        assert!(
            profile
                .facts
                .contains(&TableFact::new(TableFactKind::RowGroups, "2"))
        );

        let id = &profile.columns[0];
        assert_eq!(id.codec.as_deref(), Some("BitPacking"));
        assert_eq!(id.null_count, Some(0), "no segment saw a null");
        let range = id.range.as_ref().unwrap();
        assert_eq!(
            (range.min.as_ref(), range.max.as_ref(), range.exact),
            ("0", "299999", true)
        );
        assert_eq!(
            id.compressed_bytes, None,
            "DuckDB reports no bytes per column"
        );

        let path = &profile.columns[1];
        assert_eq!(path.codec.as_deref(), Some("Dictionary, FSST"));
        assert!(
            !path.range.as_ref().unwrap().exact,
            "string bounds are prefixes"
        );

        assert_eq!(
            profile.columns[2].null_count, None,
            "a null was seen, its count is unknown"
        );
        assert_eq!(
            profile.columns[3].range, None,
            "a list's statistics describe its children"
        );
        assert_eq!(profile.columns[3].codec.as_deref(), Some("BitPacking"));
    }

    #[test]
    fn numbers_compare_as_numbers_across_segments() {
        let segments = vec![
            Segment {
                column_id: 0,
                values: true,
                compression: "RLE".into(),
                count: 1,
                min: Some("9".into()),
                max: Some("10".into()),
                has_null: Some(false),
                row_group: 0,
            },
            Segment {
                column_id: 0,
                values: true,
                compression: "RLE".into(),
                count: 1,
                min: Some("100".into()),
                max: Some("1000".into()),
                has_null: Some(false),
                row_group: 1,
            },
        ];
        let profile = build_profile(
            &[("v".to_string(), "INTEGER".to_string())],
            &segments,
            Utc::now(),
        );
        let range = profile.columns[0].range.as_ref().unwrap();

        assert_eq!((range.min.as_ref(), range.max.as_ref()), ("9", "1000"));
    }

    #[test]
    fn string_bounds_are_kept_verbatim_and_missing_statistics_are_skipped() {
        let rows = vec![
            row(
                0,
                0,
                "[0]",
                "FSST",
                10,
                Some(("a, Max: b", "z]")),
                Some(false),
            ),
            row(1, 0, "[0]", "FSST", 10, None, None),
            row(1, 0, "[0, 0]", "Constant", 10, None, None),
        ];
        let profile = build_profile(
            &[("s".to_string(), "VARCHAR".to_string())],
            &segments_from_rows(&rows),
            Utc::now(),
        );
        let column = &profile.columns[0];
        let range = column.range.as_ref().unwrap();

        assert_eq!(
            (range.min.as_ref(), range.max.as_ref()),
            ("a, Max: b", "z]")
        );
        assert_eq!(profile.row_count, Some(20));
        assert_eq!(
            column.null_count, None,
            "an unknown null flag proves nothing"
        );
    }

    #[test]
    fn a_sample_query_caps_rows_and_skips_ranges_of_nested_types() {
        let sql = sample_sql("\"lake\".\"main\".\"ev\"", &columns()[2..], 1_000);

        assert_eq!(
            sql,
            "SELECT count(*), count(*) - count(\"n\"), approx_count_distinct(\"n\"), min(\"n\")::VARCHAR, max(\"n\")::VARCHAR, \
             count(*) - count(\"arr\"), approx_count_distinct(\"arr\") \
             FROM (SELECT \"n\", \"arr\" FROM \"lake\".\"main\".\"ev\" LIMIT 1000)"
        );

        let row = vec![
            Value::Int(1_000),
            Value::Int(100),
            Value::Int(7),
            Value::Text("0".into()),
            Value::Text("6".into()),
            Value::Int(0),
            Value::Int(990),
        ];
        let sample = sample_from_row(&row, &columns()[2..], Utc::now());

        assert_eq!(sample.source_label, ProfileSource::Sample { rows: 1_000 });
        assert_eq!(sample.columns[0].null_count, Some(100));
        assert_eq!(sample.columns[0].distinct_count, Some(7));
        assert_eq!(sample.columns[1].range, None);
        assert_eq!(sample.columns[1].distinct_count, Some(990));
    }

    #[test]
    fn qualified_names_quote_every_part() {
        assert_eq!(
            qualified_name("my db", &TableRef::with_schema("main", "it\"s")),
            "\"my db\".\"main\".\"it\"\"s\""
        );
        assert_eq!(
            qualified_name("lake", &TableRef::new("ev")),
            "\"lake\".\"main\".\"ev\""
        );
    }
}
