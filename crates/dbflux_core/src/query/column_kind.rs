use crate::query::types::{ColumnKind, ColumnMeta};
use crate::query::visual_query::{AggFn, VisualQuerySpec};

/// Infers a `ColumnKind` from a driver-reported `type_name` string.
///
/// This is a fallback for columns that arrive via schema cache (`ColumnInfo`)
/// before a query has run. Drivers that set `ColumnMeta.kind` directly remain
/// authoritative; this function is only called when `kind` is not yet known.
///
/// The name is normalized (lowercased, parenthesized arguments such as
/// `(10,2)` removed, `unsigned`/`signed`/`zerofill` modifiers dropped, ClickHouse
/// `Nullable(..)`/`LowCardinality(..)` wrappers unwrapped) and then matched as
/// a whole type token, never as a substring:
/// - `Timestamp` — `timestamp`, `timestamptz`, `datetime`, `datetime2`, `date`,
///   `time`, `timestamp with time zone`, ...
/// - `Float` — `real`, `float`, `float8`, `double`, `double precision`,
///   `numeric`, `decimal`, `money`, ...
/// - `Integer` — `int`, `integer`, `int4`, `smallint`, `bigint`, `tinyint`,
///   `serial`, `bigserial`, `uint8`, ...
/// - `Text` — `char`, `varchar`, `character varying`, `nvarchar`, `text`,
///   `string`, `uuid`, `uniqueidentifier`, `json`, `jsonb`, `xml`, ...
/// - `Unknown` — everything else, including arrays (`int[]`, PostgreSQL's
///   internal `_int4`, `array<..>`, `Array(..)`), geometric types such as
///   `point`, `interval`, `bytea`, `blob`, `enum` and the empty string
pub fn infer_column_kind(type_name: &str) -> ColumnKind {
    let Some(token) = scalar_type_token(type_name) else {
        return ColumnKind::Unknown;
    };

    match classify_token(&token) {
        ColumnKind::Unknown => {}
        kind => return kind,
    }

    // SQLite and others accept multi-word numeric spellings such as
    // `big int` or `long double`: a name whose last word is a numeric type
    // keeps that kind. Only numeric kinds are taken from the last word, so
    // `interval day to second` and similar stay unknown.
    match token.rsplit_once(' ').map(|(_, last)| classify_token(last)) {
        Some(kind @ (ColumnKind::Integer | ColumnKind::Float)) => kind,
        _ => ColumnKind::Unknown,
    }
}

/// Classifies one normalized type token by exact match.
fn classify_token(token: &str) -> ColumnKind {
    match token {
        "timestamp"
        | "timestamptz"
        | "timestamp with time zone"
        | "timestamp without time zone"
        | "timestamp with local time zone"
        | "timestamp_ntz"
        | "timestamp_ltz"
        | "timestamp_tz"
        | "datetime"
        | "datetime2"
        | "datetime64"
        | "datetimeoffset"
        | "smalldatetime"
        | "date"
        | "date32"
        | "timestamp_s"
        | "timestamp_ms"
        | "timestamp_ns"
        | "time"
        | "timetz"
        | "time with time zone"
        | "time without time zone" => ColumnKind::Timestamp,

        "real" | "float" | "float4" | "float8" | "float16" | "float32" | "float64" | "bfloat16"
        | "double" | "double precision" | "binary_float" | "binary_double" | "numeric"
        | "decimal" | "dec" | "decimal32" | "decimal64" | "decimal128" | "decimal256" | "money"
        | "smallmoney" => ColumnKind::Float,

        "int" | "integer" | "int1" | "int2" | "int4" | "int8" | "int16" | "int32" | "int64"
        | "int128" | "int256" | "uint8" | "uint16" | "uint32" | "uint64" | "uint128"
        | "uint256" | "tinyint" | "smallint" | "mediumint" | "bigint" | "varint" | "hugeint"
        | "uhugeint" | "utinyint" | "usmallint" | "uinteger" | "ubigint" | "serial" | "serial2"
        | "serial4" | "serial8" | "smallserial" | "bigserial" => ColumnKind::Integer,

        "char"
        | "character"
        | "nchar"
        | "bpchar"
        | "varchar"
        | "varchar2"
        | "nvarchar"
        | "nvarchar2"
        | "character varying"
        | "char varying"
        | "national character"
        | "national character varying"
        | "text"
        | "tinytext"
        | "mediumtext"
        | "longtext"
        | "ntext"
        | "citext"
        | "string"
        | "fixedstring"
        | "uuid"
        | "uniqueidentifier"
        | "json"
        | "jsonb"
        | "xml" => ColumnKind::Text,

        _ => ColumnKind::Unknown,
    }
}

/// Reduces a driver type name to a lowercase scalar type token, or `None` for
/// non-scalar types (arrays, generics) that must not be classified by their
/// element type.
fn scalar_type_token(type_name: &str) -> Option<String> {
    let mut lower = type_name.trim().to_lowercase();

    loop {
        let unwrapped = ["nullable(", "lowcardinality("]
            .iter()
            .find_map(|wrapper| lower.strip_prefix(wrapper))
            .and_then(|inner| inner.strip_suffix(')'))
            .map(|inner| inner.trim().to_string());

        match unwrapped {
            Some(inner) => lower = inner,
            None => break,
        }
    }

    if lower.starts_with('_')
        || lower.starts_with("array")
        || lower.contains('[')
        || lower.contains('<')
    {
        return None;
    }

    let mut without_arguments = String::with_capacity(lower.len());
    let mut depth = 0usize;
    for character in lower.chars() {
        match character {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ if depth == 0 => without_arguments.push(character),
            _ => {}
        }
    }

    let words: Vec<&str> = without_arguments
        .split_whitespace()
        .filter(|word| !matches!(*word, "unsigned" | "signed" | "zerofill"))
        .collect();

    Some(words.join(" "))
}

/// Overwrites `ColumnKind` on aggregate result columns in a grouped query.
///
/// The `columns` slice must be positionally aligned with the query output:
/// the first `spec.group_by.len()` entries correspond to group-by columns and
/// are left untouched. The remaining entries correspond to `spec.aggregates` in
/// order; each entry's `kind` is set according to the aggregate function and
/// the kind already present in that slot (as reported by the driver for the
/// result column).
///
/// If `columns` is shorter than `group_by.len() + aggregates.len()`, the
/// function processes only the columns that exist and never panics.
///
/// Kind assignment rules per aggregate function:
/// - `Count`, `CountStar`, `CountDistinct` → always `Integer`
/// - `Avg` → always `Float`
/// - `Sum` → `Integer` if existing kind is `Integer`; `Float` if `Float`; else `Unknown`
/// - `Min`, `Max` → preserve the existing kind (driver-reported or inferred)
pub fn project_aggregate_kinds(spec: &VisualQuerySpec, columns: &mut [ColumnMeta]) {
    let offset = spec.group_by.len();

    for (i, agg) in spec.aggregates.iter().enumerate() {
        let col_index = offset + i;
        if col_index >= columns.len() {
            break;
        }

        let Some(column) = columns.get_mut(col_index) else {
            break;
        };

        let existing_kind = column.kind;

        column.kind = match agg.function {
            AggFn::Count | AggFn::CountStar | AggFn::CountDistinct => ColumnKind::Integer,
            AggFn::Avg => ColumnKind::Float,
            AggFn::Sum => match existing_kind {
                ColumnKind::Integer => ColumnKind::Integer,
                ColumnKind::Float => ColumnKind::Float,
                _ => ColumnKind::Unknown,
            },
            AggFn::Min | AggFn::Max => existing_kind,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::types::ColumnKind;

    #[test]
    fn sqlite_text_maps_to_text() {
        assert_eq!(infer_column_kind("TEXT"), ColumnKind::Text);
    }

    #[test]
    fn sqlite_integer_maps_to_integer() {
        assert_eq!(infer_column_kind("INTEGER"), ColumnKind::Integer);
    }

    #[test]
    fn sqlite_real_maps_to_float() {
        assert_eq!(infer_column_kind("REAL"), ColumnKind::Float);
    }

    #[test]
    fn postgres_text_maps_to_text() {
        assert_eq!(infer_column_kind("text"), ColumnKind::Text);
    }

    #[test]
    fn postgres_int4_maps_to_integer() {
        assert_eq!(infer_column_kind("int4"), ColumnKind::Integer);
    }

    #[test]
    fn postgres_int8_maps_to_integer() {
        assert_eq!(infer_column_kind("int8"), ColumnKind::Integer);
    }

    #[test]
    fn postgres_numeric_maps_to_float() {
        assert_eq!(infer_column_kind("numeric"), ColumnKind::Float);
    }

    #[test]
    fn postgres_timestamptz_maps_to_timestamp() {
        assert_eq!(infer_column_kind("timestamptz"), ColumnKind::Timestamp);
    }

    #[test]
    fn postgres_uuid_maps_to_text() {
        assert_eq!(infer_column_kind("uuid"), ColumnKind::Text);
    }

    #[test]
    fn postgres_jsonb_maps_to_text() {
        assert_eq!(infer_column_kind("jsonb"), ColumnKind::Text);
    }

    #[test]
    fn mysql_varchar_maps_to_text() {
        assert_eq!(infer_column_kind("varchar"), ColumnKind::Text);
    }

    #[test]
    fn mysql_datetime_maps_to_timestamp() {
        assert_eq!(infer_column_kind("datetime"), ColumnKind::Timestamp);
    }

    #[test]
    fn mysql_decimal_maps_to_float() {
        assert_eq!(infer_column_kind("decimal"), ColumnKind::Float);
    }

    #[test]
    fn mssql_nvarchar_maps_to_text() {
        assert_eq!(infer_column_kind("nvarchar"), ColumnKind::Text);
    }

    #[test]
    fn mssql_datetime2_maps_to_timestamp() {
        assert_eq!(infer_column_kind("datetime2"), ColumnKind::Timestamp);
    }

    #[test]
    fn mssql_uniqueidentifier_maps_to_text() {
        assert_eq!(infer_column_kind("uniqueidentifier"), ColumnKind::Text);
    }

    #[test]
    fn bytea_maps_to_unknown() {
        assert_eq!(infer_column_kind("bytea"), ColumnKind::Unknown);
    }

    #[test]
    fn enum_type_maps_to_unknown() {
        assert_eq!(infer_column_kind("enum"), ColumnKind::Unknown);
    }

    #[test]
    fn empty_string_maps_to_unknown() {
        assert_eq!(infer_column_kind(""), ColumnKind::Unknown);
    }

    #[test]
    fn blob_maps_to_unknown() {
        assert_eq!(infer_column_kind("blob"), ColumnKind::Unknown);
    }

    #[test]
    fn case_insensitive_matching() {
        assert_eq!(infer_column_kind("VARCHAR"), ColumnKind::Text);
        assert_eq!(infer_column_kind("Int"), ColumnKind::Integer);
        assert_eq!(infer_column_kind("FLOAT"), ColumnKind::Float);
        assert_eq!(infer_column_kind("TIMESTAMP"), ColumnKind::Timestamp);
    }

    #[test]
    fn bigint_maps_to_integer() {
        assert_eq!(infer_column_kind("bigint"), ColumnKind::Integer);
    }

    #[test]
    fn money_maps_to_float() {
        assert_eq!(infer_column_kind("money"), ColumnKind::Float);
    }

    #[test]
    fn non_scalar_types_are_not_numeric_or_timestamp() {
        for name in [
            "point",
            "int[]",
            "_int4",
            "numeric[]",
            "integer[]",
            "timestamp[]",
            "interval",
            "array<int>",
            "Array(Int32)",
            "_timestamptz",
            "line",
            "polygon",
        ] {
            assert_eq!(infer_column_kind(name), ColumnKind::Unknown, "{name}");
        }
    }

    #[test]
    fn integer_tokens_keep_integer_kind() {
        for name in [
            "integer",
            "int4",
            "bigint",
            "smallint",
            "int unsigned",
            "bigint unsigned",
            "tinyint(1)",
            "int(11)",
            "Nullable(Int32)",
            "serial",
        ] {
            assert_eq!(infer_column_kind(name), ColumnKind::Integer, "{name}");
        }
    }

    #[test]
    fn float_tokens_keep_float_kind() {
        for name in [
            "numeric(10,2)",
            "decimal",
            "decimal(10, 2)",
            "double precision",
            "real",
            "float8",
            "DOUBLE PRECISION",
            "Decimal(18, 4)",
        ] {
            assert_eq!(infer_column_kind(name), ColumnKind::Float, "{name}");
        }
    }

    #[test]
    fn multi_word_scalar_tokens_keep_their_kind() {
        assert_eq!(
            infer_column_kind("timestamp with time zone"),
            ColumnKind::Timestamp
        );
        assert_eq!(
            infer_column_kind("timestamp(3) without time zone"),
            ColumnKind::Timestamp
        );
        assert_eq!(
            infer_column_kind("character varying(255)"),
            ColumnKind::Text
        );
        assert_eq!(infer_column_kind("varchar(255)"), ColumnKind::Text);
    }

    #[test]
    fn former_substring_false_positives_are_unknown() {
        for name in [
            "interval",
            "interval day to second",
            "int4range",
            "daterange",
            "timeuuid",
            "Map(String, UInt64)",
            "Tuple(Int32, String)",
            "money[]",
            "_numeric",
        ] {
            assert_eq!(infer_column_kind(name), ColumnKind::Unknown, "{name}");
        }
    }

    #[test]
    fn driver_numeric_names_keep_their_kind() {
        for name in [
            "mediumint unsigned zerofill",
            "smallint unsigned",
            "UInt64",
            "Int64",
            "int64",
            "hugeint",
        ] {
            assert_eq!(infer_column_kind(name), ColumnKind::Integer, "{name}");
        }
        for name in [
            "Float32",
            "Float64",
            "Decimal128",
            "smallmoney",
            "Nullable(Decimal(18, 4))",
            "double unsigned",
            "decimal(10,2) unsigned",
            "float(53)",
        ] {
            assert_eq!(infer_column_kind(name), ColumnKind::Float, "{name}");
        }
    }

    #[test]
    fn multi_word_numeric_declarations_keep_their_kind() {
        for name in ["UNSIGNED BIG INT", "big int", "long integer", "BIG INT(11)"] {
            assert_eq!(infer_column_kind(name), ColumnKind::Integer, "{name}");
        }
        for name in ["long double", "LONG DOUBLE"] {
            assert_eq!(infer_column_kind(name), ColumnKind::Float, "{name}");
        }
        for name in ["interval day to second", "point", "big int[]", "int4range"] {
            assert_eq!(infer_column_kind(name), ColumnKind::Unknown, "{name}");
        }
    }
}
