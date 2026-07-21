use crate::query::types::{ColumnKind, ColumnMeta};
use crate::query::visual_query::{AggFn, VisualQuerySpec};

/// Infers a `ColumnKind` from a driver-reported `type_name` string using
/// case-insensitive substring matching.
///
/// This is a fallback for columns that arrive via schema cache (`ColumnInfo`)
/// before a query has run. Drivers that set `ColumnMeta.kind` directly remain
/// authoritative; this function is only called when `kind` is not yet known.
///
/// Mapping rules (case-insensitive substring match against `type_name`):
/// - `Timestamp` — contains: `timestamp`, `datetime`, `date`, `time`
/// - `Float` — contains: `real`, `double`, `float`, `numeric`, `decimal`, `money`
/// - `Integer` — contains: `int`, `serial`, `bigserial`, `smallint`, `bigint`,
///   `tinyint`
/// - `Text` — contains: `char`, `text`, `varchar`, `nvarchar`, `string`, `uuid`,
///   `json`, `xml`
/// - `Unknown` — everything else (e.g. `bytea`, `blob`, `enum`, empty string)
pub fn infer_column_kind(type_name: &str) -> ColumnKind {
    let lower = type_name.to_lowercase();

    if lower.contains("timestamp")
        || lower.contains("datetime")
        || lower.contains("date")
        || lower.contains("time")
    {
        return ColumnKind::Timestamp;
    }

    if lower.contains("real")
        || lower.contains("double")
        || lower.contains("float")
        || lower.contains("numeric")
        || lower.contains("decimal")
        || lower.contains("money")
    {
        return ColumnKind::Float;
    }

    if lower.contains("int")
        || lower.contains("serial")
        || lower.contains("bigserial")
        || lower.contains("smallint")
        || lower.contains("bigint")
        || lower.contains("tinyint")
    {
        return ColumnKind::Integer;
    }

    if lower.contains("char")
        || lower.contains("text")
        || lower.contains("varchar")
        || lower.contains("nvarchar")
        || lower.contains("string")
        || lower.contains("uuid")
        || lower.contains("identifier")
        || lower.contains("json")
        || lower.contains("xml")
    {
        return ColumnKind::Text;
    }

    ColumnKind::Unknown
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

        let existing_kind = columns[col_index].kind;

        columns[col_index].kind = match agg.function {
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
}
