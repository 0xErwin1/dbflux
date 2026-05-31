use crate::query::types::ColumnKind;

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
