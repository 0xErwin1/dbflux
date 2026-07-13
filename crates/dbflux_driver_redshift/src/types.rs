use dbflux_core::{ColumnKind, Value};

/// Amazon Redshift preserves the standard PostgreSQL `pg_type` OID space for
/// the common scalar types, so timestamp/integer/float/text classification
/// mirrors upstream PostgreSQL exactly.
///
/// Redshift's own extended types (`SUPER`, `VARBYTE`, `GEOMETRY`, `GEOGRAPHY`,
/// `HLLSKETCH`) are assigned Redshift-specific OIDs that do not exist in the
/// `postgres` crate's built-in type registry (which targets upstream
/// PostgreSQL). They classify as `Text`, matching the defensive text-decode
/// fallback the connection layer uses for any OID it cannot otherwise
/// resolve. These OID values are unverified against a live cluster; live
/// introspection tests confirm the actual wire behavior.
const SUPER_OID: u32 = 4000;
const GEOMETRY_OID: u32 = 3000;
const GEOGRAPHY_OID: u32 = 3001;
const VARBYTE_OID: u32 = 6001;
const HLLSKETCH_OID: u32 = 3410;

/// Maps a Redshift column type OID to a semantic `ColumnKind`.
pub fn redshift_oid_to_kind(oid: u32) -> ColumnKind {
    match oid {
        1114 | 1184 | 1082 => ColumnKind::Timestamp, // TIMESTAMP, TIMESTAMPTZ, DATE
        21 | 23 | 20 => ColumnKind::Integer,         // INT2, INT4, INT8
        700 | 701 | 1700 => ColumnKind::Float,       // FLOAT4, FLOAT8, NUMERIC
        25 | 1043 | 1042 | 19 => ColumnKind::Text,   // TEXT, VARCHAR, BPCHAR, NAME
        SUPER_OID | GEOMETRY_OID | GEOGRAPHY_OID | VARBYTE_OID | HLLSKETCH_OID => ColumnKind::Text,
        _ => ColumnKind::Unknown,
    }
}

/// Decodes a column's raw wire bytes when its OID does not match one of the
/// natively-typed scalars the connection layer decodes directly.
///
/// Always attempts a UTF-8 text decode first: this covers Redshift's
/// extended types (`SUPER`, `VARBYTE`, `GEOMETRY`, `GEOGRAPHY`, `HLLSKETCH`,
/// all classified `ColumnKind::Text` by [`redshift_oid_to_kind`]) as well as
/// any OID this driver does not recognize at all (`ColumnKind::Unknown`). A
/// non-UTF8 payload degrades to `Value::Unsupported` rather than panicking —
/// there is no `FromSql` path here that can fail unexpectedly.
pub(crate) fn decode_defensive_fallback(oid: u32, type_name: &str, raw: Option<&[u8]>) -> Value {
    let Some(bytes) = raw else {
        return Value::Null;
    };

    match std::str::from_utf8(bytes) {
        Ok(text) => Value::Text(text.to_string()),
        Err(_) => {
            log::debug!(
                "Redshift column of type '{type_name}' (oid {oid}, kind {:?}) has a non-UTF8 payload; reporting as unsupported",
                redshift_oid_to_kind(oid)
            );
            Value::Unsupported(type_name.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_defensive_fallback, redshift_oid_to_kind};
    use dbflux_core::{ColumnKind, Value};

    #[test]
    fn redshift_oid_to_kind_maps_common_and_extended_types() {
        let cases = [
            (1114, ColumnKind::Timestamp),
            (1184, ColumnKind::Timestamp),
            (1082, ColumnKind::Timestamp),
            (21, ColumnKind::Integer),
            (23, ColumnKind::Integer),
            (20, ColumnKind::Integer),
            (700, ColumnKind::Float),
            (701, ColumnKind::Float),
            (1700, ColumnKind::Float),
            (25, ColumnKind::Text),
            (1043, ColumnKind::Text),
            (1042, ColumnKind::Text),
            (19, ColumnKind::Text),
            (4000, ColumnKind::Text), // SUPER
            (3000, ColumnKind::Text), // GEOMETRY
            (3001, ColumnKind::Text), // GEOGRAPHY
            (6001, ColumnKind::Text), // VARBYTE
            (3410, ColumnKind::Text), // HLLSKETCH
            (999_999, ColumnKind::Unknown),
        ];

        for (oid, expected) in cases {
            assert_eq!(redshift_oid_to_kind(oid), expected, "oid {oid} mismatch");
        }
    }

    #[test]
    fn decode_defensive_fallback_decodes_valid_utf8_as_text() {
        assert_eq!(
            decode_defensive_fallback(4000, "super", Some(b"{\"a\":1}")),
            Value::Text("{\"a\":1}".to_string())
        );
    }

    #[test]
    fn decode_defensive_fallback_returns_unsupported_on_invalid_utf8() {
        assert_eq!(
            decode_defensive_fallback(999_999, "unknown_type", Some(&[0xFF, 0xFE])),
            Value::Unsupported("unknown_type".to_string())
        );
    }

    #[test]
    fn decode_defensive_fallback_returns_null_when_raw_bytes_absent() {
        assert_eq!(decode_defensive_fallback(4000, "super", None), Value::Null);
    }
}
