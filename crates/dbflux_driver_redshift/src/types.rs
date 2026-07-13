use dbflux_core::ColumnKind;

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

#[cfg(test)]
mod tests {
    use super::redshift_oid_to_kind;
    use dbflux_core::ColumnKind;

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
}
