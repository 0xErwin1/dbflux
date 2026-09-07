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

/// Decodes the PostgreSQL binary `NUMERIC` wire format into an exact decimal
/// string, returning `None` on malformed input.
///
/// `tokio-postgres`/`postgres` negotiate the BINARY result format for every
/// column, so a `NUMERIC` column arrives as this binary encoding, never as
/// ASCII text. The layout is a fixed 8-byte header of four big-endian 16-bit
/// fields — `ndigits`, `weight`, `sign`, `dscale` — followed by `ndigits`
/// base-10000 digit groups (each a big-endian `i16` in `0..=9999`). `weight`
/// is the base-10000 exponent of the first group; `dscale` is the number of
/// fractional decimal digits to display; `sign` is `0x0000` (positive),
/// `0x4000` (negative), `0xC000` (NaN), or `0xD000`/`0xF000` (±Infinity).
///
/// The value is reconstructed exactly (no float rounding): the integer part is
/// emitted group by group (the first without leading zeros, the rest
/// zero-padded to four digits, with implicit trailing zero groups when
/// `weight` exceeds the last stored group), and the fractional part is emitted
/// to exactly `dscale` digits, honoring leading fractional zero groups when
/// `weight` is negative.
pub(crate) fn decode_pg_numeric_binary(bytes: &[u8]) -> Option<String> {
    const NUMERIC_POS: u16 = 0x0000;
    const NUMERIC_NEG: u16 = 0x4000;
    const NUMERIC_NAN: u16 = 0xC000;
    const NUMERIC_PINF: u16 = 0xD000;
    const NUMERIC_NINF: u16 = 0xF000;

    let read_be_u16 = |offset: usize| -> Option<u16> {
        let pair: [u8; 2] = bytes.get(offset..offset + 2)?.try_into().ok()?;
        Some(u16::from_be_bytes(pair))
    };

    let ndigits = read_be_u16(0)? as i16;
    let weight = read_be_u16(2)? as i16 as i32;
    let sign = read_be_u16(4)?;
    let dscale = read_be_u16(6)? as usize;

    match sign {
        NUMERIC_NAN => return Some("NaN".to_string()),
        NUMERIC_PINF => return Some("Infinity".to_string()),
        NUMERIC_NINF => return Some("-Infinity".to_string()),
        NUMERIC_POS | NUMERIC_NEG => {}
        _ => return None,
    }

    // Redshift/PostgreSQL NUMERIC tops out at precision 38, so the base-10000
    // `weight` (the exponent of the first digit group) never legitimately
    // exceeds ~10. A far larger `weight` can only come from a malformed or
    // hostile payload, where the integer reconstruction below would otherwise
    // allocate a huge zero-filled string (`weight` is a wire-supplied `i16`, so
    // up to 32767 groups). Reject such input instead.
    const MAX_NUMERIC_WEIGHT: i32 = 96;
    if weight > MAX_NUMERIC_WEIGHT {
        return None;
    }

    let ndigits = usize::try_from(ndigits).ok()?;

    let digits_region = bytes.get(8..8usize.checked_add(ndigits.checked_mul(2)?)?)?;

    let mut groups = Vec::with_capacity(ndigits);
    for chunk in digits_region.chunks_exact(2) {
        let pair: [u8; 2] = chunk.try_into().ok()?;
        let group = i16::from_be_bytes(pair);
        if !(0..10_000).contains(&group) {
            return None;
        }
        groups.push(group);
    }

    let group_at = |exponent: i32| -> i16 {
        usize::try_from(weight - exponent)
            .ok()
            .and_then(|index| groups.get(index).copied())
            .unwrap_or(0)
    };

    let mut result = String::new();
    if sign == NUMERIC_NEG {
        result.push('-');
    }

    if weight < 0 {
        result.push('0');
    } else {
        for exponent in (0..=weight).rev() {
            let group = group_at(exponent);
            if exponent == weight {
                result.push_str(&group.to_string());
            } else {
                result.push_str(&format!("{group:04}"));
            }
        }
    }

    if dscale > 0 {
        result.push('.');

        let mut fractional = String::new();
        let mut exponent: i32 = -1;
        while fractional.len() < dscale {
            fractional.push_str(&format!("{:04}", group_at(exponent)));
            exponent -= 1;
        }
        fractional.truncate(dscale);
        result.push_str(&fractional);
    }

    Some(result)
}

/// Decodes Redshift's `NUMERIC`/`DECIMAL` (OID 1700) wire bytes into a
/// `Value::Decimal`.
///
/// `f64: FromSql` only accepts `FLOAT8`, not `NUMERIC`, so the connection
/// layer cannot decode this column through a typed `try_get` the way it does
/// for `float8`. The wire bytes arrive in the binary `NUMERIC` format, so this
/// decodes them exactly via [`decode_pg_numeric_binary`] and labels the result
/// `Value::Decimal` so downstream numeric handling (e.g. the chart engine's
/// `s.parse::<f64>()`) treats it as a number rather than an opaque string. Only
/// if the binary decode fails (malformed or unexpectedly non-binary payload)
/// does it fall back to the defensive text decode, still relabeled as
/// `Value::Decimal`, and finally to `Value::Unsupported` for undecodable bytes.
pub(crate) fn decode_numeric_fallback(oid: u32, type_name: &str, raw: Option<&[u8]>) -> Value {
    let Some(bytes) = raw else {
        return Value::Null;
    };

    if let Some(decimal) = decode_pg_numeric_binary(bytes) {
        return Value::Decimal(decimal);
    }

    match decode_defensive_fallback(oid, type_name, Some(bytes)) {
        Value::Text(text) => Value::Decimal(text),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_defensive_fallback, decode_numeric_fallback, decode_pg_numeric_binary,
        redshift_oid_to_kind,
    };
    use dbflux_core::{ColumnKind, Value};

    /// Encodes the PostgreSQL binary `NUMERIC` wire format from its logical
    /// parts, mirroring what the server sends on the wire so the decode tests
    /// exercise real payloads rather than ASCII stand-ins.
    fn encode_pg_numeric(
        ndigits: i16,
        weight: i16,
        sign: u16,
        dscale: u16,
        groups: &[i16],
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + groups.len() * 2);
        bytes.extend_from_slice(&ndigits.to_be_bytes());
        bytes.extend_from_slice(&weight.to_be_bytes());
        bytes.extend_from_slice(&sign.to_be_bytes());
        bytes.extend_from_slice(&dscale.to_be_bytes());
        for group in groups {
            bytes.extend_from_slice(&group.to_be_bytes());
        }
        bytes
    }

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

    #[test]
    fn decode_pg_numeric_binary_decodes_zero() {
        // NUMERIC 0 is `ndigits = 0`: its bytes are all `< 0x80`, the exact
        // payload that a naive `from_utf8` would silently accept as garbage.
        let bytes = encode_pg_numeric(0, 0, 0x0000, 0, &[]);
        assert_eq!(decode_pg_numeric_binary(&bytes).as_deref(), Some("0"));
    }

    #[test]
    fn decode_pg_numeric_binary_decodes_fraction() {
        // 123.45 = [123, 4500] base-10000, weight 0, scale 2.
        let bytes = encode_pg_numeric(2, 0, 0x0000, 2, &[123, 4500]);
        assert_eq!(decode_pg_numeric_binary(&bytes).as_deref(), Some("123.45"));
    }

    #[test]
    fn decode_pg_numeric_binary_decodes_negative_fraction() {
        let bytes = encode_pg_numeric(2, 0, 0x4000, 2, &[123, 4500]);
        assert_eq!(decode_pg_numeric_binary(&bytes).as_deref(), Some("-123.45"));
    }

    #[test]
    fn decode_pg_numeric_binary_decodes_pure_fraction_with_leading_zero() {
        // 0.0045 = group 45 at exponent -1, scale 4.
        let bytes = encode_pg_numeric(1, -1, 0x0000, 4, &[45]);
        assert_eq!(decode_pg_numeric_binary(&bytes).as_deref(), Some("0.0045"));
    }

    #[test]
    fn decode_pg_numeric_binary_decodes_large_multi_group_integer() {
        // 123456789 = [1, 2345, 6789] base-10000, weight 2, scale 0.
        let bytes = encode_pg_numeric(3, 2, 0x0000, 0, &[1, 2345, 6789]);
        assert_eq!(
            decode_pg_numeric_binary(&bytes).as_deref(),
            Some("123456789")
        );
    }

    #[test]
    fn decode_pg_numeric_binary_pads_trailing_integer_groups() {
        // 20000 = group [2] at weight 1 (implicit trailing zero group).
        let bytes = encode_pg_numeric(1, 1, 0x0000, 0, &[2]);
        assert_eq!(decode_pg_numeric_binary(&bytes).as_deref(), Some("20000"));
    }

    #[test]
    fn decode_pg_numeric_binary_decodes_nan() {
        let bytes = encode_pg_numeric(0, 0, 0xC000, 0, &[]);
        assert_eq!(decode_pg_numeric_binary(&bytes).as_deref(), Some("NaN"));
    }

    #[test]
    fn decode_pg_numeric_binary_rejects_absurd_weight_without_allocating() {
        // A hostile payload claims a base-10000 weight far beyond the ~10 a
        // real precision-38 NUMERIC can reach; the decoder must bail out rather
        // than build a multi-kilobyte zero-filled integer string.
        let bytes = encode_pg_numeric(1, 30_000, 0x0000, 0, &[1]);
        assert_eq!(decode_pg_numeric_binary(&bytes), None);

        // A weight just past the cap is also rejected.
        let just_over = encode_pg_numeric(1, 97, 0x0000, 0, &[1]);
        assert_eq!(decode_pg_numeric_binary(&just_over), None);
    }

    #[test]
    fn decode_pg_numeric_binary_accepts_weight_at_the_cap() {
        // A weight exactly at the cap still decodes: group 5 at weight 96 is a
        // 1-followed-by-many-zeros integer, well-formed and within bounds.
        let bytes = encode_pg_numeric(1, 96, 0x0000, 0, &[5]);
        let decoded = decode_pg_numeric_binary(&bytes).expect("weight at cap decodes");
        assert!(decoded.starts_with('5'));
        assert_eq!(decoded.len(), 1 + 96 * 4);
    }

    #[test]
    fn decode_pg_numeric_binary_returns_none_on_malformed_input() {
        // Header shorter than 8 bytes, and a group count exceeding the payload.
        assert_eq!(decode_pg_numeric_binary(&[0xFF, 0xFE]), None);

        let truncated = encode_pg_numeric(3, 0, 0x0000, 0, &[1]);
        assert_eq!(decode_pg_numeric_binary(&truncated), None);
    }

    #[test]
    fn decode_numeric_fallback_decodes_binary_wire_value_as_decimal() {
        let bytes = encode_pg_numeric(2, 0, 0x0000, 2, &[123, 4500]);
        assert_eq!(
            decode_numeric_fallback(1700, "numeric", Some(&bytes)),
            Value::Decimal("123.45".to_string())
        );

        let zero = encode_pg_numeric(0, 0, 0x0000, 0, &[]);
        assert_eq!(
            decode_numeric_fallback(1700, "numeric", Some(&zero)),
            Value::Decimal("0".to_string())
        );
    }

    #[test]
    fn decode_numeric_fallback_returns_null_for_a_real_sql_null() {
        assert_eq!(decode_numeric_fallback(1700, "numeric", None), Value::Null);
    }

    #[test]
    fn decode_numeric_fallback_falls_back_to_unsupported_on_undecodable_bytes() {
        assert_eq!(
            decode_numeric_fallback(1700, "numeric", Some(&[0xFF, 0xFE])),
            Value::Unsupported("numeric".to_string())
        );
    }
}
