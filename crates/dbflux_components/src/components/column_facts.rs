//! Display text for the facts a column profile carries: byte sizes, and the
//! dash that stands for a fact the source does not know.
//!
//! A missing fact is never shown as zero, because zero is itself a fact.

/// Shown in place of a fact the source does not know.
pub const UNKNOWN_FACT: &str = "—";

const BYTE_UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

/// A byte count with a binary-prefix unit: exact below one kibibyte, one
/// decimal place below ten units, and whole units from ten up (`512 B`,
/// `2.9 GiB`, `46 MiB`).
pub fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        return format!("{bytes} B");
    }

    let mut value = bytes as f64;
    let mut unit = 0;

    while value >= 1024.0 && unit + 1 < BYTE_UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }

    if value < 10.0 {
        format!("{value:.1} {}", BYTE_UNITS[unit])
    } else {
        format!("{value:.0} {}", BYTE_UNITS[unit])
    }
}

/// [`format_bytes`] for a size the source may not know.
pub fn format_optional_bytes(bytes: Option<u64>) -> String {
    bytes.map_or_else(|| UNKNOWN_FACT.to_string(), format_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_use_binary_units_with_one_decimal_below_ten() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(46 * 1024 * 1024), "46 MiB");
        assert_eq!(format_bytes(3 * 1024 * 1024 * 1024 - 1), "3.0 GiB");
        assert_eq!(format_bytes(u64::MAX), "16384 PiB");
    }

    #[test]
    fn an_unknown_size_is_a_dash_not_zero() {
        assert_eq!(format_optional_bytes(None), UNKNOWN_FACT);
        assert_eq!(format_optional_bytes(Some(0)), "0 B");
    }
}
