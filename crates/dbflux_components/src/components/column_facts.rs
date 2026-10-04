//! Display text for the facts a column profile carries: byte sizes, ratios,
//! percentages, counts and value ranges, and the dash that stands for a fact
//! the source does not know.
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
    let mut unit = "B";

    for larger_unit in BYTE_UNITS.iter().skip(1) {
        if value < 1024.0 {
            break;
        }

        value /= 1024.0;
        unit = larger_unit;
    }

    if value < 10.0 {
        format!("{value:.1} {unit}")
    } else {
        format!("{value:.0} {unit}")
    }
}

/// [`format_bytes`] for a size the source may not know.
pub fn format_optional_bytes(bytes: Option<u64>) -> String {
    bytes.map_or_else(|| UNKNOWN_FACT.to_string(), format_bytes)
}

/// A compression ratio as a multiplier: one decimal place below ten, whole
/// numbers from ten up (`6.1×`, `41×`).
pub fn format_ratio(ratio: Option<f64>) -> String {
    match ratio {
        Some(ratio) if ratio < 10.0 => format!("{ratio:.1}×"),
        Some(ratio) => format!("{ratio:.0}×"),
        None => UNKNOWN_FACT.to_string(),
    }
}

/// A fraction from 0.0 to 1.0 as a percentage: `0%` only for exactly zero,
/// `<0.1%` for a sliver above it, one decimal place below ten percent and
/// whole numbers from ten up (`1.4%`, `38%`).
pub fn format_percent(fraction: Option<f64>) -> String {
    let Some(fraction) = fraction else {
        return UNKNOWN_FACT.to_string();
    };

    let percent = fraction * 100.0;

    if percent <= 0.0 {
        "0%".to_string()
    } else if percent < 0.1 {
        "<0.1%".to_string()
    } else if percent < 10.0 {
        format!("{percent:.1}%")
    } else {
        format!("{percent:.0}%")
    }
}

/// A count the source may not know.
pub fn format_count(count: Option<u64>) -> String {
    count.map_or_else(|| UNKNOWN_FACT.to_string(), |count| count.to_string())
}

/// The smallest and largest value as `min → max`, marked with `≈` when a
/// bound is not itself a value of the column.
pub fn format_range(min: &str, max: &str, exact: bool) -> String {
    if exact {
        format!("{min} → {max}")
    } else {
        format!("≈ {min} → {max}")
    }
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

    #[test]
    fn ratios_percents_and_counts_show_a_dash_when_unknown() {
        assert_eq!(format_ratio(Some(6.12)), "6.1×");
        assert_eq!(format_ratio(Some(41.3)), "41×");
        assert_eq!(format_ratio(None), UNKNOWN_FACT);

        assert_eq!(format_percent(Some(0.0)), "0%");
        assert_eq!(format_percent(Some(0.0004)), "<0.1%");
        assert_eq!(format_percent(Some(0.014)), "1.4%");
        assert_eq!(format_percent(Some(0.38)), "38%");
        assert_eq!(format_percent(None), UNKNOWN_FACT);

        assert_eq!(format_count(Some(0)), "0");
        assert_eq!(format_count(None), UNKNOWN_FACT);
    }

    #[test]
    fn an_inexact_range_is_marked() {
        assert_eq!(format_range("1", "211", true), "1 → 211");
        assert_eq!(format_range("a", "zz", false), "≈ a → zz");
    }
}
