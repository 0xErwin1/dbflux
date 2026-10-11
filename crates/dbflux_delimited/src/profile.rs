//! Per-column profile of delimited records, computed from their text.
//!
//! [`ProfileAccumulator`] takes records one at a time, in any number of
//! batches, and keeps for every column what it needs to summarize it: which
//! value kinds every non-empty value still parses as, the null count, the
//! smallest and largest value under each kind still possible, and the
//! distinct values. Records can be added after a summary was taken, so a
//! caller that loads a file page by page feeds only the new records.
//!
//! An empty field is a null. A column's kind is the narrowest one every
//! non-empty value parses as, in the order integer, decimal, boolean, date,
//! timestamp, and text when none fits (or when the column has no non-empty
//! value). Values are trimmed of ASCII whitespace before they are parsed,
//! never before they are counted.
//!
//! Distinct values are counted exactly up to [`EXACT_DISTINCT_LIMIT`], and
//! the [`TOP_VALUE_COUNT`] most frequent are reported with their counts. Past
//! the limit the counts are dropped and a HyperLogLog sketch estimates the
//! distinct count, so the memory a column takes stays bounded however many
//! values it holds.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// The most distinct values a column counts exactly. One more switches the
/// column to an estimate and drops its top values.
pub const EXACT_DISTINCT_LIMIT: usize = 10_000;

/// How many of the most frequent values a summary reports.
pub const TOP_VALUE_COUNT: usize = 3;

/// The register index bits of the distinct-count sketch: 2^14 registers,
/// a standard error of about 0.8%.
const SKETCH_PRECISION: u32 = 14;
const SKETCH_REGISTERS: usize = 1 << SKETCH_PRECISION;

/// The kind of value every non-empty value of a column parses as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    /// A whole number that fits in 64 bits, with an optional sign.
    Integer,
    /// A decimal number, with an optional fraction and exponent.
    Decimal,
    /// `true`, `false`, `t`, `f`, `yes` or `no`, in any letter case.
    Boolean,
    /// A calendar date written `YYYY-MM-DD`.
    Date,
    /// An ISO 8601 date and time, with or without a fraction and a zone. A
    /// date alone also parses as a timestamp at midnight.
    Timestamp,
    /// Anything else.
    Text,
}

/// The smallest and largest value of a column, as they are written in the
/// file, compared under the column's kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueBounds {
    pub min: String,
    pub max: String,
}

/// What the profile knows about one column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSummary {
    pub kind: ValueKind,
    pub null_count: u64,
    pub distinct_count: u64,
    /// False when `distinct_count` is an estimate.
    pub distinct_exact: bool,
    /// `None` when the column has no non-empty value.
    pub range: Option<ValueBounds>,
    /// The most frequent values, most frequent first and ties in value
    /// order. Empty when the distinct count is an estimate.
    pub top_values: Vec<(String, u64)>,
}

/// The profile of the records added so far.
#[derive(Debug, Clone, Default)]
pub struct ProfileAccumulator {
    columns: Vec<ColumnAccumulator>,
    rows: u64,
}

impl ProfileAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one record. A field the record lacks is a null, and a field past
    /// the known columns adds a column that is null in every earlier record.
    pub fn push_record<S: AsRef<str>>(&mut self, fields: &[S]) {
        self.set_column_count(fields.len());

        for (index, column) in self.columns.iter_mut().enumerate() {
            match fields.get(index) {
                Some(field) => column.observe(field.as_ref()),
                None => column.null_count += 1,
            }
        }

        self.rows += 1;
    }

    /// Makes sure there are at least `count` columns. A column added here is
    /// null in every record added so far.
    pub fn set_column_count(&mut self, count: usize) {
        while self.columns.len() < count {
            self.columns.push(ColumnAccumulator::with_nulls(self.rows));
        }
    }

    /// How many records were added.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// The summary of every column, in column order.
    pub fn summaries(&self) -> Vec<ColumnSummary> {
        self.columns
            .iter()
            .map(ColumnAccumulator::summary)
            .collect()
    }
}

/// The smallest and largest key seen, with the text each was written as.
#[derive(Debug, Clone)]
struct Bounds<K> {
    min: Option<(K, String)>,
    max: Option<(K, String)>,
}

impl<K> Default for Bounds<K> {
    fn default() -> Self {
        Self {
            min: None,
            max: None,
        }
    }
}

impl<K: PartialOrd + Copy> Bounds<K> {
    fn observe(&mut self, key: K, text: &str) {
        let below = self
            .min
            .as_ref()
            .is_none_or(|(min, _)| key.partial_cmp(min) == Some(Ordering::Less));

        if below {
            self.min = Some((key, text.to_string()));
        }

        let above = self
            .max
            .as_ref()
            .is_none_or(|(max, _)| key.partial_cmp(max) == Some(Ordering::Greater));

        if above {
            self.max = Some((key, text.to_string()));
        }
    }

    fn value_bounds(&self) -> Option<ValueBounds> {
        let (_, min) = self.min.as_ref()?;
        let (_, max) = self.max.as_ref()?;

        Some(ValueBounds {
            min: min.clone(),
            max: max.clone(),
        })
    }
}

/// The lexicographically smallest and largest text seen.
#[derive(Debug, Clone, Default)]
struct TextBounds {
    min: Option<String>,
    max: Option<String>,
}

impl TextBounds {
    fn observe(&mut self, text: &str) {
        if self.min.as_deref().is_none_or(|min| text < min) {
            self.min = Some(text.to_string());
        }

        if self.max.as_deref().is_none_or(|max| text > max) {
            self.max = Some(text.to_string());
        }
    }

    fn value_bounds(&self) -> Option<ValueBounds> {
        Some(ValueBounds {
            min: self.min.clone()?,
            max: self.max.clone()?,
        })
    }
}

/// The distinct values of a column: exact counts up to the limit, then an
/// estimate.
#[derive(Debug, Clone)]
enum Distinct {
    Exact(HashMap<String, u64>),
    Estimated(Box<HyperLogLog>),
}

impl Default for Distinct {
    fn default() -> Self {
        Self::Exact(HashMap::new())
    }
}

impl Distinct {
    fn observe(&mut self, value: &str) {
        match self {
            Self::Exact(counts) => {
                if let Some(count) = counts.get_mut(value) {
                    *count += 1;
                    return;
                }

                if counts.len() < EXACT_DISTINCT_LIMIT {
                    counts.insert(value.to_string(), 1);
                    return;
                }

                let mut sketch = Box::new(HyperLogLog::new());

                for known in counts.keys() {
                    sketch.insert(known);
                }

                sketch.insert(value);
                *self = Self::Estimated(sketch);
            }

            Self::Estimated(sketch) => sketch.insert(value),
        }
    }
}

#[derive(Debug, Clone)]
struct ColumnAccumulator {
    null_count: u64,
    non_null_count: u64,

    could_be_integer: bool,
    could_be_decimal: bool,
    could_be_boolean: bool,
    could_be_date: bool,
    could_be_timestamp: bool,

    integer: Bounds<i64>,
    decimal: Bounds<f64>,
    boolean: Bounds<bool>,
    date: Bounds<i64>,
    timestamp: Bounds<(i64, u32)>,
    text: TextBounds,

    distinct: Distinct,
}

impl ColumnAccumulator {
    fn with_nulls(null_count: u64) -> Self {
        Self {
            null_count,
            non_null_count: 0,
            could_be_integer: true,
            could_be_decimal: true,
            could_be_boolean: true,
            could_be_date: true,
            could_be_timestamp: true,
            integer: Bounds::default(),
            decimal: Bounds::default(),
            boolean: Bounds::default(),
            date: Bounds::default(),
            timestamp: Bounds::default(),
            text: TextBounds::default(),
            distinct: Distinct::default(),
        }
    }

    fn observe(&mut self, value: &str) {
        if value.is_empty() {
            self.null_count += 1;
            return;
        }

        self.non_null_count += 1;
        self.distinct.observe(value);
        self.text.observe(value);

        let trimmed = value.trim_matches(|character: char| character.is_ascii_whitespace());

        if self.could_be_integer {
            match trimmed.parse::<i64>() {
                Ok(number) => self.integer.observe(number, value),
                Err(_) => self.could_be_integer = false,
            }
        }

        if self.could_be_decimal {
            match parse_decimal(trimmed) {
                Some(number) => self.decimal.observe(number, value),
                None => self.could_be_decimal = false,
            }
        }

        if self.could_be_boolean {
            match parse_boolean(trimmed) {
                Some(flag) => self.boolean.observe(flag, value),
                None => self.could_be_boolean = false,
            }
        }

        if self.could_be_date {
            match parse_date(trimmed) {
                Some(days) => self.date.observe(days, value),
                None => self.could_be_date = false,
            }
        }

        if self.could_be_timestamp {
            match parse_timestamp(trimmed) {
                Some(instant) => self.timestamp.observe(instant, value),
                None => self.could_be_timestamp = false,
            }
        }
    }

    fn kind(&self) -> ValueKind {
        if self.non_null_count == 0 {
            ValueKind::Text
        } else if self.could_be_integer {
            ValueKind::Integer
        } else if self.could_be_decimal {
            ValueKind::Decimal
        } else if self.could_be_boolean {
            ValueKind::Boolean
        } else if self.could_be_date {
            ValueKind::Date
        } else if self.could_be_timestamp {
            ValueKind::Timestamp
        } else {
            ValueKind::Text
        }
    }

    fn summary(&self) -> ColumnSummary {
        let kind = self.kind();

        let range = match kind {
            ValueKind::Integer => self.integer.value_bounds(),
            ValueKind::Decimal => self.decimal.value_bounds(),
            ValueKind::Boolean => self.boolean.value_bounds(),
            ValueKind::Date => self.date.value_bounds(),
            ValueKind::Timestamp => self.timestamp.value_bounds(),
            ValueKind::Text => self.text.value_bounds(),
        };

        let (distinct_count, distinct_exact, top_values) = match &self.distinct {
            Distinct::Exact(counts) => (counts.len() as u64, true, top_values(counts)),
            Distinct::Estimated(sketch) => (sketch.estimate(), false, Vec::new()),
        };

        ColumnSummary {
            kind,
            null_count: self.null_count,
            distinct_count,
            distinct_exact,
            range,
            top_values,
        }
    }
}

/// The [`TOP_VALUE_COUNT`] most frequent values of `counts`, most frequent
/// first and ties in value order.
fn top_values(counts: &HashMap<String, u64>) -> Vec<(String, u64)> {
    let mut entries: Vec<(&String, &u64)> = counts.iter().collect();

    entries.sort_by(|(left_value, left_count), (right_value, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_value.cmp(right_value))
    });

    entries
        .into_iter()
        .take(TOP_VALUE_COUNT)
        .map(|(value, count)| (value.clone(), *count))
        .collect()
}

/// A decimal number: an optional sign, digits with an optional fraction (at
/// least one digit in all), and an optional exponent. `inf` and `NaN`, which
/// `f64` parses, are not numbers here.
fn parse_decimal(text: &str) -> Option<f64> {
    let bytes = text.as_bytes();
    let mut position = 0;

    if matches!(bytes.first(), Some(b'+' | b'-')) {
        position += 1;
    }

    let mantissa_start = position;
    let mut digits = 0;

    while let Some(byte) = bytes.get(position) {
        match byte {
            b'0'..=b'9' => digits += 1,
            b'.' => {}
            _ => break,
        }

        position += 1;
    }

    let mantissa = text.get(mantissa_start..position)?;

    if digits == 0 || mantissa.matches('.').count() > 1 {
        return None;
    }

    if matches!(bytes.get(position), Some(b'e' | b'E')) {
        position += 1;

        if matches!(bytes.get(position), Some(b'+' | b'-')) {
            position += 1;
        }

        let exponent_start = position;

        while bytes.get(position).is_some_and(u8::is_ascii_digit) {
            position += 1;
        }

        if position == exponent_start {
            return None;
        }
    }

    if position != bytes.len() {
        return None;
    }

    text.parse::<f64>().ok().filter(|number| number.is_finite())
}

fn parse_boolean(text: &str) -> Option<bool> {
    const TRUE: [&str; 3] = ["true", "t", "yes"];
    const FALSE: [&str; 3] = ["false", "f", "no"];

    if TRUE.iter().any(|word| word.eq_ignore_ascii_case(text)) {
        Some(true)
    } else if FALSE.iter().any(|word| word.eq_ignore_ascii_case(text)) {
        Some(false)
    } else {
        None
    }
}

/// `count` ASCII digits at the start of `text` as a number.
fn digits(text: &str, count: usize) -> Option<u32> {
    let part = text.get(..count)?;

    if !part.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    part.parse().ok()
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        _ => 28,
    }
}

/// Days from 1970-01-01 to the given civil date, in the proleptic
/// Gregorian calendar.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;

    era * 146_097 + day_of_era - 719_468
}

/// A date written `YYYY-MM-DD`, as days since 1970-01-01.
fn parse_date(text: &str) -> Option<i64> {
    if text.len() != 10 {
        return None;
    }

    parse_date_prefix(text)
}

fn parse_date_prefix(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();

    if bytes.get(4) != Some(&b'-') || bytes.get(7) != Some(&b'-') {
        return None;
    }

    let year = i64::from(digits(text, 4)?);
    let month = digits(text.get(5..)?, 2)?;
    let day = digits(text.get(8..)?, 2)?;

    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }

    Some(days_from_civil(year, month, day))
}

/// An ISO 8601 timestamp as seconds since the epoch in UTC and nanoseconds:
/// `YYYY-MM-DD`, optionally followed by `T` or a space, `HH:MM`, optional
/// `:SS` with an optional fraction, and an optional zone (`Z`, `+HH`,
/// `+HHMM` or `+HH:MM`). A timestamp without a zone is ordered as if it were
/// UTC.
fn parse_timestamp(text: &str) -> Option<(i64, u32)> {
    let days = parse_date_prefix(text)?;

    let rest = text.get(10..)?;

    if rest.is_empty() {
        return Some((days * 86_400, 0));
    }

    let rest = rest
        .strip_prefix(['T', 't', ' '])
        .filter(|rest| rest.len() >= 5)?;

    let hour = digits(rest, 2)?;
    let minute = digits(rest.get(3..)?, 2)?;

    if rest.as_bytes().get(2) != Some(&b':') || hour > 23 || minute > 59 {
        return None;
    }

    let mut rest = rest.get(5..)?;
    let mut second = 0;
    let mut nanos = 0;

    if let Some(after_colon) = rest.strip_prefix(':') {
        second = digits(after_colon, 2)?;

        if second > 60 {
            return None;
        }

        rest = after_colon.get(2..)?;

        if let Some(fraction) = rest.strip_prefix(['.', ',']) {
            let length = fraction.bytes().take_while(u8::is_ascii_digit).count();

            if length == 0 {
                return None;
            }

            let significant = fraction.get(..length.min(9))?;
            let scale = 10_u32.pow(9 - significant.len() as u32);
            nanos = significant.parse::<u32>().ok()? * scale;
            rest = fraction.get(length..)?;
        }
    }

    let offset_seconds = parse_zone(rest)?;
    let seconds =
        days * 86_400 + i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second)
            - offset_seconds;

    Some((seconds, nanos))
}

/// The offset of a zone suffix from UTC, in seconds. An empty suffix is
/// UTC.
fn parse_zone(text: &str) -> Option<i64> {
    if text.is_empty() || text == "Z" || text == "z" {
        return Some(0);
    }

    let sign = match text.as_bytes().first() {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => return None,
    };

    let zone = text.get(1..)?;
    let hours = digits(zone, 2)?;

    let minutes = match zone.len() {
        2 => 0,
        4 => digits(zone.get(2..)?, 2)?,
        5 if zone.as_bytes().get(2) == Some(&b':') => digits(zone.get(3..)?, 2)?,
        _ => return None,
    };

    if hours > 23 || minutes > 59 {
        return None;
    }

    Some(sign * (i64::from(hours) * 3_600 + i64::from(minutes) * 60))
}

/// A HyperLogLog sketch of distinct values on a 64-bit hash.
#[derive(Debug, Clone)]
struct HyperLogLog {
    registers: Vec<u8>,
}

impl HyperLogLog {
    fn new() -> Self {
        Self {
            registers: vec![0; SKETCH_REGISTERS],
        }
    }

    fn insert(&mut self, value: &str) {
        // `DefaultHasher::new` uses fixed keys, so the same values give the
        // same estimate on every run.
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        let hash = hasher.finish();

        let index = (hash >> (64 - SKETCH_PRECISION)) as usize;
        let remaining = hash << SKETCH_PRECISION;
        let rank = (remaining.leading_zeros().min(64 - SKETCH_PRECISION) + 1) as u8;

        if let Some(register) = self.registers.get_mut(index)
            && rank > *register
        {
            *register = rank;
        }
    }

    fn estimate(&self) -> u64 {
        let registers = SKETCH_REGISTERS as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / registers);

        let sum: f64 = self
            .registers
            .iter()
            .map(|register| 2_f64.powi(-i32::from(*register)))
            .sum();
        let raw = alpha * registers * registers / sum;

        let empty = self
            .registers
            .iter()
            .filter(|register| **register == 0)
            .count();

        let estimate = if raw <= 2.5 * registers && empty > 0 {
            registers * (registers / empty as f64).ln()
        } else {
            raw
        };

        estimate.round() as u64
    }
}
