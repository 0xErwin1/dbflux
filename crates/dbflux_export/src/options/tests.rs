use super::*;
use dbflux_core::chrono::{NaiveDate, NaiveTime, TimeZone, Utc};

fn timestamp() -> Value {
    Value::DateTime(Utc.with_ymd_and_hms(2026, 7, 7, 10, 30, 15).unwrap())
}

fn date() -> Value {
    Value::Date(NaiveDate::from_ymd_opt(2026, 7, 7).unwrap())
}

fn time() -> Value {
    Value::Time(NaiveTime::from_hms_milli_opt(10, 30, 15, 250).unwrap())
}

#[test]
fn default_options_are_the_previous_output() {
    let options = FormatOptions::default();
    assert!(options.is_default());
    assert_eq!(options.delimiter.byte(), b',');
    assert_eq!(options.encoding, TextEncoding::Utf8);
    assert_eq!(options.null_text, "");
    assert_eq!(options.date_format, DateFormat::Rfc3339);
}

#[test]
fn delimiter_bytes() {
    assert_eq!(CsvDelimiter::Comma.byte(), b',');
    assert_eq!(CsvDelimiter::Semicolon.byte(), b';');
    assert_eq!(CsvDelimiter::Tab.byte(), b'\t');
    assert_eq!(CsvDelimiter::Pipe.byte(), b'|');
}

#[test]
fn rfc3339_preset_renders_as_before() {
    assert_eq!(
        DateFormat::Rfc3339.render(&timestamp()).as_deref(),
        Some("2026-07-07T10:30:15+00:00")
    );
    assert_eq!(
        DateFormat::Rfc3339.render(&date()).as_deref(),
        Some("2026-07-07")
    );
    assert_eq!(
        DateFormat::Rfc3339.render(&time()).as_deref(),
        Some("10:30:15.250")
    );
}

#[test]
fn date_time_preset_renders_space_separated_utc() {
    assert_eq!(
        DateFormat::DateTime.render(&timestamp()).as_deref(),
        Some("2026-07-07 10:30:15")
    );
    assert_eq!(
        DateFormat::DateTime.render(&date()).as_deref(),
        Some("2026-07-07")
    );
    assert_eq!(
        DateFormat::DateTime.render(&time()).as_deref(),
        Some("10:30:15")
    );
}

#[test]
fn epoch_preset_renders_integer_seconds() {
    assert_eq!(
        DateFormat::Epoch.render(&timestamp()).as_deref(),
        Some("1783420215")
    );
    assert_eq!(
        DateFormat::Epoch.render(&date()).as_deref(),
        Some("1783382400")
    );
    assert_eq!(DateFormat::Epoch.render(&time()).as_deref(), Some("37815"));
}

#[test]
fn presets_leave_other_values_alone() {
    for format in DateFormat::ALL {
        assert_eq!(format.render(&Value::Int(5)), None);
    }
}

#[test]
fn options_without_new_fields_deserialize_to_defaults() {
    let options: FormatOptions = serde_json::from_str("{}").unwrap();
    assert_eq!(options, FormatOptions::default());
}

#[test]
fn options_round_trip_through_json() {
    let options = FormatOptions {
        delimiter: CsvDelimiter::Semicolon,
        encoding: TextEncoding::Windows1252,
        null_text: "\\N".to_string(),
        date_format: DateFormat::Epoch,
        sql_batch_size: 25,
    };
    let json = serde_json::to_string(&options).unwrap();
    assert_eq!(
        serde_json::from_str::<FormatOptions>(&json).unwrap(),
        options
    );
}

#[test]
fn sql_batch_size_defaults_to_five_hundred_and_older_manifests_read_it_back() {
    assert_eq!(FormatOptions::default().sql_batch_size, 500);

    let parsed: FormatOptions = serde_json::from_str(r#"{"null_text":"NA"}"#).unwrap();
    assert_eq!(parsed.sql_batch_size, 500);
    assert_eq!(parsed.null_text, "NA");
}

#[test]
fn a_non_default_sql_batch_size_is_not_default() {
    let options = FormatOptions {
        sql_batch_size: 50,
        ..FormatOptions::default()
    };
    assert!(!options.is_default());
}
